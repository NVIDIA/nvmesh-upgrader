#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import signal
import socket
import datetime
import subprocess
import logging
import json
import sys
import traceback
import re
import time
import asyncio
import uuid
import glob
from fsatomic import atomicWrite
import shutil
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from utils import (
    getDateTime, isIntervalElapsed, readFile, isFileExists, getLinuxDistro,
    extractVersion, getLibrdkafkaVersion, calculateMD5
)

# Remote debugging support
import argparse
try:
    import debugpy
    DEBUGPY_AVAILABLE = True
except ImportError:
    DEBUGPY_AVAILABLE = False

# Constants
MANAGEMENT_TOPIC_NAME = 'default.management.priority.1.0.0'
UPGRADE_AGENT_TOPIC_SUFFIX = 'upgradeAgent.commands.1.0.0'
NVMESH_CONFIG_FILE_PATH = '/etc/nvmesh/nvmesh.conf'
CONFIG_FILE_PATH = '/etc/nvmesh/upgradeagent.conf'
VERSION_FILE_BASE_PATH = '/opt/nvmesh/upgradeagent/version'
TLS_CERTS_DIR = '/var/run/nvmesh/tls/nvmeshupgradeagent'
MIN_KEEPALIVE_INTERVAL = 5

# Journal
JOURNAL_DIR = '/var/opt/nvmesh/upgradeagent'
JOURNAL_PATH = os.path.join(JOURNAL_DIR, 'journal.json')


class JournalStates:
    STARTED = 'started'
    RESULT_READY = 'resultReady'
    RESPONSE_PRODUCED = 'responseProduced'
    COMPLETED = 'completed'


class MessageTypes:
    # UPGRADE AGENT -> MGMT
    UPGRADE_AGENT_KEEPALIVE = 'keepalive'
    UPGRADE_AGENT_COMMAND_RESULT = 'commandResult'
    # MGMT ->UPGRADE AGENT
    UPDATE_UPGRADE_AGENT_KEEPALIVE_TOKEN = 'updateUpgradeAgentKeepaliveToken'
    UPGRADE_AGENT_COMMAND = 'upgradeAgentCommand'


class UpgradeAgent:
    def __init__(self):
        self.archType = None
        self.osID = None
        self.osVersionID = None
        self.osVersion = None
        self.osDistribution = None
        self.linuxDistro = None
        self.logger = self.initLogger()
        self.config = readFile(CONFIG_FILE_PATH)
        self.nvmeshConfig = readFile(NVMESH_CONFIG_FILE_PATH)
        self.upgradeAgentVersion = self.getUpgradeAgentVersion().get('version')
        self.keepaliveInterval = self.config.get('DEFAULT_KEEPALIVE_INTERVAL', 5)
        self.consumerPollTimeout = self.config.get('CONSUMER_POLL_TIMEOUT', 1.0)
        self.producerPollTimeout = self.config.get('PRODUCER_POLL_TIMEOUT', 0.1)
        self.artifactsDir = self.config.get('ARTIFACTS_DIR')
        self.isKafkaTLS = self.config.get('KAFKA_TLS_ENABLED', False) in ['Yes', 'yes', 'True', 'true']
        self.dataCollectionCheckInterval = self.config.get('DATA_COLLECTION_CHECK_INTERVAL', 5)

        self.retrieveMachineInformation()
        self.featureCompatibilityVersion = '1'
        self.lastMessageMD5 = None
        self.lastDataCollectionCheckTime = None
        self.shouldContinue = True
        self.isExecutingCommand = False
        self.messageSequence = 0
        self.lastKeepAliveTime = None
        self.hostname = socket.gethostname()
        self.additionalData = []
        self.upgradeAgentToken = -1
        self.bootstrapServers = self.getBootstrapServers()
        self.kafkaAdminClient = None
        self.consumer = None
        self.producer = None
        self.consumerId = None
        self.producerId = None
        self.needsReload = False
        self.isReloadingKafkaConnections = False
        self.isClosingProducer = False
        self.isClosingConsumer = False
        self.isRestartingAgent = False
        signal.signal(signal.SIGINT, self.handleSignal)
        signal.signal(signal.SIGTERM, self.handleSignal)
        signal.signal(signal.SIGHUP, self.handleSIGHUP)

        # Copy TLS certificates to runtime directory if TLS is enabled
        if self.isKafkaTLS:
            self.copyCertificates()

        self.loadJournal()

    @staticmethod
    def getUpgradeAgentVersion():
        """Read upgrade agent version from version file."""
        upgradeAgentVersion = {}
        with open(VERSION_FILE_BASE_PATH, "r") as f:
            for line in f:
                if "=" in line:
                    key, val = line.strip().split("=", 1)
                    upgradeAgentVersion[key] = val.strip('"')
        return upgradeAgentVersion

    def loadJournal(self):
        try:
            if isFileExists(JOURNAL_PATH):
                with open(JOURNAL_PATH, 'r') as f:
                    self.journal = json.load(f)
            else:
                self.journal = {
                    'version': 1,
                    'currentToken': -1,
                    'commands': {}
                }
        except Exception as e:
            self.logger.error(f'Failed to load journal: {e}')
            self.exitGracefully()

    def saveJournal(self):
        """
        Persist the journal atomically so that readers never see a partially-written file.
        We serialize to bytes and use atomic_write which fsyncs data and the parent directory.
        """
        try:
            payload = json.dumps(self.journal, ensure_ascii=False).encode('utf-8')
            atomicWrite(JOURNAL_PATH, payload)
        except Exception as e:
            self.logger.error(f'Failed to save journal: {e}')
            self.exitGracefully()

    def isRHELBased(self):
        return 'rhel' in self.osDistribution

    def isUbuntuBased(self):
        return any(x in self.osDistribution for x in ('ubuntu', 'debian'))

    def retrieveMachineInformation(self):
        self.linuxDistro = getLinuxDistro()
        if not self.linuxDistro.get('ID_LIKE') or not self.linuxDistro.get('PRETTY_NAME'):
            self.logger.error('Failed to retrieve Linux distribution information')
            sys.exit(1)

        self.osDistribution = self.linuxDistro.get('ID_LIKE').lower()

        if not self.isRHELBased() and not self.isUbuntuBased():
            self.logger.error(f'Unsupported OS distribution: {self.osDistribution}')
            sys.exit(1)

        self.osVersion = self.linuxDistro.get('PRETTY_NAME')
        self.osVersionID = self.linuxDistro.get('VERSION_ID')
        self.osID = self.linuxDistro.get('ID')
        self.archType = self.getArchType()

    def initLogger(self):
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        return logger

    async def start(self):
        self.logger.debug(f'Starting upgrade agent, hostname: {self.hostname}, osDistribution: {self.osDistribution}, osVersion: {self.osVersion}')

        self.initAdminClient()
        self.validateOutgoingTopicsExists()

        self.initConsumer()
        self.initProducer()

        while self.shouldContinue:
            if self.producer:
                self.producePeriodicReports()
            if self.consumer and not self.isExecutingCommand:
                await self.consume()
            else:
                await asyncio.sleep(self.consumerPollTimeout)

            self.checkAndPerformReload()

        self.exitGracefully()

    def initAdminClient(self):
        conf = {
            'bootstrap.servers': ','.join(self.bootstrapServers)
        }

        if self.isKafkaTLS:
            ssl_conf = self.getKafkaSSLConfig()
            conf.update(ssl_conf)

        self.kafkaAdminClient = AdminClient(conf)

    def validateOutgoingTopicsExists(self):
        while self.shouldContinue:
            try:
                topics = self.kafkaAdminClient.list_topics(topic=MANAGEMENT_TOPIC_NAME, timeout=5).topics
                if MANAGEMENT_TOPIC_NAME in topics:
                    self.logger.debug('Done validating outgoing topics!')
                    break

                self.logger.debug(f'{MANAGEMENT_TOPIC_NAME} topic doesn\'t exist. Is the management running?')
                time.sleep(5)

            except Exception as e:
                self.logger.error(f"Failed to validate outgoing topics: {e}")
                self.logger.error(f"Reloading kafka admin client...")
                self.kafkaAdminClient = None
                time.sleep(5)

                if self.isKafkaTLS:
                    self.copyCertificates()
                self.initAdminClient()

    def getBootstrapServers(self):
        bootstrapServersConfig = self.config.get('KAFKA_SERVERS') or self.nvmeshConfig.get('KAFKA_SERVERS')

        if not bootstrapServersConfig:
            self.logger.error('Failed to find KAFKA_SERVERS in upgradeagent.conf or nvmesh.conf, exiting')
            sys.exit(1)

        try:
            bootstrapServers = bootstrapServersConfig.split(',')
        except Exception as e:
            self.logger.error(f'Failed to parse KAFKA_SERVERS, found: {bootstrapServersConfig}, ex: {e} , exiting')
            sys.exit(1)

        self.logger.debug(f'Kafka bootstrap servers: {bootstrapServers}')
        return bootstrapServers

    def copyCertificates(self):
        """
        Copy TLS certificates to runtime directory.
        """
        certConfigKeys = ['KAFKA_SSL_CERTIFICATE', 'KAFKA_SSL_KEY', 'KAFKA_SSL_CA']

        # Verify that the certificate files exist
        for certConfigKey in certConfigKeys:
            certFile = self.config.get(certConfigKey)
            if not certFile:
                self.logger.error(f'Config key {certConfigKey} is missing')
                sys.exit(1)

            if not os.path.exists(certFile):
                self.logger.error(f'Certificate file {certFile} does not exist')
                sys.exit(1)

        # Copy certificates to runtime directory
        try:
            if not os.path.exists(TLS_CERTS_DIR):
                self.logger.debug(f'Creating TLS certificates directory: {TLS_CERTS_DIR}')
                os.makedirs(TLS_CERTS_DIR, mode=0o700, exist_ok=True)

            srcCertFiles = list(map(lambda certConfigKey: self.config.get(certConfigKey), certConfigKeys))

            for srcCertFile in srcCertFiles:
                destCertFile = os.path.join(TLS_CERTS_DIR, os.path.basename(srcCertFile))
                shutil.copy2(srcCertFile, destCertFile)
                os.chmod(destCertFile, 0o600)

            self.logger.debug(f'Successfully copied TLS certificates to TLS certificates directory {TLS_CERTS_DIR}')

        except Exception as e:
            self.logger.error(f'Failed to copy certificates: {e}')
            raise

    def getKafkaSSLConfig(self):
        kafkaSslCertFilePath = os.path.join(TLS_CERTS_DIR, os.path.basename(self.config.get('KAFKA_SSL_CERTIFICATE')))
        kafkaSslKeyFilePath = os.path.join(TLS_CERTS_DIR, os.path.basename(self.config.get('KAFKA_SSL_KEY')))
        kafkaSslCaFilePath = os.path.join(TLS_CERTS_DIR, os.path.basename(self.config.get('KAFKA_SSL_CA')))

        sslConf = {
            'security.protocol': 'SSL',
            'enable.ssl.certificate.verification': 'true',
            'ssl.certificate.location': kafkaSslCertFilePath,
            'ssl.key.location': kafkaSslKeyFilePath,
            'ssl.ca.location': kafkaSslCaFilePath,
        }

        # Add key password if specified
        if self.config.get('KAFKA_SSL_KEY_PASSWORD'):
            sslConf['ssl.key.password'] = self.config.get('KAFKA_SSL_KEY_PASSWORD')

        return sslConf

    def reloadKafkaConnections(self):
        self.logger.debug(f"Reloading kafka connections...")
        self.isReloadingKafkaConnections = True

        self.closeConsumer()
        self.closeProducer()

        # Copy certificates on reload if TLS is enabled
        if self.isKafkaTLS:
            self.logger.debug("Reloading TLS certificates...")
            self.copyCertificates()

        self.initConsumer()
        self.initProducer()

        self.needsReload = False
        self.isReloadingKafkaConnections = False

    def checkAndPerformReload(self):
        if self.needsReload and not self.isReloadingKafkaConnections:
            self.reloadKafkaConnections()

    def isSSLRelatedError(self, err):
        errorMessage = str(err)
        return "SSL" in errorMessage or "certificate" in errorMessage

    def onConsumerError(self, err, consumerId):
        self.logger.debug(f"Kafka consumer {consumerId} error: {err}")

        if self.isSSLRelatedError(err) and not self.isClosingConsumer:
            if consumerId == self.consumerId:
                self.needsReload = True

    def onProducerError(self, err, producerId):
        self.logger.debug(f"Kafka producer {producerId} error: {err}")

        if self.isSSLRelatedError(err) and not self.isClosingProducer:
            if producerId == self.producerId:
                self.needsReload = True

    def initConsumer(self):
        consumerId = uuid.uuid4().hex[:8]
        self.consumerId = consumerId

        topicsConfig = self.getInterestConsumersConfig()

        conf = {
            'bootstrap.servers': ','.join(self.bootstrapServers),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'group.id': 'UPGRADE_AGENT_' + self.hostname,
            'error_cb': lambda err: self.onConsumerError(err, consumerId),
        }

        if self.isKafkaTLS:
            ssl_conf = self.getKafkaSSLConfig()
            conf.update(ssl_conf)

        consumer = Consumer(conf)

        topicsPartitions = map(lambda topic: TopicPartition(topic.get('name'), topic.get('partition')), topicsConfig)

        consumer.assign(list(topicsPartitions))
        self.consumer = consumer
        self.logger.debug(f'Consumer initialized. ID: {self.consumerId}')

    def initProducer(self):
        producerId = uuid.uuid4().hex[:8]
        self.producerId = producerId

        conf = {
            'bootstrap.servers': ','.join(self.bootstrapServers),
            'client.id': 'upgradeAgent_' + self.hostname,
            'error_cb': lambda err: self.onProducerError(err, producerId),
            'retries': 5
        }

        if self.isKafkaTLS:
            ssl_conf = self.getKafkaSSLConfig()
            conf.update(ssl_conf)

        self.producer = Producer(conf)
        self.logger.debug(f'Producer initialized. ID: {self.producerId}')

    def produceMessageToTopic(self, message, topic):
        def onProduce(err, msg):
            if err:
                self.logger.error(f'Produced message Failed! topic: {topic}, error: {err}')

        try:
            self.producer.produce(topic, value=json.dumps(message), callback=onProduce)
            self.messageSequence += 1

        except Exception as e:
            self.logger.error(f'Failed to produce message to topic: {topic}, message: {message}, ex: {str(e)}')
            self.exitGracefully()

    async def produceMessageToTopicAwaitAck(self, message, topic):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def _deliveryCallback(err, msg):
            if not future.done():
                if err is not None:
                    future.set_exception(RuntimeError(f'Kafka delivery failed: {err}'))
                else:
                    future.set_result(msg)
                    self.messageSequence += 1

        try:
            self.producer.produce(
                topic,
                value=json.dumps(message),
                on_delivery=_deliveryCallback
            )
        except Exception as e:
            self.logger.error(f'Failed to produce message to topic: {topic}, message: {message}, ex: {str(e)}')
            raise e

        try:
            while not future.done():
                self.producer.poll(self.producerPollTimeout) # Poll for delivery events
                await asyncio.sleep(self.producerPollTimeout)

            return await future
        except Exception as e:
            self.logger.error(f'Failed to produce message to topic: {topic}, message: {message}, ex: {str(e)}')
            raise e


    def getRPMVersions(self):
        rpmVersions = []
        if self.isRHELBased():
            rpmVersions = self.runCommand("rpm -qa nvmesh*").stdout.splitlines()
        elif self.isUbuntuBased():
            rpmVersions = self.runCommand("dpkg-query -W -f='${binary:Package}-${Version}\n' | grep '^nvmesh'").stdout.splitlines()

        rpmDict = {}
        for rpm in rpmVersions:
            match = re.match(r'^(nvmesh-[^\d]*)-(.*)$', rpm)
            if match:
                rpmName, rpmVersion = match.groups()
                rpmDict[rpmName] = rpmVersion
        return rpmDict

    def getArchType(self):
        return self.runCommand("uname -m").stdout.rstrip()

    def getKernelVersion(self):
        return self.runCommand("uname -r").stdout.rstrip()

    def getOFEDVersion(self):
        whichOfedRes = self.runCommand("which ofed_info")
        if whichOfedRes.returncode != 0:
            return "inbox"

        return self.runCommand("ofed_info -n").stdout.rstrip()

    def closeProducer(self):
        if self.producer:
            self.logger.debug(f"Closing Kafka producer... ID: {self.producerId}")
            self.isClosingProducer = True
            try:
                self.producer.flush(5)
            except Exception as e:
                self.logger.debug(f"Error flushing producer, ID: {self.producerId}, error: {e}")
            finally:
                self.producer = None
                self.isClosingProducer = False

    def closeConsumer(self):
        self.isClosingConsumer = True

        if self.consumer:
            try:
                self.logger.debug(f"Closing Kafka consumer... ID: {self.consumerId}")
                self.consumer.close()
            except Exception as e:
                self.logger.debug(f"Error while closing consumer, ID: {self.consumerId}, error: {e}")
            finally:
                self.consumer = None

        self.isClosingConsumer = False

    def cleanup(self):
        self.logger.debug('Cleaning up...')
        try:
            self.closeConsumer()
            self.closeProducer()
        except Exception as e:
            self.logger.error(f'Error during cleanup: {e}')

    def handleSignal(self, signum, frame):
        self.logger.debug(f'Received signal {signum}')
        self.exitOnNextIteration()

    def exitOnNextIteration(self):
        self.logger.debug('Marking service to exit on next iteration...')
        self.shouldContinue = False

    def exitGracefully(self, signum = None, frame = None):
        self.logger.debug('Exiting gracefully...')
        self.cleanup()
        sys.exit(0)

    def handleSIGHUP(self, signum, frame):
        self.logger.debug(f'Received SIGHUP signal - reloading certificates and Kafka connections')
        if not self.isReloadingKafkaConnections:
            self.needsReload = True
        else:
            self.logger.warning("Reload already in progress, ignoring SIGHUP")

    async def consume(self):
        message = self.consumer.poll(timeout=self.consumerPollTimeout)

        if message is None:
            return

        error = message.error()
        if error:
            self.logger.debug('Consumer.poll error occurred. code:{} error:{}'.format(error.code(), error.str()))
            return

        try:
            message_value = json.loads(message.value().decode('utf-8'))
        except Exception as e:
            self.logger.error(f'Failed to decode message! message value: {message.value()}, exception: {e}')
            return

        message_type = message_value.get('messageType')

        self.logger.debug(f'Received a message! topic: {message.topic()}, type: {message_type}, offset: {message.offset()}')

        try:
            kafkaCtx = {
                'topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset(),
            }
            await self.handleMessage(message_value, kafkaCtx)
        except Exception as e:
            self.logger.error(
                f'Failed to handle message! topic: {message.topic()}, '
                f'type: {message_type}, offset: {message.offset()}, exception: {e}, traceback: {traceback.format_exc()}')
            self.rewindConsumer(message)

    def rewindConsumer(self, message):
        self.logger.debug(f"Seeking back to offset {message.offset()} on partition {message.partition()} for replay")
        try:
            self.consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
        except Exception as seekErr:
            self.logger.error(f"Failed to seek to offset {message.offset()} for replay: {seekErr}")
            self.exitGracefully()

    def getInterestConsumersConfig(self):
        topicsConfig = [{'name': f'{self.hostname}.{UPGRADE_AGENT_TOPIC_SUFFIX}', 'partition': 0}]
        return topicsConfig

    def getComponentSpecificMessageHeaders(self):
        return {
            'messageSequence': self.messageSequence,
            'upgradeAgentID': self.hostname,
            'hostname': self.hostname,
            'upgradeAgentToken': self.upgradeAgentToken
        }

    def collectData(self):
        payload = {
            "version": extractVersion(self.upgradeAgentVersion),
            "librdkafkaVersion": getLibrdkafkaVersion(),
            "operatingSystem": {
                "name": self.osVersion,
                "id": self.osID,
                "versionID": self.osVersionID
            },
            "archType": self.archType,
            "kernel": self.getKernelVersion(),
            "nvmeshVersions": self.getRPMVersions(),
            "ofed": self.getOFEDVersion(),
            "additionalData": {d["key"]: self.runCommand(d["command"]) for d in self.additionalData}
        }

        return payload

    def runCommand(self, pipeline, timeout=None):
        return subprocess.run(
            pipeline,
            shell=True,
            executable="/bin/bash",
            capture_output=True,
            text=True,
            timeout=timeout
        )

    async def runUpgradeAgentCommand(self, cmd, args=None, timeout=None):
        if args is None:
            args = []

        self.logger.debug('Executing command: {} with args: {}, timeout: {}'.format(cmd, args, timeout))
        try:
            process = await asyncio.create_subprocess_exec(
                cmd,
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
                result = {
                    "exitCode": process.returncode,
                    "stdOut": stdout.decode('utf-8').rstrip(),
                    "stdErr": stderr.decode('utf-8').rstrip(),
                    "isError": process.returncode != 0
                }
                self.logger.debug('Command execution result: {}'.format(result))
                return result
            except asyncio.TimeoutError:
                process.kill()
                self.logger.error(f'Command execution timed out after {timeout} seconds: {cmd}')
                return {"isTimeout": True}

        except subprocess.TimeoutExpired:
            self.logger.error(f'Command execution timed out after {timeout} seconds: {cmd}')
            return {"isTimeout": True}
        except subprocess.SubprocessError as e:
            self.logger.error(f'Subprocess error executing command {cmd}: {e}')
            return {"isException": True, "exception": str(e)}
        except Exception as e:
            self.logger.error(f'Unexpected error executing command {cmd}: {e}')
            return {"isException": True, "exception": str(e)}

    def updateKeepAliveIntervalIfNeeded(self, keepaliveInterval):
        if keepaliveInterval and keepaliveInterval != self.keepaliveInterval:
            self.logger.debug(f'Updating keepalive interval from {self.keepaliveInterval} to {keepaliveInterval}')
            self.keepaliveInterval = keepaliveInterval

    def buildMessage(self, messageType, payload, messageTypeVersion=1):
        message = {
            'messageType': messageType,
            'messageTypeVersion': messageTypeVersion,
            'messageSequence': self.messageSequence,
            'originType': 'UPGRADE_AGENT',
            'keepaliveInterval': self.keepaliveInterval,
            'payload': payload
        }

        message.update(self.getComponentSpecificMessageHeaders())

        return message

    # UPGRADE AGENT -> MGMT
    def producePeriodicReports(self):
        if not self.lastKeepAliveTime or (self.upgradeAgentToken >= 0 and isIntervalElapsed(self.lastKeepAliveTime, self.keepaliveInterval)):
            self.sendKeepaliveMessage()
        else:
            if (not self.lastDataCollectionCheckTime or isIntervalElapsed(self.lastDataCollectionCheckTime,
                                                                          self.dataCollectionCheckInterval)) and isIntervalElapsed(self.lastKeepAliveTime,
                                                                                                                                   MIN_KEEPALIVE_INTERVAL):
                data = self.collectData()
                currentMessageMD5 = calculateMD5(data)
                if currentMessageMD5 != self.lastMessageMD5:
                    self.logger.debug(f'Detected data change, sending a new keepalive message with updated data')
                    self.lastMessageMD5 = currentMessageMD5
                    self.sendKeepaliveMessage(data)

                self.lastDataCollectionCheckTime = datetime.datetime.now()

    def sendKeepaliveMessage(self, data=None):
        try:
            if data is None:
                data = self.collectData()
                self.lastMessageMD5 = calculateMD5(data)

            payload = {
                'health': 'healthy',
                **data
            }
        except Exception as e:
            self.logger.error(f"Failed to collect data: {str(e)}")
            payload = {
                'health': 'critical',
                'healthError': str(e)
            }
        finally:
            payload['featureCompatibilityVersion'] = self.featureCompatibilityVersion

        message = self.buildMessage(messageType=MessageTypes.UPGRADE_AGENT_KEEPALIVE, payload=payload)

        self.logger.debug(f'Going to send keepalive message, token: {message["upgradeAgentToken"]}, messageSequence: {message["messageSequence"]}')
        self.produceMessageToTopic(message, MANAGEMENT_TOPIC_NAME)
        self.lastKeepAliveTime = datetime.datetime.now()

    def getCommandResultMessage(self, resultData):
        payload = {
            'upgradeStepID': resultData.get('upgradeStepID'),
            'command': resultData.get('command'),
            'verificationCommand': resultData.get('verificationCommand'),
            'success': resultData.get('success', False),
            'isTimeout': resultData.get('isTimeout', False),
            'isError': resultData.get('isError', False),
            'isVerification': resultData.get('isVerification', False)
        }
        return self.buildMessage(messageType=MessageTypes.UPGRADE_AGENT_COMMAND_RESULT, payload=payload)

    def sendCommandResultMessage(self, resultData):
        message = self.getCommandResultMessage(resultData)
        self.logger.debug(f"Going to send command result message for upgradeStepID: {resultData.get('upgradeStepID')} success: {resultData.get('success', False)}")
        self.produceMessageToTopic(message, MANAGEMENT_TOPIC_NAME)

    async def sendCommandResultMessageAwaitAck(self, resultData):
        message = self.getCommandResultMessage(resultData)
        self.logger.debug(f"Going to send (await ack) command result message for upgradeStepID: {resultData.get('upgradeStepID')} success: {resultData.get('success', False)}")
        await self.produceMessageToTopicAwaitAck(message, MANAGEMENT_TOPIC_NAME)

    def commitOffset(self, kafkaCtx):
        tp = TopicPartition(kafkaCtx['topic'], kafkaCtx['partition'], kafkaCtx['offset'] + 1)
        try:
            self.consumer.commit(offsets=[tp])
        except Exception as e:
            self.logger.error(f'Commit failed for topic {kafkaCtx["topic"]} partition {kafkaCtx["partition"]} offset {kafkaCtx["offset"] + 1}: {e}')
            raise e

    def setJournalState(self, upgradeStepID, state, kafkaCtx, entry=None, extra=None):
        if entry is None:
            entry = self.journal['commands'].get(upgradeStepID) or {
                'upgradeStepID': upgradeStepID,
                'state': state,
                'stateTransitions': {},
            }
        entry['state'] = state
        entry['stateTransitions'][state + 'At'] = getDateTime()

        entry['topic'] = kafkaCtx.get('topic')
        entry['partition'] = kafkaCtx.get('partition')
        entry['offset'] = kafkaCtx.get('offset')

        if extra:
            entry.update(extra)

        self.journal['commands'][upgradeStepID] = entry
        self.saveJournal()
        return entry

    async def handleReplay(self, upgradeStepID, entry, kafkaCtx):
        isReplay = False

        if not entry:
            return isReplay

        state = entry['state']

        if state == JournalStates.STARTED:
            return isReplay
        if state == JournalStates.RESULT_READY:
            resultPayload = entry.get('result')
            await self.produceResultAndFinalize(upgradeStepID, resultPayload, entry, kafkaCtx)
        if state in (JournalStates.RESPONSE_PRODUCED, JournalStates.COMPLETED):
            self.commitOffset(kafkaCtx)

            if state == JournalStates.RESPONSE_PRODUCED:
                self.setJournalState(upgradeStepID, JournalStates.COMPLETED, kafkaCtx, entry)

        isReplay = True
        return isReplay

    async def produceCommandResultMessage(self, upgradeStepID, resultPayload, entry, kafkaCtx):
        await self.sendCommandResultMessageAwaitAck(resultPayload)
        entry = self.setJournalState(upgradeStepID, JournalStates.RESPONSE_PRODUCED, kafkaCtx, entry)
        return entry

    def commitCommandResultMessage(self, upgradeStepID, entry, kafkaCtx):
        self.commitOffset(kafkaCtx)
        self.setJournalState(upgradeStepID, JournalStates.COMPLETED, kafkaCtx, entry)

    async def produceResultAndFinalize(self, upgradeStepID, resultPayload, entry, kafkaCtx):
        entry = self.setJournalState(upgradeStepID, JournalStates.RESULT_READY, kafkaCtx, entry, {'result': resultPayload})
        entry = await self.produceCommandResultMessage(upgradeStepID, resultPayload, entry, kafkaCtx)
        self.commitCommandResultMessage(upgradeStepID, entry, kafkaCtx)

    async def handleAgentCommand(self, upgradeStepID, commandObj, entry, kafkaCtx):
        """
        Execute a command and produce the result.
        """
        self.isExecutingCommand = True

        try:
            command = commandObj.get('cmd')

            # Set state to STARTED
            entry = self.setJournalState(upgradeStepID, JournalStates.STARTED, kafkaCtx, entry, {'command': commandObj})

            # Execute the command
            if command == '<install>':
                commandRes = await self.handleInstallCommand(commandObj)
            elif command == '<agent-restart>':
                commandRes = self.handleAgentRestartCommand()
            else:
                commandArgs = commandObj.get('args')
                commandTimeout = commandObj.get('timeout')
                commandRes = await self.runUpgradeAgentCommand(command, commandArgs, commandTimeout)

            # Process the result
            isTimeout = commandRes.get('isTimeout')
            isError = commandRes.get('isError')
            isException = commandRes.get('isException')
            success = not isTimeout and not isError and not isException

            resultPayload = {
                'upgradeStepID': upgradeStepID,
                'command': commandRes,
                'success': success,
                'isTimeout': isTimeout,
                'isError': isError,
                'isException': isException
            }

            # If the agent is restarting, we will produce the result after the agent is restarted.
            if self.isRestartingAgent:
                entry = self.setJournalState(upgradeStepID, JournalStates.RESULT_READY, kafkaCtx, entry, {'result': resultPayload})
            else:
                await self.produceResultAndFinalize(upgradeStepID, resultPayload, entry, kafkaCtx)
        finally:
            self.isExecutingCommand = False


    def extractKafkaContext(self, entry):
        topic = entry.get('topic')
        partition = entry.get('partition')
        offset = entry.get('offset')
        return {'topic': topic, 'partition': partition, 'offset': offset}


    # MGMT -> UPGRADE AGENT
    async def handleMessage(self, message, kafkaCtx):
        messageType = message.get('messageType')
        payload = message.get('payload')
        token = payload.get('upgradeAgentToken')

        if token is not None and token < self.upgradeAgentToken:
            self.logger.warning(f'Received a message with an older token: {token}')
        else:
            if token is not None and token > self.upgradeAgentToken:
                self.logger.debug(f'Updating upgrade agent token from {self.upgradeAgentToken} to {token}')
                self.upgradeAgentToken = token
                # persist token
                self.journal['currentToken'] = token
                self.saveJournal()

            if messageType == MessageTypes.UPDATE_UPGRADE_AGENT_KEEPALIVE_TOKEN:
                self.handleUpdateKeepaliveToken(payload)
                self.commitOffset(kafkaCtx)
            elif messageType == MessageTypes.UPGRADE_AGENT_COMMAND:
                # defer commit to the command handler (after produce ack)
                await self.handleUpgradeAgentCommand(payload, kafkaCtx)
            else:
                self.logger.warning(f'Unable to handle message with messageType {messageType}. Ignoring this message...')


    def handleUpdateKeepaliveToken(self, payload):
        keepaliveInterval = payload.get('keepaliveInterval')
        token = payload.get('upgradeAgentToken')
        additionalData = payload.get('additionalData')
        messageSequence = payload.get('messageSequence')

        self.logger.debug(f'Got update upgrade agent keepalive token message with keepaliveInterval: {keepaliveInterval} token: {token}')

        if messageSequence is not None and messageSequence + 1 > self.messageSequence:
            newMessageSeq = messageSequence + 1
            self.logger.debug(f'Updating upgrade agent messageSequence from {self.messageSequence} to {newMessageSeq}')
            self.messageSequence = newMessageSeq

        if additionalData:
            self.additionalData = additionalData

        self.updateKeepAliveIntervalIfNeeded(keepaliveInterval)
        self.sendKeepaliveMessage()

    async def handleVerificationCommand(self, verificationCommand, upgradeStepID, entry, kafkaCtx):
        cmdRes = self.runCommand(verificationCommand)
        verificationRes = {
            'exitCode': cmdRes.returncode,
            'stdOut': cmdRes.stdout.rstrip(),
            'stdErr': cmdRes.stderr.rstrip(),
            'isError': cmdRes.returncode != 0
        }
        isError = verificationRes.get('isError')
        success = not isError

        self.logger.debug(f'Verification command exit code: {verificationRes.get("exitCode")}, stdOut: {verificationRes.get("stdOut")}')

        if verificationRes.get('exitCode') == 0:
            self.logger.debug(f'Command verification succeeded, skipping execution for upgradeStepID: {upgradeStepID}')
            resultPayload = {
                'upgradeStepID': upgradeStepID,
                'verificationCommand': verificationRes,
                'success': success,
                'isError': isError,
                'isVerification': True
            }
            await self.produceResultAndFinalize(upgradeStepID, resultPayload, entry, kafkaCtx)
            return


    async def handleUpgradeAgentCommand(self, payload, kafkaCtx=None):
        commandObj = payload.get('command')
        verificationCommand = commandObj.get('verificationCommand')
        upgradeStepID = payload.get('upgradeStepID')

        # Ensure journal entry exists or load current
        entry = self.journal['commands'].get(upgradeStepID)

        if await self.handleReplay(upgradeStepID, entry, kafkaCtx):
            return

        if verificationCommand:
            return await self.handleVerificationCommand(verificationCommand, upgradeStepID, entry, kafkaCtx)

        await self.handleAgentCommand(upgradeStepID, commandObj, entry, kafkaCtx)

    async def handleInstallCommand(self, commandObj):
        installCommandFiles = commandObj.get('args')
        commandTimeout = commandObj.get('timeout')
        fullPaths = [os.path.join(self.artifactsDir, file) for file in installCommandFiles]
        resolvedFiles = []
        errors = {}

        for filePath in fullPaths:
            matchedFiles = glob.glob(filePath)
            if len(matchedFiles) > 1:
                self.logger.error(f'Multiple files found for {filePath}, cannot proceed')
                errors[filePath] = 'Multiple files found'
            elif len(matchedFiles) == 0:
                self.logger.error(f'File {filePath} does not exist')
                errors[filePath] = 'File does not exist'
            else:
                resolvedFiles.append(matchedFiles[0])

        if errors:
            return {
                'stdErr': f'Errors found for installation files: {json.dumps(errors)}',
                'success': False,
                'isError': True
            }

        if self.isRHELBased():
            self.logger.debug(f'Executing RPM install for files: {", ".join(resolvedFiles)}')
            return await self.runUpgradeAgentCommand('dnf', ['install', '-y', *resolvedFiles], commandTimeout)
        elif self.isUbuntuBased():
            self.logger.debug(f'Executing APT install for files: {", ".join(resolvedFiles)}')
            return await self.runUpgradeAgentCommand('apt-get', ['install', '-y', *resolvedFiles], commandTimeout)

    def handleAgentRestartCommand(self):
        self.logger.debug('Received agent restart command, sending systemctl restart command to systemd to restart the service...')

        try:
            subprocess.Popen(
                ["systemctl", "restart", "nvmeshupgradeagent.service"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )
        except Exception as e:
            self.logger.error(f'Error during agent restart command: {e}')
            return {
                'exitCode': 1,
                'stdOut': '',
                'stdErr': str(e),
                'isError': True
            }

        self.isRestartingAgent = True
        self.exitOnNextIteration()

        return {
            'exitCode': 0,
            'stdOut': 'Upgrade agent restarted successfully',
            'stdErr': '',
            'isError': False
        }


def setupRemoteDebugging(debug_host, debug_port, wait_for_client=False):
    if not DEBUGPY_AVAILABLE:
        print("❌ debugpy not available. Install with: pip install debugpy")
        return False

    try:
        # Configure debugpy
        debugpy.configure(subProcess=False)

        # Listen for debugger connections
        debugpy.listen((debug_host, debug_port))
        print(f"🐛 Debug server started on {debug_host}:{debug_port}")
        print(f"   Connect your debugger to {debug_host}:{debug_port}")

        if wait_for_client:
            print("⏳ Waiting for debugger to attach...")
            debugpy.wait_for_client()
            print("✅ Debugger attached!")
        else:
            print("🔄 Continuing without waiting for debugger")

        return True

    except Exception as e:
        print(f"❌ Failed to setup remote debugging: {e}")
        return False

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='NVMesh Upgrade Agent with remote debugging support',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable remote debugging with debugpy'
    )

    parser.add_argument(
        '--wait-for-debugger',
        action='store_true',
        help='Wait for debugger to attach before starting agent'
    )

    return parser.parse_args()

def main():
    args = parse_arguments()

    # Setup remote debugging if requested
    if args.debug:
        success = setupRemoteDebugging(
            debug_host='0.0.0.0',
            debug_port=5678,
            wait_for_client=args.wait_for_debugger
        )
        if not success:
            print("Continuing without debugging...")

    # Start the upgrade agent
    try:
        upgradeAgent = UpgradeAgent()
        asyncio.run(upgradeAgent.start())
    except Exception as e:
        import traceback
        upgradeAgent.logger.error(f"Upgrade agent crashed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
