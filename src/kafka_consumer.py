import uuid
import json
from src.logger import getLogger
from confluent_kafka import Consumer, TopicPartition


class KafkaConsumer:
	def __init__(self, config, consumerPollTimeout):
		self.consumerId = uuid.uuid4().hex[:8]
		self.logger = getLogger(f"Consumer.{self.consumerId}")
		self.config = config
		self.isClosing = False
		self.needsReload = False
		self.consumerPollTimeout = consumerPollTimeout
		self.config['error_cb'] = lambda err: self.onConsumerError(err, self.consumerId)
		self.consumer = Consumer(self.config)
		self.logger.debug(f'Consumer initialized')

	def assign(self, topics):
		self.consumer.assign(topics)
		self.logger.debug(f'Consumer assigned to topics: {topics}')

	def isSSLRelatedError(self, err):
		errorMessage = str(err)
		return "SSL" in errorMessage or "certificate" in errorMessage

	def onConsumerError(self, err, consumerId):
		self.logger.debug(f"Consumer {consumerId} error: {err}")

		if self.isSSLRelatedError(err) and not self.isClosing:
			if consumerId == self.consumerId:
				self.needsReload = True

	def getKafkaContext(self, message):
		return {
			'topic': message.topic(),
			'partition': message.partition(),
			'offset': message.offset(),
		}

	def consume(self):
		message = self.consumer.poll(timeout=self.consumerPollTimeout)

		if message is None:
			return None

		error = message.error()
		if error:
			self.logger.debug('Consumer.poll error occurred. code:{} error:{}'.format(error.code(), error.str()))
			return None

		self.logger.debug(f'Received a message! topic: {message.topic()}, offset: {message.offset()}')

		return message

	def decode(self, message):
		try:
			message_value = json.loads(message.value().decode('utf-8'))
		except Exception as e:
			self.logger.error(f'Failed to decode message! message value: {message.value()}, exception: {e}')
			return None

		self.logger.debug(f'Decoded message value: {message_value}')

		return message_value

	def rewind(self, message):
		self.logger.debug(f"Seeking back to offset {message.offset()} on partition {message.partition()} for replay")
		try:
			self.consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
		except Exception as seekErr:
			self.logger.error(f"Failed to seek to offset {message.offset()} for replay: {seekErr}")
			raise seekErr

	def commitOffset(self, kafkaCtx):
		tp = TopicPartition(kafkaCtx['topic'], kafkaCtx['partition'], kafkaCtx['offset'] + 1)
		try:
			self.consumer.commit(offsets=[tp])
		except Exception as e:
			self.logger.error(f'Commit failed for topic {kafkaCtx["topic"]} partition {kafkaCtx["partition"]} offset {kafkaCtx["offset"] + 1}: {e}')
			raise e

	def close(self):
		self.isClosing = True

		if self.consumer:
			try:
				self.logger.debug(f"Closing Consumer...")
				self.consumer.close()
			except Exception as e:
				self.logger.debug(f"Error while closing consumer, error: {e}")
			finally:
				self.consumer = None

		self.isClosing = False
