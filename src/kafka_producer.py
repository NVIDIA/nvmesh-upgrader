import uuid
import json
import asyncio
from logger import getLogger
from confluent_kafka import Producer


class KafkaProducer:
	def __init__(self, config, producerPollTimeout):
		self.producerId = uuid.uuid4().hex[:8]
		self.logger = getLogger(f"Producer.{self.producerId}")
		self.config = config
		self.isClosing = False
		self.needsReload = False
		self.producerPollTimeout = producerPollTimeout
		self.config['error_cb'] = lambda err: self.onProducerError(err, self.producerId)
		self.producer = Producer(self.config)
		self.logger.debug('Producer initialized')

	def isSSLRelatedError(self, err):
		errorMessage = str(err)
		return "SSL" in errorMessage or "certificate" in errorMessage

	def onProducerError(self, err, producerId):
		self.logger.debug(f"Producer error: {err}")

		if self.isSSLRelatedError(err) and not self.isClosing:
			if producerId == self.producerId:
				self.needsReload = True

	def produce(self, topic, message):
		def onProduce(err, msg):
			if err:
				self.logger.error(f'Produced message Failed! topic: {topic}, error: {err}')

		try:
			self.producer.produce(topic, value=json.dumps(message), callback=onProduce)
		except Exception as e:
			self.logger.error(f'Failed to produce message to topic: {topic}, message: {message}, ex: {str(e)}')
			raise e

	async def produceAwaitAck(self, topic, message):
		loop = asyncio.get_running_loop()
		future = loop.create_future()

		def _deliveryCallback(err, msg):
			if not future.done():
				if err is not None:
					future.set_exception(RuntimeError(f'Kafka delivery failed: {err}'))
				else:
					future.set_result(msg)

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
				self.producer.poll(self.producerPollTimeout)  # Poll for delivery events
				await asyncio.sleep(self.producerPollTimeout)

			return await future
		except Exception as e:
			self.logger.error(f'Failed to produce message to topic: {topic}, message: {message}, ex: {str(e)}')
			raise e

	def close(self):
		self.isClosing = True

		if self.producer:
			try:
				self.logger.debug("Closing Producer...")
				self.producer.flush(5)
			except Exception as e:
				self.logger.debug(f"Error flushing producer, error: {e}")
			finally:
				self.producer = None

		self.isClosing = False

