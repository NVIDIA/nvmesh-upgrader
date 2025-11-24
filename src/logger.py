import logging
import sys

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(levelname)s: [%(name)s] %(message)s')
handler.setFormatter(formatter)


def getLogger(name):
	logger = logging.getLogger(name)
	if not logger.handlers:
		logger.addHandler(handler)
	logger.setLevel(logging.DEBUG)
	return logger
