#!/usr/bin/env python3

import os
import datetime
import hashlib
import json
from confluent_kafka import libversion


def getDateTime():
	"""Return current UTC time in ISO 8601 format with trailing 'Z'."""
	return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


def isIntervalElapsed(previousActionTime, interval):
	"""Check if specified interval has elapsed since previous action time."""
	return (datetime.datetime.now() - previousActionTime).total_seconds() > interval


def readFile(filename):
	"""Read and execute a Python configuration file, returning local variables."""
	globals_dict = {}
	locals_dict = {}

	if os.path.exists(filename):
		with open(filename) as f:
			exec(f.read(), globals_dict, locals_dict)

	return locals_dict


def isFileExists(path):
	"""Check if file exists at given path."""
	return os.path.isfile(path)


def getLinuxDistro():
	"""Parse /etc/os-release to get Linux distribution information."""
	linuxDistro = {}
	with open("/etc/os-release", "r") as f:
		for line in f:
			if "=" in line:
				key, val = line.strip().split("=", 1)
				linuxDistro[key] = val.strip('"')
	return linuxDistro


def extractVersion(fullVersion):
	"""Extract base version and release number from full version string."""
	if '-' not in fullVersion:
		return fullVersion

	base, rest = fullVersion.split('-', 1)

	release = rest.split('.', 1)[0]
	return f"{base}-{release}"


def getLibrdkafkaVersion():
	"""Get librdkafka version from confluent_kafka."""
	version = libversion()
	return version[0] if version else None


def calculateMD5(data):
	"""Calculate MD5 hash of JSON-serialized data."""
	return hashlib.md5(json.dumps(data).encode('utf-8')).hexdigest()


def readVersionFile(versionFilePath):
	"""Read version file and return a dictionary of key-value pairs."""
	result = {}
	with open(versionFilePath, "r") as f:
		for line in f:
			if "=" in line and line.strip():
				key, val = line.strip().split("=", 1)
				result[key] = val.strip('"')
	return result
