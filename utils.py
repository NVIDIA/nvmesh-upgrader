#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

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
    return (datetime.datetime.now() - previousActionTime).seconds > interval


def readFile(filename):
    """Read and execute a Python configuration file, returning local variables."""
    g = {}
    l = {}

    if os.path.exists(filename):
        exec(open(filename).read(), g, l)

    return l


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
