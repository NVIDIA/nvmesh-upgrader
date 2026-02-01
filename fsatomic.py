# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os

def _fsyncDir(path):
    """
    Fsync a directory to persist metadata updates (like rename) to stable storage.
    Why: On POSIX, directories are files. An atomic rename (os.replace) updates the
    directory's entries. To ensure that rename survives a crash, fsync the parent dir.
    """
    dirFD = os.open(path, os.O_RDONLY)
    try:
        os.fsync(dirFD)
    except Exception as e:
        raise Exception(f'Failed to fsync directory {path}: {e}')
    finally:
        os.close(dirFD)


def atomicWrite(path, dataBytes):
    """
    Atomically replace the file at 'path' with 'dataBytes'.

    Guarantees (on local POSIX filesystems like ext4/xfs):
    - Readers see either the old complete file or the new complete file; never partial.
    - Data durability across crashes once this function returns.

    Steps:
    1) Write to a temp file in the same directory (ensures same filesystem for rename).
    2) Flush Python buffers (f.flush()) and fsync the file (os.fsync) so content+size are durable.
    3) os.replace(temp, target) to atomically swap names.
    4) fsync the parent directory so the rename is durable.

    Caveats:
    - Atomic rename is guaranteed only within the same filesystem. We therefore create
      the temp file in the same directory as 'path'.
    """
    try:
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        tmpPath = path + '.tmp'

        with open(tmpPath, 'wb') as f:
            f.write(dataBytes)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmpPath, path)
        _fsyncDir(directory)
    except Exception as e:
        raise Exception(f'Failed to atomic write to {path}: {e}')


