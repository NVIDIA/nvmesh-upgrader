# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
./py_to_exec.sh

cd RPM
./buildrpm $BUILDRPM_FLAGS
