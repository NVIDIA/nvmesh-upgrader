<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# NVMesh Upgrade Agent

The NVMesh Upgrade Agent is a service component of NVMesh that handles system upgrades and maintenance operations through Kafka messaging. It provides a reliable way to manage and execute upgrade commands across NVMesh nodes.

## Configuration

Modify the `/etc/nvmesh/upgradeagent.conf` file for configuartion.

#### If `KAFKA_SERVERS` is not defined, the upgrade agent will attempt to establish a connection using the `KAFKA_SERVERS` specified in `/etc/nvmesh/nvmesh.conf`.

## Service Management

### Starting the Service

```bash
sudo systemctl start nvmeshupgradeagent

# Check service status
sudo systemctl status nvmeshupgradeagent
```

### Stopping the Service

```bash
sudo systemctl stop nvmeshupgradeagent
```

### Enabling Service at Boot

```bash
sudo systemctl enable nvmeshupgradeagent
```

### View service logs:

```bash
sudo journalctl -u nvmeshupgradeagent
```

## NVMesh Open Source Repositories List
* https://github.com/NVIDIA/nvmesh-documentation
* https://github.com/NVIDIA/nvmesh-utils
* https://github.com/NVIDIA/nvmesh-interop-db
* https://github.com/NVIDIA/nvmesh-kernel
* https://github.com/NVIDIA/nvmesh-management
* https://github.com/NVIDIA/nvmesh-upgrader
* https://github.com/Excelero/nvmesh-csi-driver

The NVMesh Roadmap is published in the documentation repo

