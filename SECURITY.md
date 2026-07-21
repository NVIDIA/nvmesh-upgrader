# Security Policy: NVMesh Upgrade Agent

NVIDIA is dedicated to the security and trust of our software products and
services, including all source code repositories managed through our
organization.

If you need to report a security issue, please use the appropriate contact
points outlined below. **Please do not report security vulnerabilities through
GitHub/GitLab issues or pull requests.**

## Reporting a Vulnerability

To report a potential security vulnerability in the NVMesh Upgrade Agent:

* **Web (preferred):** [NVIDIA Vulnerability Disclosure Program](https://www.nvidia.com/en-us/security/)
  — the preferred method for reporting security concerns across all NVIDIA
  products.
* **E-Mail:** [psirt@nvidia.com](mailto:psirt@nvidia.com)
  - We encourage you to use the following PGP key for secure email
    communication: [NVIDIA public PGP Key](https://www.nvidia.com/en-us/security/pgp-key)
* **GitHub:** Use this repository's **Security** tab > **Report a vulnerability**
  to submit a report directly.

If a security vulnerability is reported through public channels (issues, pull
requests, or discussions), maintainers may limit public discussion and redirect
the reporter to the appropriate private disclosure channels.

### What to Include in Your Report

Detailed reports help NVIDIA evaluate and address issues faster. Please include:

- Product/project name and version or branch affected
- Type of vulnerability (e.g., command injection, privilege escalation, denial
  of service)
- Step-by-step instructions to reproduce the issue
- Proof-of-concept code or exploit (if available)
- Potential impact assessment

**Detailed reports help NVIDIA evaluate and address issues faster.**

NVIDIA's Product Security Incident Response Team (PSIRT) will acknowledge receipt
of your report, validate the vulnerability and assess severity, develop and test
a fix, and publish a security bulletin as appropriate. While NVIDIA does not
currently operate a public bug bounty program, externally reported issues receive
acknowledgement under our coordinated vulnerability disclosure policy. For ongoing
updates, subscribe at the [NVIDIA Product Security](https://www.nvidia.com/en-us/security/)
portal.

## Security Architecture & Context

The NVMesh Upgrade Agent is a background **service** component of NVMesh
(NVIDIA's distributed block storage product). It runs as a long-lived,
privileged systemd service (`nvmeshupgradeagent.service`, `ExecStart` in
`systemd/nvmeshupgradeagent.service`) on each NVMesh node. Its job is to receive
upgrade and maintenance instructions from the NVMesh management server over
Kafka, execute them locally (package installs, verification commands, service
restarts), and report node state and command results back over Kafka.

This software operates at the **Service** level. Its primary security
responsibility is to faithfully execute only legitimate, management-issued
upgrade commands on the host while protecting the integrity of the Kafka control
channel and the local execution environment. Because the agent runs commands and
installs packages with system privileges, it is effectively a remote command
execution surface for whatever principal can reach its Kafka command topic.

**Repository Exposure Classification:** Public.
Basis: the repository is Apache-2.0 licensed (`LICENSE`) and the `README.md`
lists a public NVIDIA GitHub mirror for this project, so this document is world-readable and is written to
public-safe detail.

**Service Exposure Classification:** External / Regulated (high confidence).
Basis: an externally distributed, commercially supported enterprise storage
product component, packaged and shipped to customers as RPM/DEB packages.

### Key Boundaries and Interfaces

- **Kafka control channel (primary, untrusted input):** the agent consumes
  command messages from a per-host topic (`<hostname>.upgradeAgent.commands.1.0.0`)
  and produces keepalive/result messages to management topics
  (`src/upgradeagent.py`, `src/kafka_consumer.py`, `src/kafka_producer.py`).
  Messages are JSON, decoded in `KafkaConsumer.decode`.
- **Local configuration:** `/etc/nvmesh/upgradeagent.conf` and
  `/etc/nvmesh/nvmesh.conf`, loaded via `utils.readFile` (note: parsed with
  Python `exec`).
- **Command execution:** `subprocess.run(..., shell=True)` and
  `asyncio.create_subprocess_exec` in `src/upgradeagent.py`.
- **Local state:** a JSON journal at `/var/opt/nvmesh/upgradeagent/journal.json`
  (`src/journal_manager.py`, `src/fsatomic.py`) and TLS certificate material
  copied to a runtime directory (`copyCertificates`).
- **Optional remote debug listener:** `debugpy` server enabled via CLI flags in
  `src/main.py`.

### Threat Model

The following scenarios represent the primary security concerns for this project
(including auxiliary/support code):

1. **Transport authentication in the development-only non-mTLS configuration:**
   Production deployments require mutual TLS to the Kafka broker; the non-TLS
   mode (`KAFKA_TLS_ENABLED = "false"` in `upgradeagent.conf`) is a
   development-only configuration. When mTLS is disabled, the agent's trust in
   the origin of a command rests entirely on the network and broker: the
   `upgradeAgentToken` monotonic counter checked in `handleMessage` only rejects
   stale/older tokens and is not a cryptographic authenticator, so it does not
   prevent a spoofed or injected message from a party with topic access. This
   does not apply to production deployments, which authenticate the broker
   connection with mutual TLS.
2. **Arbitrary code execution through configuration files:** `utils.readFile`
   evaluates configuration with `exec(f.read())` on
   `/etc/nvmesh/upgradeagent.conf` and `/etc/nvmesh/nvmesh.conf`. Any actor able
   to write to these files (or trick the agent into reading an attacker-authored
   file) executes arbitrary Python as the service user at startup/reload.
3. **Privileged package installation from the artifacts directory:**
   `handleInstallCommand` globs files under `ARTIFACTS_DIR`
   (`/var/nvmesh/artifacts`) and runs `dnf install -y` / `apt-get install -y` on
   the resolved paths. A planted or tampered package in that directory is
   installed with root privileges; there is no signature/provenance check in this
   component.
4. **Exposed remote debugging interface:** `src/main.py` can start a `debugpy`
   listener (`--debug`, default port 5678). Binding it to a non-loopback address
   (`--debug-host 0.0.0.0`, explicitly documented in the help text) exposes an
   interface that permits arbitrary code execution to anyone who can reach the
   port.
5. **Sensitive data at rest and plaintext key password:** the journal
   (`/var/opt/nvmesh/upgradeagent/journal.json`) persists full command payloads
   and results in cleartext, and `KAFKA_SSL_KEY_PASSWORD` may be stored in
   plaintext in the config file and passed to librdkafka
   (`getKafkaSSLConfig`). Read access to these files can disclose operational
   detail and the TLS key passphrase.

### Accepted Risks

Previously triaged threats retained by team decision:

- **Remote command execution via Kafka command messages:** the core function of
  the agent is to run commands delivered over Kafka. `handleUpgradeAgentCommand`
  → `handleAgentCommand` → `runUpgradeAgentCommand` in `src/upgradeagent.py`
  execute the `cmd`, `args`, and `timeout` taken from the `upgradeAgentCommand`
  payload. This is the agent's intended function by design. — *Accepted
  (2026-07-21): remote command execution over Kafka is the intended design and
  was reviewed and approved by security; the risk is mitigated operationally by
  restricting Kafka command-topic access and enabling mutual TLS to the broker.*
- **Shell injection via verification commands:** `handleVerificationCommand`
  passes the message-supplied `verificationCommand` into `runCommand`, which
  invokes `subprocess.run(pipeline, shell=True, executable="/bin/bash")`, so a
  crafted verification string is interpreted by the shell. — *Accepted
  (2026-07-21): verification commands are supplied by the trusted management
  server over the same approved control channel as upgrade commands; this is
  part of the reviewed and security-approved design, mitigated operationally by
  restricting Kafka command-topic access and enabling mutual TLS to the broker.*

### Critical Security Assumptions

- **Trusted control channel:** assumes the Kafka broker and network are trusted
  and that only the legitimate NVMesh management server can produce to the
  agent's command topic. The agent performs no message-level authentication or
  authorization of command senders, and mTLS is optional/off by default.
- **Trusted local files:** assumes `/etc/nvmesh/upgradeagent.conf`,
  `/etc/nvmesh/nvmesh.conf`, the artifacts directory, and the journal are
  writable only by root/trusted administrators. Configuration files are executed
  as Python, and artifacts are installed with system privileges.
- **Well-formed, benign commands:** assumes the management server issues only
  intended commands; the agent does not sanitize or allow-list `cmd`, `args`, or
  `verificationCommand`.
- **Host OS enforces isolation and permissions:** assumes the OS enforces
  filesystem permissions on the journal, the TLS runtime directory (created mode
  `0700`), certificate/key files, and version files, and that process isolation
  is intact.
- **Debugging is dev-only:** assumes the `--debug` remote debug listener is used
  only in controlled development environments and is never bound to a
  non-loopback interface in production.
- **Trusted package provenance:** assumes upgrade artifacts placed in
  `ARTIFACTS_DIR` are authentic and integrity-verified upstream, since this
  component installs them without an independent signature check.

## Deployment Assumptions & Operational Guidance

- Enable mutual TLS for Kafka (`KAFKA_TLS_ENABLED = "true"`) in production and
  provision per-node client certificates; keep certificate verification enabled.
- Restrict who can produce to the per-node `upgradeAgent.commands` topic via
  broker ACLs so only the management server can issue commands.
- Protect `/etc/nvmesh/*.conf`, `ARTIFACTS_DIR`, and the journal directory with
  strict filesystem permissions (root-only write).
- Never enable `--debug` on production nodes, and never bind the debug listener
  to `0.0.0.0`.
- Prefer an encrypted key store or file permissions over an inline
  `KAFKA_SSL_KEY_PASSWORD` in the config file.

## Out of Scope

- The integrity and authenticity of upgrade packages/artifacts produced upstream
  (handled by the NVMesh build/release pipeline, not this agent).
- Security of the Kafka broker deployment and its ACL configuration.
- Vulnerabilities requiring pre-existing root/administrator access on the host
  (the agent already runs privileged and trusts local root-owned files).
