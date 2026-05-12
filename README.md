# Throttled SMTP Relay Server

A local SMTP relay server that accepts incoming emails, queues them persistently
(up to 10,000), and relays them one-by-one to an upstream SMTP server with
configurable throttling (30s–120s jitter) and robust retry logic.

Built with **aiosmtpd** (inbound), **aiosmtplib** (outbound), and **SQLite**
(queue) — fully async, single-process, zero external dependencies beyond Python.

## Features

- **RFC 5321 / 5322 compliant** inbound SMTP server
- **Persistent SQLite queue** — survives restarts, up to 10,000 emails
- **Single-threaded relay** — strict one-by-one outbound delivery
- **Configurable throttling** — random jitter between 30s and 120s (customizable)
- **Smart retry logic** — exponential backoff for transient (4xx) failures, max 5 retries
- **Bounce handling** — logs + optional NDR email for permanent (5xx) failures
- **CLI admin tool** — inspect queue, list emails, clear by status
- **Test suite** — 25 tests (unit + integration) covering the full pipeline

## Architecture

```
Local Clients --> aiosmtpd (port 1025) --> SQLite Queue --> RelayWorker --> Upstream SMTP
                                                        (one-by-one + jitter)
```

| Component | File | Responsibility |
|-----------|------|----------------|
| Inbound SMTP | `src/smtp_handler.py` | aiosmtpd handler, receives mail, enqueues to SQLite |
| Queue | `src/queue_manager.py` | Async SQLite queue with FIFO, retry scheduling, capacity limit |
| Relay worker | `src/relay_worker.py` | Single async consumer, jitter delay, aiosmtplib relay, 4xx/5xx handling |
| Admin CLI | `src/admin.py` | Queue inspection and management commands |
| Config | `src/config.py` | YAML + environment variable configuration loader |

## Installation

### Prerequisites

- Python 3.10 or newer
- pip (or any PEP 517-compatible build tool)

### Install

```bash
git clone https://github.com/Pix13/throtsmtp.git
cd throtsmtp

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install in development mode (includes test dependencies)
pip install -e ".[test]"
```

This installs two CLI commands:

- `throt-relay` — start the relay server
- `throt-admin` — manage the email queue

## Configuration

### Option A: YAML config file

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your upstream SMTP details
```

Full example (`config.example.yaml`):

```yaml
local:
  host: "0.0.0.0"
  port: 1025
  hostname: "localhost"

upstream:
  host: "smtp.provider.com"
  port: 587
  username: ""
  password: ""
  tls: "starttls"      # "starttls" (port 587) or "ssl" (port 465)
  timeout: 30

throttle:
  min_delay: 30        # seconds
  max_delay: 120       # seconds

queue:
  db_path: "queue.db"
  max_size: 10000
  max_retries: 5
  retry_base: 60       # base backoff in seconds (doubles each retry)
  retry_cap: 3600      # maximum backoff in seconds (1 hour)

bounce:
  enabled: true
  from: "mailer-daemon@localhost"

logging:
  level: "INFO"
  file: "relay.log"
  max_bytes: 10485760  # 10 MB
  backup_count: 5
```

### Option B: Environment variables

All config values can be overridden with environment variables (see `.env.example`):

| Variable | Description |
|----------|-------------|
| `UPSTREAM_HOST` | Upstream SMTP host |
| `UPSTREAM_PORT` | Upstream SMTP port |
| `UPSTREAM_USERNAME` | Upstream username |
| `UPSTREAM_PASSWORD` | Upstream password |
| `UPSTREAM_TLS` | TLS mode: `starttls` or `ssl` |
| `THROT_LOCAL_PORT` | Local listening port |
| `THROT_MIN_DELAY` | Minimum throttle delay (seconds) |
| `THROT_MAX_DELAY` | Maximum throttle delay (seconds) |
| `THROT_DB_PATH` | SQLite database path |
| `THROT_MAX_QUEUE` | Maximum queue size |
| `THROT_MAX_RETRIES` | Maximum retry attempts |
| `THROT_BOUNCE_ENABLED` | Enable bounce emails (`true`/`false`) |
| `THROT_LOG_LEVEL` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `THROT_LOG_FILE` | Log file path |

## Usage

### Start the relay server

```bash
# Default config (config.yaml in current directory)
throt-relay

# Custom config file
throt-relay --config /path/to/config.yaml
```

The server listens on `0.0.0.0:1025` by default.

### Send a test email

```bash
# Using swaks (install: dnf install swaks / apt install swaks)
swaks --server 127.0.0.1 --to recipient@example.com \
      --from sender@localhost --body "Test message"

# Or using Python
python3 -c "
import smtplib
from email.mime.text import MIMEText

msg = MIMEText('Test message from relay')
msg['Subject'] = 'Test'
msg['From'] = 'sender@localhost'
msg['To'] = 'recipient@example.com'

s = smtplib.SMTP('localhost', 1025)
s.send_message(msg)
s.quit()
print('Sent!')
"
```

### Admin commands

```bash
# View queue statistics
throt-admin stats

# List emails (all statuses)
throt-admin list

# List specific status, limited count
throt-admin list --status queued -n 10
throt-admin list --status bounced

# Clear all emails of a given status (with confirmation)
throt-admin clear --status queued --yes

# Prune old sent and bounced emails
throt-admin prune --yes
```

## Retry Strategy

| Attempt | Delay Range | Notes |
|---------|-------------|-------|
| 1 | 60–120s | Base backoff |
| 2 | 120–240s | 2x |
| 3 | 300–600s | 5x |
| 4 | 600–1200s | 10x |
| 5 | 3600s | Capped at 1 hour |
| 6+ | Bounced | Max retries exceeded |

## SMTP Response Handling

- **2xx** — Success, mark as `sent`
- **4xx** — Transient failure, schedule retry with exponential backoff
- **5xx** — Permanent failure, mark as `bounced`, send NDR if enabled
- **Connection errors** — Transient, retry with backoff

## Testing

```bash
# Run all tests (unit + integration)
pytest

# Verbose output
pytest -v

# Only integration tests
pytest tests/test_integration.py -v -s
```

The test suite includes 25 tests:

- **Config tests** — defaults, validation, environment overrides
- **Queue manager tests** — enqueue/dequeue, capacity, retries, clearing
- **SMTP handler tests** — DATA handling, validation, Message-ID extraction
- **Relay worker tests** — sending, transient failure handling
- **Integration tests** — full pipeline (10 emails), queue capacity,
  transient retry, permanent bounce, FIFO ordering, retry exhaustion,
  queue persistence, existing queue processing, multiple recipients

## License

MIT
