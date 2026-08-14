# ClaudeRouter

Local Director → Managers → Workers coding orchestrator. FastAPI backend, React UI, loopback only.

## Requirements

- Python 3.11 or 3.12
- Node.js 22
- Git

## Setup

```powershell
python -m pip install -r requirements-dev.txt
Push-Location frontend
npm ci
npm run build
Pop-Location
python scripts/orchestrator_admin.py migrate
python -m orchestrator.cli --port 8000
```

Open `http://127.0.0.1:8000`. Do not expose the port.

Runtime data: `~/.ai_orchestrator/`. Cookies: ignored `cookies/`. Copy `.env.example` and export vars in the shell. Never commit `.env`, cookies, or keys.

## Layout

```text
orchestrator/   Python runtime
frontend/       React + Vite UI
scripts/        admin and acceptance
tests/          backend tests
docs/           architecture and runbooks
server.py       FastAPI entry
```

## Test

```powershell
python -m pytest -q
.\scripts\run-acceptance.ps1 -Profile full -Label local
```

## Docs

- [Architecture](docs/architecture.md)
- [Deployment](docs/deployment.md)
- [Operations](docs/operations.md)
- [Security](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
