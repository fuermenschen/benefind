# Local development

## Prerequisites

- Python 3.12+
- `uv`
- OpenAI API key

## Setup

```bash
# Clone
git clone https://github.com/fuermenschen/benefind.git
cd benefind

# Install dependencies
uv sync

# Set up API keys
cp .env.example .env
# Edit .env and add your OpenAI API key
```

## Notes

- Runtime outputs are written to `data/` (gitignored).
- Use `config/settings.local.toml` for machine-local overrides.
