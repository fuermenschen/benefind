# Local development

## Prerequisites

- Python 3.12+
- `uv`
- Brave Search API key (for website discovery)
- Firecrawl API key (optional, for discover fallback)
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
# Edit .env and add:
# - BRAVE_API_KEY (discover)
# - FIRECRAWL_API_KEY (discover fallback, optional)
# - OPENAI_API_KEY (evaluate)
```

## Notes

- Runtime outputs are written to `data/` (gitignored).
- Use `config/settings.local.toml` for machine-local overrides.
