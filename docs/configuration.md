# Configuration

All configuration lives in `config/`.

- `settings.toml`: general settings (thresholds, delays, model choice)
- `municipalities.toml`: list of municipalities in Bezirk Winterthur
- `prompts.toml`: LLM prompt templates for organization evaluation

For local machine overrides, create:

```text
config/settings.local.toml
```

The local settings file is gitignored.
