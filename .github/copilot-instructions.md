Thadam's agent rules, including the website section, are in `AGENTS.md`; read it before changing code or the site.
Site build: `website/scripts/build.sh`; check: `python3 website/scripts/check.py` (enforces `Available`/`In progress`/`Planned` status labels and `ADR-0011` claims across `/` and `/docs/`); local preview: `website/scripts/preview.sh`.
Do not add deploy steps, wrangler config, or secrets; brand assets are canonical only in `thadam-assets/brand/`.
