
# Tafsir GUI — Universal Semantic Annotation Pipeline

A universal pipeline for semantic annotation, classification, and rendering of entire books via a NiceGUI wizard and a FastAPI viewer.

Originally built for the [Tafsir pipeline](https://github.com/mu-mino/Auto-Classify-Hadiths) — a project for annotating Islamic Quranic exegesis (Tafsir) texts using Gemini. This repo generalises that pipeline so any structured book (SQLite-backed) can be annotated, reviewed, and rendered.

---

## What it does

- Guides users through the full annotation pipeline via a **10-step NiceGUI wizard** — no coding required
- **Pre-flight validation**: checks API key, Gemini cache, input file, output dir, DB access
- **Two annotation modes:**
  - **Legacy** — direct integration with the original Tafsir pipeline (`gemini_api`)
  - **Universal** — AI-driven schema + rules + guard derivation from any input book
- Generates and manages a **Gemini context cache** (cost-efficient for large books)
- Live run controls: pause / resume / cancel, auto-resume on rate-limit
- **FastAPI viewer app**: renders annotated books with semantic highlighting, tag filtering, and full-text search

---

## Showcase legacy version : Annotated UI

<img width="1800" height="1200" alt="output" src="https://github.com/user-attachments/assets/4247c60c-d402-48bc-85ec-f10aa1e2d710" />

---

## Quick start

```bash
pip install -r requirements.txt
```

**Run the NiceGUI wizard:**
```bash
python -m tafsir_gui.main
```

**Run the FastAPI viewer:**
```bash
uvicorn tafsir_gui.app:app --reload
# → http://localhost:8000
```

---

## Modes

### Universal mode
Derives annotation schema, validation rules, guards, and prompt from the input book automatically. Output artifacts versioned under `projects/<name>/`:
- `schema_v*.json`, `validation_rules_v*.json`, `repair_policy_v*.json`
- `guards_v*.json`, `prompt_v*.txt`, `validation_report.json`

### Legacy mode
Direct adapter into the original Tafsir pipeline (`pipeline/gemini_api`). Requires a SQLite book in the format used by the [classify](https://github.com/muhammed-emin-eser/classify) repo.

---

## Configuration

All secrets via environment variables (or `.env` file at repo root):

| Variable | Description |
|---|---|
| `GEMINI_API_KEY` | Primary Gemini API key |
| `GEMINI_CACHE_NAME` | Gemini context cache name |
| `GEMINI_MODEL_ID` | Model ID (default: `models/gemini-2.5-pro`) |
| `GEMINI_API_KEY_ROLLBACK` | Rollback key for second-pass correction |
| `SOURCE_DIR` | Override default input books directory |
| `ANNOTATED_DIR` | Override default output directory |

---

## Project layout

```
tafsir_gui/
  app.py               # FastAPI viewer entrypoint
  app_vars.py          # Tag colors, filter definitions, surah names
  main.py              # NiceGUI wizard entrypoint
  core/                # Runner, state machine, preflight, scheduler, events
  integrations/        # Pipeline adapters (legacy + universal)
  pipeline/            # Vendored pipeline modules from classify
    gemini_api/        # API-based annotation engine
    gemini_gui/        # Clipboard-based annotation engine (legacy GUI mode)
    gemini_common.py   # Shared guard logic, normalisation, XML helpers
    analysis/          # Rollback, divergence checks, text comparison
    config/            # Centralised path configuration
  ui/                  # NiceGUI pages and theme
  utils/               # Env, logging, file detection, DB helpers
  static/              # viewer.html + theme.css (served by FastAPI)
  storage/             # Reserved for metadata tables
  tests/               # Unit + E2E tests
  scripts/             # UI crawler, test runner
```

---

## Tests

```bash
pytest tests/
```

**E2E UI crawl:**
```bash
pip install -r tests/requirements.txt
playwright install
python -m tafsir_gui.scripts.crawl_ui
# --scenario rate_limit|error|invalid_key   mock specific runner paths
# --no-headless                              watch browser interact
```
