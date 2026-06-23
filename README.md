# projects

Private development workspace. The **DbAssistant** database management tool
lives in [`DbAssistant/`](DbAssistant/) during local development.

## Public repository

DbAssistant is published from a dedicated public repository:

**https://github.com/dhananjay-chaturvedi/dbassistant**

- Documentation: https://dhananjay-chaturvedi.github.io/dbassistant/
- Install: `git clone https://github.com/dhananjay-chaturvedi/dbassistant.git`

To sync changes from this monorepo folder to the public repo, copy or push the
contents of `DbAssistant/` to the root of `dbassistant` (not the `DbAssistant/`
subfolder). See [`DbAssistant/FIRST_COMMIT.md`](DbAssistant/FIRST_COMMIT.md).

## Local development

```bash
cd DbAssistant
bash install.sh
source .venv/bin/activate
python dbtool.py modules
```

## License

**MIT License** — Copyright (c) 2026 Dhananjay Chaturvedi.
