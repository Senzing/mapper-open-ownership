# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data mapper for the Open Ownership Register. It converts Open Ownership Register JSONL data into Senzing-compatible JSON format for entity resolution. The data source contains ~18 million entities representing beneficial ownership relationships.

## Commands

### Install dependencies

```bash
python -m venv ./venv
source ./venv/bin/activate
pip install --group all .
```

### Run the mapper

```bash
python src/oor_mapper.py -i <input_file.jsonl.gz> -o <output_file.jsonl.gz> [-l <log_file.json>]
```

### Lint

```bash
pylint $(git ls-files '*.py' ':!:docs/source/*')
black --check src/
flake8 src/
isort --check src/
mypy src/
bandit -r src/
```

## Architecture

### Core Mapper (`src/oor_mapper.py`)

The `mapper` class transforms Open Ownership Register statements into Senzing records:

- **Statement Types**:
  - `entityStatement` → Organizations with names, addresses, identifiers
  - `personStatement` → Individuals with names, birth dates, nationalities
  - `ownershipOrControlStatement` → Relationships linking persons/entities to entities

- **Key Methods**:
  - `map()` - Main entry point, routes by statement type
  - `map_entity()` / `map_person()` - Convert to Senzing format
  - `map_relationship()` - Creates `REL_POINTER` relationships
  - `map_addresses()` / `map_identifiers()` - Handle nested data structures

- **Data Flow**: Input is streamed, records cached by `RECORD_ID`, relationships merged onto their subject entities, then output as JSONL

### Senzing Integration

- Data source: `OPEN-OWNERSHIP`
- Relationships use `REL_ANCHOR_DOMAIN`/`REL_POINTER_DOMAIN` = "OOR"
- Config in `src/oor_config_updates.g2c` adds relationship date attributes

### Code Style

- Line length: 120 characters
- Uses black formatting with isort (black profile)
