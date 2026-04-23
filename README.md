git add src/v2/
git commit -m "Add v2 pipeline"
git push




# COREP_check

Single canonical project to read COREP files, evaluate rules, and explain rule logic.

## Canonical block flow (one structure)

Use `src/corep_blocks.py` as the **only public block API**.

### Block 1 — `block_process_1_extract_values`
- Purpose: get COREP values (`get_RC` reusable block)
- Input:
	- `templates_used`, `tables`, `rows`, `columns`, `sheets`, `corep_dir`
- Output:
	- `{template: {file_path, tables: {table_or_sheet: {sheet_name, dataframe}}}}`

Table-to-sheet resolution uses `src/data/mapping_table.xlsx` first (columns: `tables_input`, `tables_output`).
If no mapping match is found, it falls back to automatic marker-based sheet detection.

### Block 2 — `block_process_2_prepare_data`
- Purpose: create missing workbook stubs + seed deterministic values
- Input:
	- `config_path`, `sheet_name`, `corep_dir`, `overwrite_seed`
- Output:
	- `{"stubs": {...}, "seed": {...}}`

### Block 3 — `block_process_3_evaluate_single_rule`
- Purpose: debug one rule row
- Input:
	- `rule_row`, `data_mapping` (optional), `corep_dir`
- Output:
	- `{"rule_id", "status", "reason", "details"}`

### Block 4 — `block_process_4_evaluate_config`
- Purpose: evaluate config and return raw list
- Input:
	- `config_path`, `sheet_name`, `corep_dir`, `max_rules`
- Output:
	- `List[rule_result_dict]`

### Block 5 — `block_process_5_build_outputs`
- Purpose: evaluate config and write output files
- Input:
	- `config_path`, `sheet_name`, `corep_dir`, `output_dir`, `max_rules`
- Output:
	- summary dict with `status_counts`, `xlsx_path`, `json_path`

### Block 6 — `block_process_6_explain_logic`
- Purpose: produce human-readable rule interpretation workbook
- Input:
	- `config_path`, `sheet_name`, `output_path`, `max_rules`
- Output:
	- summary dict with `rules_processed`, `mappings_processed`, `output_path`

## Workflow function

`run_block_workflow(...)` calls blocks in order:
1. `block_process_2_prepare_data`
2. `block_process_5_build_outputs`
3. `block_process_6_explain_logic`

## Files in use

- `src/corep_blocks.py` → canonical block API
- `src/pipeline.py` → CLI that calls each block process
- `src/get_RC_value.py` → extraction internals
- `src/rule_engine.py` → rule parsing/evaluation engine
- `src/run_pipeline.py` → XLSX/JSON output builder
- `src/explain_rule_logic.py` → logic interpretation workbook
- `src/create_stub_workbooks.py` → missing-file preparation
- `src/seed_corep_values.py` → deterministic data seeding

## CLI commands

Show block contracts:
```bash
.venv/bin/python -m src.pipeline specs
```

Run each block process directly:
```bash
.venv/bin/python -m src.pipeline extract --templates-used C07.00 --tables C07.00.a --rows 0010 --columns 0210
.venv/bin/python -m src.pipeline prepare
.venv/bin/python -m src.pipeline run --max-rules 400
.venv/bin/python -m src.pipeline explain --max-rules 400
```

Run the full workflow:
```bash
.venv/bin/python -m src.pipeline all --max-rules 400
```

Quick in-memory self-test:
```bash
.venv/bin/python -m src.pipeline test
```

## Output files

- `outputs/rule_results.xlsx`
- `outputs/rule_results.json`
- `outputs/rule_logic_audit.xlsx`
# COREP_CHECK
