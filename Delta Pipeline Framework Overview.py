#!/usr/bin/env python

from docx import Document

doc = Document("template.docx")

def add_para(text, style, keep_next=True, keep_together=True):
    p = doc.add_paragraph(text, style=style)
    pf = p.paragraph_format
    if keep_next:
        pf.keep_with_next = True
    if keep_together:
        pf.keep_together = True
    return p

add_para("Delta Pipeline Framework Overview", "_Title")

add_para(
    "This document describes a generalized Delta Lake pipeline framework suitable for ingesting, transforming, and writing structured data. "
    "The design separates the pipeline into bronze, silver, and gold layers, with each table's logic defined in JSON configuration files. "
    "Python functions handle reusable logic, and Databricks notebooks orchestrate the workflow.",
    "Normal"
)

add_para(
    ".\n"
    "├─ layer_01_bronze/      # raw ingestion configs\n"
    "├─ layer_02_silver/      # cleaned & deduplicated configs\n"
    "├─ layer_03_gold/        # final tables\n"
    "├─ functions/            # PySpark helper modules\n"
    "│  ├─ read.py, transform.py, write.py, history.py\n"
    "│  ├─ internal/          # sanity checks & rescue helpers\n"
    "│  └─ catch_up/          # experimental catch‑up code\n"
    "├─ utilities/            # notebooks and shell scripts\n"
    "├─ dashboards/           # data exploration notebooks\n"
    "├─ sandbox/              # ad‑hoc notebooks\n"
    "└─ *.ipynb               # workflow notebooks (00_job_settings …)",
    "_Console",
    keep_next=False,
    keep_together=False
)

add_para("Configuration Files", "_Heading 1")

add_para(
    "Each dataset's settings live in JSON files under each layer directory. These configurations map directly to the pipeline's Python function modules. "
    "An example configuration illustrates how to define read, transform, and write operations for any given table:",
    "Normal"
)

add_para(
    '{\n'
    '    "read_function": "functions.stream_read_table",\n'
    '    "transform_function": "functions.silver_scd2_transform",\n'
    '    "write_function": "functions.stream_upsert_table",\n'
    '    "upsert_function": "functions.microbatch_scd2_upsert",\n'
    '    "src_table_name": "source_layer.table_name",\n'
    '    "dst_table_name": "target_layer.table_name",\n'
    '    "business_key": ["id", "key", "systemId"],\n'
    '    "surrogate_key": ["attribute", …],\n'
    '    "use_row_hash": true,\n'
    '    "row_hash_col": "row_hash",\n'
    '    "writeStreamOptions": {\n'
    '        "mergeSchema": "false",\n'
    '        "checkpointLocation": "/Volumes/framework/silver/checkpoints/table_name",\n'
    '        "delta.columnMapping.mode": "name"\n'
    '    },\n'
    '    "ingest_time_column": "derived_ingest_time"\n'
    '}',
    "_Computer",
    keep_next=False,
    keep_together=False
)

add_para("Core Functions", "_Heading 1")

add_para(
    "All reusable ETL functions are registered in functions/__init__.py, enabling notebooks to dynamically reference them by string path.",
    "Normal"
)

add_para("Key operations include:", "Normal")
add_para("Layered Transformation – cleans and standardizes columns, attaches metadata, and generates timestamps.", "Thin")
add_para("Microbatch SCD2 Upsert – enables dimension management and historical tracking in the silver layer.", "Thin")
add_para("History Reconstruction – reads Delta logs and replays file-level operations for traceability.", "Thin")
add_para("Downloader Utility – fetches source files and arranges them into staging directories.", "Thin")
add_para(
    "The internal module also includes validation tools to rebuild __init__.py and check JSON configurations.",
    "Normal",
    keep_next=False,
    keep_together=False
)

add_para("Notebook Workflow", "_Heading 1")

add_para("The notebook suite implements the full orchestration of the framework:", "Normal")
add_para("00_job_settings – gathers table configurations.", "_Console")
add_para("01_rebuild_init_files – rebuilds function entry points.", "_Console")
add_para("02_sanity_check – performs config validation.", "_Console")
add_para("03_ingest – runs ingestion jobs.", "_Console")
add_para("06_bad_records / 06_history – runs diagnostics and history checks.", "_Console")
add_para(
    "Additional notebooks manage checkpoints, table recovery, and exploratory data validation.",
    "Normal",
    keep_next=False,
    keep_together=False
)

add_para(
    "This modular framework allows scalable ingestion and transformation pipelines by externalizing logic into configurations and enabling reuse of tested ETL functions.",
    "Normal"
)

doc.save("Delta Pipeline Framework Overview.docx")

