# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Common job utilities for Glue scripts."""


def convert_namespace_to_dict(obj):
    """Recursively convert SimpleNamespace objects to dictionaries"""
    if hasattr(obj, "__dict__"):
        obj = vars(obj)
    if isinstance(obj, dict):
        return {k: convert_namespace_to_dict(v) for k, v in obj.items()}
    return obj


def extract_error_message(exception):
    """Extract meaningful error message from exception."""
    error_msg = str(exception)
    if "\n" in error_msg:
        return error_msg.split("\n", maxsplit=1)[0].strip()
    return error_msg.strip()


def get_source_name(cfg):
    """Get source name based on source type."""
    return (
        cfg.source
        if cfg.source_type.lower() in ["xlsx", "csv", "json", "parquet"]
        else cfg.source_table_name
    )


def finalize_job_success(
    audit_manager, args, source_name, output_path, row_count, job_execution_time=None
):
    """Finalize successful job execution with audit logging."""
    audit_kwargs = {
        "stepfunction_execution_id": args["stepfunction_execution_id"],
        "source_table_name": source_name,
        "job_type": args["job_type"],
        "status": "Completed",
        "data_path": output_path,
        "no_of_rows_processed": row_count,
    }
    if job_execution_time:
        audit_kwargs["udm_processed_date"] = job_execution_time

    audit_manager.end_stage_job(**audit_kwargs)


def finalize_job_failure(audit_manager, args, source_name, exception):
    """Finalize failed job execution with audit logging."""
    audit_manager.end_stage_job(
        args["stepfunction_execution_id"],
        source_table_name=source_name,
        job_type=args["job_type"],
        status="Failed",
        failure_reason=extract_error_message(exception),
    )


def get_curation_targets(cfg):
    """Get curation targets as a list. Config must have curation as an array."""
    curation_config = getattr(cfg, "curation", None)
    if not isinstance(curation_config, list):
        raise ValueError("Config error: 'curation' must be an array of target tables")
    return curation_config
