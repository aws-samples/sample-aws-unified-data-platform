# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Config validation utility for UDM pipeline configurations."""

import logging

logger = logging.getLogger(__name__)


def validate_config_structure(cfg):
    """Validate the config structure enforces new array-based format."""

    # Enforce curation is a list (array format)
    curation_config = getattr(cfg, "curation", None)

    if not isinstance(curation_config, list):
        logger.info("✗ ERROR: 'curation' must be an array of target tables")
        return False

    logger.info("✓ Multi-target curation format detected")
    for i, target in enumerate(curation_config):
        target_name = getattr(target, "name", f"target_{i}")
        logger.info("  - Target: %s", target_name)
        logger.info("    Table: %s", getattr(target, 'target_table_name', 'N/A'))
        columns = getattr(target, "columns", [])
        logger.info("    Columns: %s defined", len(columns))
        if not columns:
            logger.info("    ✗ WARNING: No columns defined for %s", target_name)

    # Check if defaults and columns are at root level
    defaults = getattr(cfg, "defaults", None)
    columns = getattr(cfg, "columns", None)

    if defaults:
        logger.info("✓ Defaults found at root level")
        logger.info("  Types defined: %s", list(defaults.keys()))
    else:
        logger.info("✗ No defaults found at root level")
        return False

    if columns:
        logger.info("✓ Columns found at root level")
        logger.info("  Columns defined: %s", len(columns))
    else:
        logger.info("✗ No columns found at root level")
        return False

    return True
