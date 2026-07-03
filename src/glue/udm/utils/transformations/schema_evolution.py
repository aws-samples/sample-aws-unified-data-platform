# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Schema evolution utilities for handling dynamic schema changes in struct columns."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, to_json
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructType


class SchemaEvolutionHandler:
    """Handles schema evolution for struct columns and nested data types."""

    @staticmethod
    def normalize_struct_to_string(dataframe: DataFrame, logger) -> DataFrame:
        """Convert struct fields and primitives to strings to maintain backward compatibility."""  # pylint: disable=line-too-long
        try:
            for field in dataframe.schema.fields:
                if isinstance(field.dataType, StructType):
                    # Convert structs to JSON strings
                    if field.name == "other":
                        dataframe = dataframe.withColumn(
                            field.name,
                            to_json(col(field.name), {"ignoreNullFields": "true"}),
                        )
                    else:
                        dataframe = dataframe.withColumn(
                            field.name,
                            to_json(col(field.name), {"ignoreNullFields": "false"}),
                        )
                elif not isinstance(field.dataType, (ArrayType, StringType)):
                    # Convert all non-array, non-string types to strings
                    dataframe = dataframe.withColumn(
                        field.name, col(field.name).cast(StringType())
                    )

            return dataframe
        except Exception as e:
            logger.info(f"Schema normalization failed: {e}")
            return dataframe

    @staticmethod
    def _process_struct_column(
        dataframe: DataFrame, struct_col: str, struct_type: StructType, logger
    ) -> DataFrame:
        """Process individual struct column for schema evolution."""
        for nested_field in struct_type.fields:
            if isinstance(nested_field.dataType, StructType):
                nested_name = nested_field.name
                struct_fields = [f.name for f in nested_field.dataType.fields]

                # Build coalesce expression for all struct fields
                coalesce_exprs = []
                for sf in struct_fields:
                    field_path = f"{struct_col}.{nested_name}.{sf}"
                    if sf.upper() == "STRING":
                        coalesce_exprs.append(col(field_path))
                    else:
                        coalesce_exprs.append(col(field_path).cast("string"))

                if coalesce_exprs:
                    dataframe = dataframe.withColumn(
                        struct_col,
                        col(struct_col).withField(
                            nested_name,
                            coalesce(
                                *coalesce_exprs, col(f"{struct_col}.{nested_name}")
                            ),
                        ),
                    )

        return dataframe

    @staticmethod
    def evolve_schema_for_target(
        dataframe: DataFrame, target_schema: StructType, logger
    ) -> DataFrame:
        """Evolve DataFrame schema to match target table schema."""
        try:
            # Get current and target column sets
            current_cols = set(dataframe.columns)
            target_cols = {field.name: field.dataType for field in target_schema.fields}

            # Add missing columns with null values
            for col_name, col_type in target_cols.items():
                if col_name not in current_cols:
                    dataframe = dataframe.withColumn(col_name, lit(None).cast(col_type))

            # Handle type mismatches and struct field incompatibilities
            for field in target_schema.fields:
                if field.name in current_cols:
                    current_field = next(
                        f for f in dataframe.schema.fields if f.name == field.name
                    )

                    # Check for struct field compatibility issues
                    if isinstance(current_field.dataType, StructType) and isinstance(
                        field.dataType, StructType
                    ):
                        if SchemaEvolutionHandler._has_struct_field_mismatch(
                            current_field.dataType, field.dataType, logger
                        ):
                            # Convert entire struct to JSON string to avoid mismatch

                            dataframe = (
                                SchemaEvolutionHandler._convert_struct_column_to_string(
                                    dataframe,
                                    field.name,
                                    current_field.dataType,
                                    logger,
                                )
                            )
                            # Mark schema update needed

                        else:
                            dataframe = SchemaEvolutionHandler._align_struct_types(
                                dataframe, field.name, field.dataType, logger
                            )
                    elif (
                        isinstance(current_field.dataType, StructType)
                        and field.dataType == StringType()
                    ):
                        # Convert struct column to string
                        dataframe = (
                            SchemaEvolutionHandler._convert_struct_column_to_string(
                                dataframe, field.name, current_field.dataType, logger
                            )
                        )
                    elif isinstance(field.dataType, StructType):
                        dataframe = SchemaEvolutionHandler._align_struct_types(
                            dataframe, field.name, field.dataType, logger
                        )

            return dataframe
        except Exception as e:
            logger.info(f"Schema evolution failed: {e}")
            return dataframe

    @staticmethod
    def _align_struct_types(
        dataframe: DataFrame, col_name: str, target_struct: StructType, logger
    ) -> DataFrame:
        """Align struct column types with target schema."""
        try:
            current_field = next(
                f for f in dataframe.schema.fields if f.name == col_name
            )
            if not isinstance(current_field.dataType, StructType):
                return dataframe

            current_struct = current_field.dataType

            # Process each field in target struct
            for target_field in target_struct.fields:
                field_name = target_field.name
                target_type = target_field.dataType

                # Find corresponding field in current struct
                current_field_type = None
                for cf in current_struct.fields:
                    if cf.name == field_name:
                        current_field_type = cf.dataType
                        break

                # Handle type conversion if needed
                if current_field_type and current_field_type != target_type:
                    if (
                        isinstance(current_field_type, StructType)
                        and target_type == StringType()
                    ):
                        # Convert struct to string
                        dataframe = (
                            SchemaEvolutionHandler._convert_nested_struct_to_string(
                                dataframe,
                                col_name,
                                field_name,
                                current_field_type,
                                logger,
                            )
                        )
                    elif current_field_type == StringType() and isinstance(
                        target_type, StructType
                    ):
                        # Convert string to struct (less common, handle carefully)
                        logger.info(
                            f"String to struct conversion not implemented for "
                            f"{col_name}.{field_name}"
                        )

            return dataframe
        except Exception as e:
            logger.info(f"Struct alignment failed for {col_name}: {e}")
            return dataframe

    @staticmethod
    def _convert_struct_column_to_string(
        dataframe: DataFrame, col_name: str, struct_type: StructType, logger
    ) -> DataFrame:
        """Convert entire struct column to JSON string."""
        try:
            # Preserve null values in JSON output
            dataframe = dataframe.withColumn(
                col_name, to_json(col(col_name), {"ignoreNullFields": "false"})
            )

            return dataframe
        except Exception as e:
            logger.info(f"Struct to JSON conversion failed for {col_name}: {e}")
            return dataframe

    @staticmethod
    def _convert_nested_struct_to_string(
        dataframe: DataFrame,
        struct_col: str,
        field_name: str,
        struct_type: StructType,
        logger,
    ) -> DataFrame:
        """Convert nested struct field to JSON string."""
        try:
            # Preserve null values in JSON output
            dataframe = dataframe.withColumn(
                struct_col,
                col(struct_col).withField(
                    field_name,
                    to_json(
                        col(f"{struct_col}.{field_name}"), {"ignoreNullFields": "false"}
                    ),
                ),
            )

            return dataframe
        except Exception as e:
            logger.info(f"Nested struct to JSON conversion failed: {e}")
            return dataframe

    @staticmethod
    def get_table_schema(spark, database_name: str, table_name: str) -> StructType:
        """Get existing table schema for comparison."""
        try:
            existing_table = spark.table(f"glue_catalog.{database_name}.{table_name}")
            return existing_table.schema
        except Exception:
            return None

    @staticmethod
    def _has_struct_field_mismatch(
        source_struct: StructType, target_struct: StructType, logger
    ) -> bool:
        """Check if source struct has extra fields not present in target struct."""
        try:
            source_fields = {f.name for f in source_struct.fields}
            target_fields = {f.name for f in target_struct.fields}

            extra_fields = source_fields - target_fields
            if extra_fields:
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking struct field mismatch: {e}")
            return True  # Assume mismatch to be safe

    @staticmethod
    def _spark_type_to_sql(spark_type) -> str:
        """Convert Spark data type to SQL type string."""
        if isinstance(spark_type, StructType):
            # Build struct type definition
            field_defs = []
            for field in spark_type.fields:
                field_type = SchemaEvolutionHandler._spark_type_to_sql(field.dataType)
                field_defs.append(f"{field.name}:{field_type}")
            return f"STRUCT<{','.join(field_defs)}>"
        elif spark_type == StringType():
            return "STRING"
        elif spark_type == LongType():
            return "BIGINT"
        elif spark_type == DoubleType():
            return "DOUBLE"
        else:
            return str(spark_type).upper()
