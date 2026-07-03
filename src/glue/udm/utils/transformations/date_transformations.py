# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    lower,
    regexp_replace,
    to_date,
    to_timestamp,
)


class DateTransformations:
    """Date and timestamp transformation operations"""

    @staticmethod
    def clean_date_strings(
        df: DataFrame, column: str, date_override_mapping: dict = None
    ) -> DataFrame:
        """Clean date strings by removing time components and invalid dates"""
        if date_override_mapping is None:
            date_override_mapping = {
                "no expiration date": "",
                "exp_date_not_available": "",
                "TBD": "",
            }

        result_col = regexp_replace(lower(col(column)), " 00:00:00", "")
        for invalid_date, replacement in date_override_mapping.items():
            result_col = regexp_replace(result_col, invalid_date, replacement)

        return df.withColumn(column, result_col)

    @staticmethod
    def convert_date_format(
        df: DataFrame, column: str, input_format: str = None, target_format: str = None
    ) -> DataFrame:
        """Convert date format from input to target format"""
        if input_format and target_format:
            df = df.withColumn(
                column, date_format(to_date(col(column), input_format), target_format)
            )
        elif target_format:
            df = df.withColumn(column, date_format(to_date(col(column)), target_format))

        return df

    @staticmethod
    def convert_timestamp_format(
        df: DataFrame, column: str, input_format: str = None, target_format: str = None
    ) -> DataFrame:
        """Convert timestamp format from input to target format"""
        if input_format and target_format:
            df = df.withColumn(
                column,
                date_format(to_timestamp(col(column), input_format), target_format),
            )
        elif target_format:
            df = df.withColumn(
                column, date_format(to_timestamp(col(column)), target_format)
            )

        return df

    @staticmethod
    def standardize_dates(
        df: DataFrame, column: str, format_type: str = "first_day_month"
    ) -> DataFrame:
        """Standardize date formats"""
        if format_type == "first_day_month":
            return df.withColumn(
                column, date_format(to_date(col(column)), "yyyy-MM-01")
            )
        return df.withColumn(column, to_date(col(column)))
