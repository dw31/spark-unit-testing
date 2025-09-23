"""DataFrame transformation utilities"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, lower, trim, when, regexp_replace
from typing import List, Optional


def add_prefix_to_columns(df: DataFrame, prefix: str, exclude_cols: Optional[List[str]] = None) -> DataFrame:
    """
    Add a prefix to all or selected columns in a DataFrame.

    Args:
        df: Input DataFrame
        prefix: Prefix to add to column names
        exclude_cols: List of columns to exclude from renaming

    Returns:
        DataFrame with renamed columns
    """
    exclude_cols = exclude_cols or []

    for column in df.columns:
        if column not in exclude_cols:
            df = df.withColumnRenamed(column, f"{prefix}_{column}")

    return df


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normalize column names by converting to lowercase and replacing spaces with underscores.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with normalized column names
    """
    for column in df.columns:
        new_name = column.lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(column, new_name)

    return df


def clean_string_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Clean string columns by trimming whitespace and converting to uppercase.

    Args:
        df: Input DataFrame
        columns: List of column names to clean

    Returns:
        DataFrame with cleaned string columns
    """
    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, upper(trim(col(column))))

    return df


def add_conditional_column(df: DataFrame, new_col: str, condition_col: str,
                          condition_value: any, true_value: any, false_value: any) -> DataFrame:
    """
    Add a new column based on a condition.

    Args:
        df: Input DataFrame
        new_col: Name of the new column
        condition_col: Column to check condition on
        condition_value: Value to check against
        true_value: Value when condition is true
        false_value: Value when condition is false

    Returns:
        DataFrame with new conditional column
    """
    return df.withColumn(
        new_col,
        when(col(condition_col) == condition_value, true_value).otherwise(false_value)
    )


def remove_special_characters(df: DataFrame, columns: List[str], keep_chars: str = "") -> DataFrame:
    """
    Remove special characters from specified columns.

    Args:
        df: Input DataFrame
        columns: List of columns to clean
        keep_chars: Characters to keep (default keeps only alphanumeric and spaces)

    Returns:
        DataFrame with cleaned columns
    """
    pattern = f"[^a-zA-Z0-9\\s{keep_chars}]"

    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, regexp_replace(col(column), pattern, ""))

    return df