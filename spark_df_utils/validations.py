"""DataFrame validation utilities"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, isnull, when, count
from typing import List, Dict, Optional, Tuple


def check_null_values(df: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, int]:
    """
    Check for null values in specified columns.

    Args:
        df: Input DataFrame
        columns: List of columns to check (if None, checks all columns)

    Returns:
        Dictionary mapping column names to null counts
    """
    columns = columns or df.columns
    null_counts = {}

    for column in columns:
        if column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_counts[column] = null_count

    return null_counts


def validate_schema(df: DataFrame, expected_schema: Dict[str, str]) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame schema against expected schema.

    Args:
        df: Input DataFrame
        expected_schema: Dictionary mapping column names to expected data types

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}

    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_schema:
            errors.append(f"Missing column: {col_name}")
        elif expected_type not in actual_schema[col_name]:
            errors.append(
                f"Type mismatch for column {col_name}: expected {expected_type}, got {actual_schema[col_name]}"
            )

    return len(errors) == 0, errors


def check_duplicates(df: DataFrame, columns: Optional[List[str]] = None) -> int:
    """
    Check for duplicate rows based on specified columns.

    Args:
        df: Input DataFrame
        columns: Columns to check for duplicates (if None, checks all columns)

    Returns:
        Number of duplicate rows
    """
    columns = columns or df.columns
    total_count = df.count()
    distinct_count = df.select(*columns).distinct().count()

    return total_count - distinct_count


def validate_numeric_ranges(df: DataFrame, column_ranges: Dict[str, Tuple[float, float]]) -> Dict[str, int]:
    """
    Validate that numeric columns fall within expected ranges.

    Args:
        df: Input DataFrame
        column_ranges: Dictionary mapping column names to (min, max) tuples

    Returns:
        Dictionary mapping column names to count of out-of-range values
    """
    out_of_range = {}

    for column, (min_val, max_val) in column_ranges.items():
        if column in df.columns:
            out_count = df.filter(
                (col(column) < min_val) | (col(column) > max_val)
            ).count()
            out_of_range[column] = out_count

    return out_of_range


def check_data_quality(df: DataFrame) -> Dict[str, any]:
    """
    Comprehensive data quality check.

    Args:
        df: Input DataFrame

    Returns:
        Dictionary with various data quality metrics
    """
    total_rows = df.count()
    total_cols = len(df.columns)

    # Check for completely empty rows
    empty_rows = df.filter(
        " AND ".join([f"{col} IS NULL" for col in df.columns])
    ).count() if total_cols > 0 else 0

    # Check for duplicate rows
    duplicate_rows = check_duplicates(df)

    # Check null values
    null_summary = check_null_values(df)

    # Calculate completeness percentage for each column
    completeness = {}
    for col_name, null_count in null_summary.items():
        if total_rows > 0:
            completeness[col_name] = ((total_rows - null_count) / total_rows) * 100
        else:
            completeness[col_name] = 0

    return {
        "total_rows": total_rows,
        "total_columns": total_cols,
        "empty_rows": empty_rows,
        "duplicate_rows": duplicate_rows,
        "null_counts": null_summary,
        "completeness_percentage": completeness,
        "overall_completeness": sum(completeness.values()) / len(completeness) if completeness else 0
    }