"""DataFrame aggregation utilities"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, avg, max as spark_max, min as spark_min, count, stddev
from typing import List, Dict, Any


def calculate_basic_stats(df: DataFrame, numeric_columns: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Calculate basic statistics for numeric columns.

    Args:
        df: Input DataFrame
        numeric_columns: List of numeric column names

    Returns:
        Dictionary with statistics for each column
    """
    stats = {}

    for column in numeric_columns:
        if column in df.columns:
            col_stats = df.agg(
                avg(col(column)).alias("mean"),
                spark_min(col(column)).alias("min"),
                spark_max(col(column)).alias("max"),
                stddev(col(column)).alias("stddev"),
                count(col(column)).alias("count")
            ).collect()[0]

            stats[column] = {
                "mean": col_stats["mean"],
                "min": col_stats["min"],
                "max": col_stats["max"],
                "stddev": col_stats["stddev"],
                "count": col_stats["count"]
            }

    return stats


def group_and_aggregate(df: DataFrame, group_cols: List[str], agg_dict: Dict[str, str]) -> DataFrame:
    """
    Group DataFrame and apply multiple aggregations.

    Args:
        df: Input DataFrame
        group_cols: Columns to group by
        agg_dict: Dictionary mapping column names to aggregation functions

    Returns:
        Aggregated DataFrame
    """
    agg_exprs = []

    for col_name, agg_func in agg_dict.items():
        if agg_func == "sum":
            agg_exprs.append(spark_sum(col(col_name)).alias(f"{col_name}_{agg_func}"))
        elif agg_func == "avg":
            agg_exprs.append(avg(col(col_name)).alias(f"{col_name}_{agg_func}"))
        elif agg_func == "max":
            agg_exprs.append(spark_max(col(col_name)).alias(f"{col_name}_{agg_func}"))
        elif agg_func == "min":
            agg_exprs.append(spark_min(col(col_name)).alias(f"{col_name}_{agg_func}"))
        elif agg_func == "count":
            agg_exprs.append(count(col(col_name)).alias(f"{col_name}_{agg_func}"))

    return df.groupBy(*group_cols).agg(*agg_exprs)


def calculate_percentiles(df: DataFrame, column: str, percentiles: List[float]) -> Dict[str, float]:
    """
    Calculate percentiles for a numeric column.

    Args:
        df: Input DataFrame
        column: Column name to calculate percentiles for
        percentiles: List of percentiles to calculate (e.g., [0.25, 0.5, 0.75])

    Returns:
        Dictionary mapping percentile to value
    """
    result = {}

    for percentile in percentiles:
        value = df.approxQuantile(column, [percentile], 0.01)[0]
        result[f"p{int(percentile * 100)}"] = value

    return result


def pivot_summary(df: DataFrame, index_col: str, pivot_col: str, values_col: str, agg_func: str = "sum") -> DataFrame:
    """
    Create a pivot table summary.

    Args:
        df: Input DataFrame
        index_col: Column to use as index
        pivot_col: Column to pivot
        values_col: Column with values to aggregate
        agg_func: Aggregation function to use

    Returns:
        Pivoted DataFrame
    """
    pivot_df = df.groupBy(index_col).pivot(pivot_col)

    if agg_func == "sum":
        return pivot_df.sum(values_col)
    elif agg_func == "avg":
        return pivot_df.avg(values_col)
    elif agg_func == "max":
        return pivot_df.max(values_col)
    elif agg_func == "min":
        return pivot_df.min(values_col)
    elif agg_func == "count":
        return pivot_df.count()
    else:
        return pivot_df.sum(values_col)