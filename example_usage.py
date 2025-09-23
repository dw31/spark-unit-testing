#!/usr/bin/env python
"""Example usage of the Spark DataFrame utilities library"""

from pyspark.sql import SparkSession
from spark_df_utils.transformations import (
    normalize_column_names,
    clean_string_columns,
    add_conditional_column
)
from spark_df_utils.aggregations import (
    calculate_basic_stats,
    group_and_aggregate
)
from spark_df_utils.validations import (
    check_null_values,
    check_data_quality
)


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("spark-df-utils-example") \
        .master("local[*]") \
        .getOrCreate()

    # Create sample data
    data = [
        ("John Doe  ", 30, 75000, "Engineering"),
        ("Jane Smith", 28, 65000, "Marketing"),
        ("Bob Johnson", 35, 85000, "Engineering"),
        ("Alice Brown", 32, 70000, "HR"),
        (None, 29, 60000, "Marketing"),
        ("Charlie Wilson", None, 80000, "Engineering")
    ]

    # Create DataFrame with non-normalized column names
    df = spark.createDataFrame(data, ["Full Name", "Age", "Salary-USD", "Department"])

    print("Original DataFrame:")
    df.show()

    # 1. TRANSFORMATIONS
    print("\n=== TRANSFORMATIONS ===")

    # Normalize column names
    df = normalize_column_names(df)
    print("\nAfter normalizing column names:")
    print(f"Columns: {df.columns}")

    # Clean string columns
    df = clean_string_columns(df, ["full_name", "department"])
    print("\nAfter cleaning string columns:")
    df.select("full_name", "department").show()

    # Add conditional column
    df = add_conditional_column(
        df, "senior_employee", "age", 30, "Senior", "Junior"
    )
    print("\nAfter adding conditional column:")
    df.select("full_name", "age", "senior_employee").show()

    # 2. AGGREGATIONS
    print("\n=== AGGREGATIONS ===")

    # Calculate basic statistics
    stats = calculate_basic_stats(df, ["age", "salary_usd"])
    print("\nBasic statistics:")
    for col, col_stats in stats.items():
        print(f"\n{col}:")
        for stat, value in col_stats.items():
            if value is not None:
                print(f"  {stat}: {value:.2f}" if isinstance(value, float) else f"  {stat}: {value}")

    # Group and aggregate
    agg_dict = {"salary_usd": "avg", "age": "max"}
    grouped_df = group_and_aggregate(df, ["department"], agg_dict)
    print("\nGrouped aggregations by department:")
    grouped_df.show()

    # 3. VALIDATIONS
    print("\n=== VALIDATIONS ===")

    # Check null values
    null_counts = check_null_values(df)
    print("\nNull value counts:")
    for col, count in null_counts.items():
        print(f"  {col}: {count}")

    # Comprehensive data quality check
    quality = check_data_quality(df)
    print("\nData Quality Report:")
    print(f"  Total rows: {quality['total_rows']}")
    print(f"  Total columns: {quality['total_columns']}")
    print(f"  Duplicate rows: {quality['duplicate_rows']}")
    print(f"  Overall completeness: {quality['overall_completeness']:.1f}%")

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()