"""Unit tests for validation functions"""

import pytest
from spark_df_utils.validations import (
    check_null_values,
    validate_schema,
    check_duplicates,
    validate_numeric_ranges,
    check_data_quality
)


class TestValidations:

    def test_check_null_values(self, sample_df):
        """Test null value checking."""
        null_counts = check_null_values(sample_df)

        assert null_counts["id"] == 0  # id is non-nullable
        assert null_counts["name"] == 1  # One null name
        assert null_counts["age"] == 1  # One null age
        assert null_counts["salary"] == 1  # One null salary
        assert null_counts["department"] == 1  # One null department

    def test_check_null_values_specific_columns(self, sample_df):
        """Test null checking for specific columns."""
        null_counts = check_null_values(sample_df, ["name", "age"])

        assert len(null_counts) == 2
        assert null_counts["name"] == 1
        assert null_counts["age"] == 1

    def test_validate_schema(self, sample_df):
        """Test schema validation."""
        # Test with correct schema
        expected_schema = {
            "id": "IntegerType",
            "name": "StringType",
            "age": "IntegerType",
            "salary": "DoubleType",
            "department": "StringType"
        }

        is_valid, errors = validate_schema(sample_df, expected_schema)
        assert is_valid
        assert len(errors) == 0

        # Test with incorrect schema
        wrong_schema = {
            "id": "StringType",  # Wrong type
            "missing_column": "IntegerType",  # Missing column
            "age": "IntegerType"
        }

        is_valid, errors = validate_schema(sample_df, wrong_schema)
        assert not is_valid
        assert len(errors) > 0
        assert any("Type mismatch" in error for error in errors)
        assert any("Missing column" in error for error in errors)

    def test_check_duplicates(self, duplicate_df):
        """Test duplicate detection."""
        # Check all columns
        dup_count = check_duplicates(duplicate_df)
        assert dup_count == 2  # 2 duplicate rows

        # Check specific columns
        dup_count = check_duplicates(duplicate_df, ["id"])
        assert dup_count == 2  # 2 rows with duplicate ids

    def test_check_duplicates_no_duplicates(self, sample_df):
        """Test duplicate check on unique data."""
        dup_count = check_duplicates(sample_df)
        assert dup_count == 0

    def test_validate_numeric_ranges(self, sample_df):
        """Test numeric range validation."""
        ranges = {
            "age": (25, 40),  # Valid range
            "salary": (50000, 75000)  # Some values outside
        }

        out_of_range = validate_numeric_ranges(sample_df, ranges)

        assert out_of_range["age"] == 0  # All ages within range
        assert out_of_range["salary"] == 2  # 80000 and 85000 are above 75000

    def test_validate_numeric_ranges_with_nulls(self, sample_df):
        """Test range validation with null values."""
        ranges = {"age": (20, 30)}

        out_of_range = validate_numeric_ranges(sample_df, ranges)
        # Should count values > 30, but not nulls
        assert out_of_range["age"] == 3  # Ages 31, 32, 35 are out of range

    def test_check_data_quality(self, sample_df):
        """Test comprehensive data quality check."""
        quality = check_data_quality(sample_df)

        assert quality["total_rows"] == 8
        assert quality["total_columns"] == 5
        assert quality["empty_rows"] == 0  # No completely empty rows
        assert quality["duplicate_rows"] == 0

        # Check null counts
        assert quality["null_counts"]["name"] == 1
        assert quality["null_counts"]["age"] == 1

        # Check completeness
        assert quality["completeness_percentage"]["id"] == 100.0
        assert quality["completeness_percentage"]["name"] == 87.5  # 7/8
        assert quality["overall_completeness"] > 85  # Most data is complete

    def test_check_data_quality_with_duplicates(self, duplicate_df):
        """Test data quality on DataFrame with duplicates."""
        quality = check_data_quality(duplicate_df)

        assert quality["duplicate_rows"] == 2
        assert quality["total_rows"] == 5

    def test_check_data_quality_empty(self, empty_df):
        """Test data quality on empty DataFrame."""
        quality = check_data_quality(empty_df)

        assert quality["total_rows"] == 0
        assert quality["empty_rows"] == 0
        assert quality["duplicate_rows"] == 0