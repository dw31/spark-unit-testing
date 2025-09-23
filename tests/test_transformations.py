"""Unit tests for transformation functions"""

import pytest
from pyspark.sql.functions import col
from spark_df_utils.transformations import (
    add_prefix_to_columns,
    normalize_column_names,
    clean_string_columns,
    add_conditional_column,
    remove_special_characters
)


class TestTransformations:

    def test_add_prefix_to_columns(self, sample_df):
        """Test adding prefix to column names."""
        # Test with no exclusions
        result = add_prefix_to_columns(sample_df, "test")
        expected_columns = ["test_id", "test_name", "test_age", "test_salary", "test_department"]
        assert result.columns == expected_columns

        # Test with exclusions
        result = add_prefix_to_columns(sample_df, "new", exclude_cols=["id", "name"])
        expected_columns = ["id", "name", "new_age", "new_salary", "new_department"]
        assert result.columns == expected_columns

    def test_add_prefix_empty_df(self, empty_df):
        """Test prefix addition on empty DataFrame."""
        result = add_prefix_to_columns(empty_df, "test")
        assert result.columns == ["test_id", "test_value"]
        assert result.count() == 0

    def test_normalize_column_names(self, messy_df):
        """Test column name normalization."""
        result = normalize_column_names(messy_df)
        expected_columns = ["product_name", "price_usd", "description"]
        assert result.columns == expected_columns

    def test_clean_string_columns(self, sample_df):
        """Test string column cleaning."""
        result = clean_string_columns(sample_df, ["name", "department"])

        # Check that whitespace is trimmed and text is uppercase
        john_row = result.filter(col("id") == 1).collect()[0]
        assert john_row["name"] == "JOHN DOE"
        assert john_row["department"] == "ENGINEERING"

        # Check null handling
        null_name_row = result.filter(col("id") == 5).collect()[0]
        assert null_name_row["name"] is None

    def test_clean_string_columns_nonexistent(self, sample_df):
        """Test cleaning non-existent columns."""
        result = clean_string_columns(sample_df, ["nonexistent_column"])
        # Should return unchanged DataFrame
        assert result.columns == sample_df.columns
        assert result.count() == sample_df.count()

    def test_add_conditional_column(self, sample_df):
        """Test adding conditional column."""
        result = add_conditional_column(
            sample_df,
            "is_engineering",
            "department",
            "Engineering",
            True,
            False
        )

        assert "is_engineering" in result.columns

        # Verify condition logic
        eng_count = result.filter(col("is_engineering") == True).count()
        assert eng_count == 3  # 3 people in Engineering

        non_eng_count = result.filter(col("is_engineering") == False).count()
        assert non_eng_count == 5  # Including null department

    def test_remove_special_characters(self, messy_df):
        """Test removing special characters."""
        result = remove_special_characters(messy_df, ["Product Name", "Description"])

        # Check special characters are removed
        first_row = result.collect()[0]
        assert "@" not in first_row["Product Name"]
        assert "#" not in first_row["Description"]
        assert "!" not in first_row["Description"]

        # Alphanumeric and spaces should be preserved
        assert "Laptop123" in first_row["Product Name"]

    def test_remove_special_characters_with_keep(self, messy_df):
        """Test removing special characters while keeping some."""
        result = remove_special_characters(messy_df, ["Description"], keep_chars="-")

        first_row = result.collect()[0]
        # Check that hyphen is kept but other special chars are removed
        assert "High-end" in first_row["Description"]
        assert "#" not in first_row["Description"]
        assert "!" not in first_row["Description"]