"""Unit tests for aggregation functions"""

import pytest
from spark_df_utils.aggregations import (
    calculate_basic_stats,
    group_and_aggregate,
    calculate_percentiles,
    pivot_summary
)


class TestAggregations:

    def test_calculate_basic_stats(self, sample_df):
        """Test basic statistics calculation."""
        stats = calculate_basic_stats(sample_df, ["age", "salary"])

        # Check that stats are calculated for requested columns
        assert "age" in stats
        assert "salary" in stats

        # Verify age stats (accounting for None values)
        age_stats = stats["age"]
        assert age_stats["count"] == 7  # One null value
        assert age_stats["min"] == 27
        assert age_stats["max"] == 35
        assert age_stats["mean"] == pytest.approx(30.43, rel=0.01)

        # Verify salary stats
        salary_stats = stats["salary"]
        assert salary_stats["count"] == 7  # One null value
        assert salary_stats["min"] == 55000.0
        assert salary_stats["max"] == 85000.0

    def test_calculate_basic_stats_empty(self, empty_df):
        """Test basic stats on empty DataFrame."""
        stats = calculate_basic_stats(empty_df, ["value"])
        # Should return empty dict or handle gracefully
        assert stats.get("value", {}).get("count") == 0 or stats == {}

    def test_group_and_aggregate(self, sample_df):
        """Test grouping and aggregation."""
        agg_dict = {
            "salary": "avg",
            "age": "max",
            "id": "count"
        }

        result = group_and_aggregate(sample_df, ["department"], agg_dict)

        # Check column names
        expected_columns = ["department", "salary_avg", "age_max", "id_count"]
        assert sorted(result.columns) == sorted(expected_columns)

        # Verify aggregations for Engineering department
        eng_row = result.filter(result.department == "Engineering").collect()[0]
        assert eng_row["id_count"] == 3
        assert eng_row["age_max"] == 35
        assert eng_row["salary_avg"] == pytest.approx(80000.0, rel=0.01)

    def test_calculate_percentiles(self, sample_df):
        """Test percentile calculation."""
        percentiles = calculate_percentiles(sample_df, "salary", [0.25, 0.5, 0.75])

        assert "p25" in percentiles
        assert "p50" in percentiles
        assert "p75" in percentiles

        # Median should be around 70000
        assert 65000 <= percentiles["p50"] <= 75000

    def test_pivot_summary(self, sample_df):
        """Test pivot table creation."""
        # Add a simple numeric column for testing
        test_df = sample_df.withColumn("bonus", sample_df.salary * 0.1)

        result = pivot_summary(test_df, "department", "id", "bonus", "sum")

        # Check that pivot created columns for each id
        assert "department" in result.columns
        # The pivoted columns will be named after the id values

        # Verify row count equals unique departments
        unique_depts = test_df.select("department").distinct().count()
        assert result.count() == unique_depts

    def test_group_and_aggregate_multiple_columns(self, duplicate_df):
        """Test grouping by multiple columns."""
        agg_dict = {"value": "sum"}

        result = group_and_aggregate(duplicate_df, ["id", "name"], agg_dict)

        # After grouping, duplicates should be consolidated
        assert result.count() == 3  # 3 unique combinations

        # Check that sum is calculated correctly for duplicates
        id1_row = result.filter(result.id == 1).collect()[0]
        assert id1_row["value_sum"] == 200  # 100 + 100

    def test_calculate_percentiles_edge_cases(self, spark_session):
        """Test percentile calculation on edge cases."""
        # Add a single row for testing
        single_row_df = spark_session.createDataFrame([(1, "test")], ["id", "value"])

        percentiles = calculate_percentiles(single_row_df, "id", [0.5])
        assert percentiles["p50"] == 1  # Only value is the median