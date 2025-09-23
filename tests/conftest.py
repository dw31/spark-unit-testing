"""Pytest configuration and fixtures for Spark tests"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("spark-unit-tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    logger.info("Spark session created for testing")
    yield spark

    logger.info("Shutting down Spark session")
    spark.stop()


@pytest.fixture(scope="function")
def sample_df(spark_session):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True)
    ])

    data = [
        (1, "  John Doe  ", 30, 75000.0, "Engineering"),
        (2, "Jane Smith", 28, 65000.0, "Marketing"),
        (3, "Bob Johnson", 35, 85000.0, "Engineering"),
        (4, "Alice Brown", 32, 70000.0, "HR"),
        (5, None, 29, 60000.0, "Marketing"),
        (6, "Charlie Wilson", None, 80000.0, "Engineering"),
        (7, "Diana Prince", 31, None, "HR"),
        (8, "Eve Adams", 27, 55000.0, None)
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def empty_df(spark_session):
    """Create an empty DataFrame for edge case testing."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("value", StringType(), True)
    ])

    return spark_session.createDataFrame([], schema)


@pytest.fixture(scope="function")
def duplicate_df(spark_session):
    """Create a DataFrame with duplicate rows."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])

    data = [
        (1, "A", 100),
        (2, "B", 200),
        (1, "A", 100),  # Duplicate
        (3, "C", 300),
        (2, "B", 200),  # Duplicate
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def messy_df(spark_session):
    """Create a DataFrame with messy data for cleaning tests."""
    schema = StructType([
        StructField("Product Name", StringType(), True),
        StructField("Price-USD", DoubleType(), True),
        StructField("Description", StringType(), True)
    ])

    data = [
        ("  Laptop@123  ", 999.99, "High-end laptop with #special chars!"),
        ("Phone$456", 599.99, "Smart phone (latest model)"),
        ("Tablet  ", 299.99, "Basic tablet & accessories"),
    ]

    return spark_session.createDataFrame(data, schema)