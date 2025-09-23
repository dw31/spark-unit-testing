# Spark DataFrame Utilities

A comprehensive library of utility functions for PySpark DataFrames with automated testing via GitHub Actions.

## Features

This library provides three main categories of utilities:

### 1. Transformations (`spark_df_utils.transformations`)
- `add_prefix_to_columns`: Add prefixes to column names
- `normalize_column_names`: Normalize column names (lowercase, replace spaces)
- `clean_string_columns`: Clean string columns (trim, uppercase)
- `add_conditional_column`: Add columns based on conditions
- `remove_special_characters`: Remove special characters from text

### 2. Aggregations (`spark_df_utils.aggregations`)
- `calculate_basic_stats`: Calculate mean, min, max, stddev for numeric columns
- `group_and_aggregate`: Group by columns and apply multiple aggregations
- `calculate_percentiles`: Calculate percentiles for numeric columns
- `pivot_summary`: Create pivot table summaries

### 3. Validations (`spark_df_utils.validations`)
- `check_null_values`: Check for null values in columns
- `validate_schema`: Validate DataFrame schema against expected schema
- `check_duplicates`: Check for duplicate rows
- `validate_numeric_ranges`: Validate numeric values fall within expected ranges
- `check_data_quality`: Comprehensive data quality check

## Installation

### For Development
```bash
git clone https://github.com/yourusername/spark-unit-testing.git
cd spark-unit-testing
pip install -e .
pip install -r requirements-test.txt
```

### As a Package
```bash
pip install spark-df-utils
```

## Usage Examples

### Transformations
```python
from pyspark.sql import SparkSession
from spark_df_utils.transformations import normalize_column_names, clean_string_columns

spark = SparkSession.builder.appName("example").getOrCreate()

# Create sample DataFrame
df = spark.createDataFrame([
    ("  John Doe  ", 30, "Engineering"),
    ("Jane Smith", 28, "Marketing")
], ["Full Name", "age", "department"])

# Normalize column names
df = normalize_column_names(df)  # Columns: full_name, age, department

# Clean string columns
df = clean_string_columns(df, ["full_name", "department"])
# Result: "JOHN DOE", "ENGINEERING", etc.
```

### Aggregations
```python
from spark_df_utils.aggregations import calculate_basic_stats, group_and_aggregate

# Calculate statistics
stats = calculate_basic_stats(df, ["age", "salary"])
print(f"Age mean: {stats['age']['mean']}")

# Group and aggregate
agg_dict = {"salary": "avg", "age": "max"}
result = group_and_aggregate(df, ["department"], agg_dict)
```

### Validations
```python
from spark_df_utils.validations import check_null_values, check_data_quality

# Check nulls
null_counts = check_null_values(df)
print(f"Null values in 'name': {null_counts['name']}")

# Comprehensive quality check
quality = check_data_quality(df)
print(f"Overall completeness: {quality['overall_completeness']}%")
```

## Running Tests

### Locally
```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=spark_df_utils --cov-report=html

# Run specific test file
pytest tests/test_transformations.py -v
```

### GitHub Actions

Tests run automatically on:
- Every push to `main` or `develop` branches
- Every pull request to `main` or `develop` branches
- When spark_df_utils/ or tests/ directories are modified

The CI pipeline includes:
- Unit tests across multiple Python versions (3.8-3.11) and Spark versions (3.4, 3.5)
- Code quality checks (black, isort, mypy, pylint, bandit)
- Coverage reporting
- Integration tests

## Project Structure
```
spark-unit-testing/
├── spark_df_utils/           # Main library package
│   ├── __init__.py
│   ├── transformations.py    # DataFrame transformation utilities
│   ├── aggregations.py       # Aggregation and statistics utilities
│   └── validations.py        # Data validation utilities
├── tests/                     # Unit tests
│   ├── __init__.py
│   ├── conftest.py           # Pytest fixtures
│   ├── test_transformations.py
│   ├── test_aggregations.py
│   └── test_validations.py
├── .github/workflows/         # GitHub Actions
│   └── test.yml              # CI/CD workflow
├── requirements.txt          # Production dependencies
├── requirements-test.txt     # Test dependencies
├── setup.py                  # Package setup
└── README.md                # This file
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests locally (`pytest tests/`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

MIT License - see LICENSE file for details

## GitHub Actions Workflow

The automated testing workflow (`test.yml`) runs on every commit and includes:

1. **Matrix Testing**: Tests across multiple OS, Python, and Spark versions
2. **Code Quality**: Linting with flake8, formatting checks with black
3. **Type Checking**: Static type analysis with mypy
4. **Security Scanning**: Security vulnerability detection with bandit
5. **Coverage Reports**: Automatic coverage upload to Codecov
6. **Integration Tests**: Additional integration testing suite
7. **Artifact Storage**: Test results and coverage reports saved as artifacts

## Requirements

- Python 3.8+
- Apache Spark 3.4.0+
- Java 8 or 11 (for Spark)