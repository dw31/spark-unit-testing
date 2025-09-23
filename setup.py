"""Setup configuration for spark-df-utils package"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="spark-df-utils",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A library of utility functions for PySpark DataFrames",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/spark-unit-testing",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.10.0",
            "pytest-spark>=0.6.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
            "pylint>=2.13.0",
            "bandit>=1.7.4",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/yourusername/spark-unit-testing/issues",
        "Source": "https://github.com/yourusername/spark-unit-testing",
    },
)