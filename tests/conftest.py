"""Pytest configuration and shared fixtures."""

import contextlib

import pytest


@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing with proper configuration."""
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession

        # Stop any existing SparkContext to avoid conflicts
        try:
            sc = SparkContext._active_spark_context
            if sc is not None:
                sc.stop()
        except Exception:
            pass

        # Create SparkSession with proper configuration for testing
        # Disable Arrow optimization to avoid compatibility issues with pandas 2.x
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("lugia-test")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
            .getOrCreate()
        )

        yield spark

        # Cleanup
        spark.stop()
        with contextlib.suppress(Exception):
            SparkContext._active_spark_context = None

    except ImportError:
        pytest.skip("PySpark not available")
    except Exception as e:
        pytest.skip(f"PySpark SparkSession creation failed: {e}")
