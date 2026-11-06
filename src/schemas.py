"""
schemas.py
The structure of the CSV event files written by data_generator.py.

Every column is read as text on purpose. If Spark read price as a number,
a value like "abc" would silently become null and the original text would be
lost. Reading text first lets the cleaning step (transform.py) decide what is
valid, and keep the exact original values of rejected rows for inspection.
"""

from pyspark.sql.types import StringType, StructField, StructType

from src.config import CSV_COLUMNS

# Streaming file sources require a schema: Spark can't guess one from files
# that haven't arrived yet
RAW_EVENT_SCHEMA = StructType([StructField(column, StringType()) for column in CSV_COLUMNS])
