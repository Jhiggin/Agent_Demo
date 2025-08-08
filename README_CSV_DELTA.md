# CSV to Delta Table Processing

This module provides a comprehensive solution for reading CSV files with various schemas and writing them to Delta tables using Apache Spark and Delta Lake.

## Features

- **Schema Inference**: Automatically infer schema from CSV files
- **Custom Schema Support**: Use predefined schemas for consistent data types
- **Multiple CSV Processing**: Process multiple CSV files with different configurations
- **Delta Lake Integration**: Write data to Delta tables with ACID transactions
- **Partitioning Support**: Partition Delta tables for improved query performance
- **Error Handling**: Robust error handling and logging
- **Metadata Addition**: Automatically add processing timestamps
- **Configurable Options**: Flexible CSV reading options (delimiters, encoding, etc.)

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

### Required Dependencies

- `pyspark==3.5.0`
- `delta-spark==3.0.0`
- `pandas>=1.5.0`
- `pyarrow>=8.0.0`

## Quick Start

### Basic Usage

```python
from csv_to_delta import CSVToDeltaProcessor

# Initialize processor
processor = CSVToDeltaProcessor("MyApp")

# Read CSV with schema inference
df = processor.read_csv_with_schema_inference("data/input.csv")

# Write to Delta table
processor.write_to_delta_table(df, "delta/output_table")

# Close processor
processor.close()
```

### Using Configuration File

```python
from example_usage import process_with_config_file

# Process multiple CSVs using config.json
process_with_config_file()
```

### Custom Schema Example

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from csv_to_delta import CSVToDeltaProcessor

# Define custom schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), True)
])

processor = CSVToDeltaProcessor()

# Read CSV with custom schema
df = processor.read_csv_with_custom_schema(
    "data/users.csv", 
    schema,
    options={"delimiter": ",", "header": "true"}
)

# Write with partitioning
processor.write_to_delta_table(
    df, 
    "delta/users_table",
    partition_cols=["department"]
)
```

## Configuration

### JSON Configuration Format

The `config.json` file allows you to define multiple CSV processing jobs:

```json
{
  "csv_processing_configs": [
    {
      "description": "User data processing",
      "file_path": "data/users.csv",
      "delta_path": "delta/users_table",
      "mode": "overwrite",
      "options": {
        "delimiter": ",",
        "header": "true",
        "inferSchema": "true",
        "encoding": "UTF-8"
      },
      "partition_cols": ["department"],
      "add_metadata": true
    }
  ],
  "spark_config": {
    "app_name": "CSV-to-Delta-ETL"
  }
}
```

### Configuration Parameters

#### CSV Processing Config
- `file_path`: Path to the source CSV file
- `delta_path`: Path where Delta table will be stored
- `mode`: Write mode (`overwrite`, `append`, `merge`)
- `options`: CSV reading options (delimiter, header, encoding, etc.)
- `partition_cols`: List of columns to partition by
- `add_metadata`: Whether to add processing timestamp

#### CSV Reading Options
- `delimiter`: Field delimiter (default: `,`)
- `header`: Whether first row is header (default: `true`)
- `inferSchema`: Automatically infer data types (default: `true`)
- `encoding`: File encoding (default: `UTF-8`)
- `multiline`: Support multiline records (default: `true`)
- `escape`: Escape character (default: `"`)
- `timestampFormat`: Timestamp format for parsing
- `dateFormat`: Date format for parsing

## API Reference

### CSVToDeltaProcessor Class

#### Constructor
```python
CSVToDeltaProcessor(app_name: str = "CSV-to-Delta-Processor")
```

#### Methods

##### read_csv_with_schema_inference()
```python
read_csv_with_schema_inference(
    file_path: str, 
    options: Optional[Dict[str, Any]] = None
) -> DataFrame
```
Reads CSV file with automatic schema inference.

##### read_csv_with_custom_schema()
```python
read_csv_with_custom_schema(
    file_path: str, 
    schema: StructType,
    options: Optional[Dict[str, Any]] = None
) -> DataFrame
```
Reads CSV file with predefined schema.

##### write_to_delta_table()
```python
write_to_delta_table(
    df: DataFrame, 
    table_path: str,
    mode: str = "overwrite",
    partition_cols: Optional[List[str]] = None,
    add_metadata: bool = True
) -> None
```
Writes DataFrame to Delta table.

##### process_multiple_csvs()
```python
process_multiple_csvs(csv_configs: List[Dict[str, Any]]) -> None
```
Processes multiple CSV files based on configuration list.

##### get_table_info()
```python
get_table_info(table_path: str) -> Dict[str, Any]
```
Retrieves information about a Delta table.

## Examples

### Example 1: Simple CSV Processing

```python
processor = CSVToDeltaProcessor("SimpleExample")

# Process single CSV
df = processor.read_csv_with_schema_inference("sales_data.csv")
processor.write_to_delta_table(df, "delta/sales_table")

processor.close()
```

### Example 2: Batch Processing with Configuration

```python
configs = [
    {
        "file_path": "data/customers.csv",
        "delta_path": "delta/customers",
        "mode": "overwrite"
    },
    {
        "file_path": "data/orders.csv",
        "delta_path": "delta/orders", 
        "mode": "overwrite",
        "partition_cols": ["order_date"]
    }
]

processor = CSVToDeltaProcessor()
processor.process_multiple_csvs(configs)
processor.close()
```

### Example 3: Error Handling

```python
processor = CSVToDeltaProcessor()

try:
    df = processor.read_csv_with_schema_inference("data/input.csv")
    processor.write_to_delta_table(df, "delta/output")
except Exception as e:
    print(f"Processing failed: {e}")
finally:
    processor.close()
```

## Running Tests

Run the unit tests:

```bash
python test_csv_to_delta.py
```

## Performance Considerations

1. **Partitioning**: Use appropriate partition columns for large datasets
2. **Schema Definition**: Use custom schemas for better performance and data quality
3. **Spark Configuration**: Tune Spark settings based on your data size and cluster
4. **File Formats**: Consider using larger CSV files to minimize overhead
5. **Memory Management**: Monitor Spark driver and executor memory usage

## Error Handling

The module provides comprehensive error handling:

- **File Not Found**: Logs error and continues with other files
- **Schema Mismatch**: Provides detailed error messages
- **Write Failures**: Rolls back failed operations
- **Invalid Configuration**: Validates config parameters

## Logging

All operations are logged with different levels:
- `INFO`: Normal processing information
- `WARNING`: Non-fatal issues
- `ERROR`: Processing failures

## Best Practices

1. **Always close the processor** to free Spark resources
2. **Use custom schemas** for production workloads
3. **Partition large tables** by commonly filtered columns
4. **Monitor Delta table statistics** for optimization
5. **Use appropriate write modes** (overwrite vs append vs merge)
6. **Validate CSV files** before processing large batches
7. **Set up proper error handling** for production pipelines

## Troubleshooting

### Common Issues

1. **OutOfMemoryError**: Increase Spark driver/executor memory
2. **Schema Evolution**: Use merge mode or update schemas
3. **File Encoding**: Specify correct encoding in options
4. **Path Issues**: Use absolute paths or ensure working directory is correct

### Debug Tips

1. Check Spark UI for job details
2. Enable DEBUG logging for detailed information
3. Use `get_table_info()` to inspect Delta tables
4. Verify CSV file structure before processing