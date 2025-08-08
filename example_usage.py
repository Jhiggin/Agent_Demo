"""
Example usage script for CSV to Delta processing.

This script demonstrates how to use the CSVToDeltaProcessor class
to read various CSV files and write them to Delta tables.
"""

import json
import logging
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from csv_to_delta import CSVToDeltaProcessor


def load_config(config_path: str = "config.json") -> dict:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.warning(f"Config file {config_path} not found, using default configuration")
        return get_default_config()


def get_default_config() -> dict:
    """
    Get default configuration for CSV processing.
    
    Returns:
        Default configuration dictionary
    """
    return {
        "csv_processing_configs": [
            {
                "description": "Sample CSV processing",
                "file_path": "data/sample.csv",
                "delta_path": "delta/sample_table",
                "mode": "overwrite",
                "options": {
                    "delimiter": ",",
                    "header": "true",
                    "inferSchema": "true"
                },
                "add_metadata": True
            }
        ],
        "spark_config": {
            "app_name": "CSV-to-Delta-ETL"
        }
    }


def create_custom_schemas() -> dict:
    """
    Create custom schemas for specific data types.
    
    Returns:
        Dictionary of schema name to StructType mappings
    """
    schemas = {
        "users": StructType([
            StructField("user_id", IntegerType(), False),
            StructField("username", StringType(), False),
            StructField("email", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("signup_date", TimestampType(), True)
        ]),
        
        "products": StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("stock_quantity", IntegerType(), True),
            StructField("description", StringType(), True)
        ]),
        
        "transactions": StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("amount", DoubleType(), False),
            StructField("transaction_date", TimestampType(), False),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True)
        ])
    }
    
    return schemas


def process_with_config_file():
    """Process CSV files using configuration from config.json."""
    logging.info("Starting CSV to Delta processing with config file")
    
    # Load configuration
    config = load_config()
    
    # Initialize processor
    app_name = config.get("spark_config", {}).get("app_name", "CSV-to-Delta-Processor")
    processor = CSVToDeltaProcessor(app_name)
    
    try:
        # Process CSV files based on configuration
        csv_configs = config.get("csv_processing_configs", [])
        processor.process_multiple_csvs(csv_configs)
        
        # Print table information for each processed table
        for config_item in csv_configs:
            delta_path = config_item.get("delta_path")
            if delta_path and Path(delta_path).exists():
                try:
                    info = processor.get_table_info(delta_path)
                    print(f"\n=== Table Info: {delta_path} ===")
                    print(f"Description: {config_item.get('description', 'N/A')}")
                    print(f"Rows: {info['row_count']}")
                    print(f"Columns: {info['column_count']}")
                    print(f"Column Names: {', '.join(info['columns'])}")
                    print(f"Partitions: {info['partitions']}")
                except Exception as e:
                    logging.error(f"Failed to get info for {delta_path}: {e}")
        
    except Exception as e:
        logging.error(f"Error during processing: {e}")
        raise
    
    finally:
        processor.close()


def process_with_custom_schemas():
    """Process CSV files using custom predefined schemas."""
    logging.info("Starting CSV to Delta processing with custom schemas")
    
    processor = CSVToDeltaProcessor("CSV-Delta-Custom-Schema")
    schemas = create_custom_schemas()
    
    try:
        # Example configurations with custom schemas
        csv_configs = [
            {
                "file_path": "data/users_data.csv",
                "delta_path": "delta/users_with_schema",
                "schema": schemas["users"],
                "mode": "overwrite",
                "options": {
                    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
                    "dateFormat": "yyyy-MM-dd"
                }
            },
            {
                "file_path": "data/products_catalog.csv", 
                "delta_path": "delta/products_with_schema",
                "schema": schemas["products"],
                "mode": "overwrite",
                "partition_cols": ["category"]
            }
        ]
        
        # Process each CSV with its custom schema
        for config in csv_configs:
            try:
                file_path = config["file_path"]
                delta_path = config["delta_path"]
                schema = config["schema"]
                
                # Check if source file exists (in real scenario)
                if not Path(file_path).exists():
                    logging.warning(f"Source file does not exist: {file_path}")
                    continue
                
                logging.info(f"Processing {file_path} with custom schema")
                
                # Read with custom schema
                df = processor.read_csv_with_custom_schema(
                    file_path, 
                    schema, 
                    config.get("options", {})
                )
                
                # Write to Delta
                processor.write_to_delta_table(
                    df, 
                    delta_path, 
                    config.get("mode", "overwrite"),
                    config.get("partition_cols")
                )
                
            except Exception as e:
                logging.error(f"Failed to process {config.get('file_path')}: {e}")
                continue
        
    except Exception as e:
        logging.error(f"Error during custom schema processing: {e}")
        raise
    
    finally:
        processor.close()


def demonstrate_error_handling():
    """Demonstrate error handling capabilities."""
    logging.info("Demonstrating error handling")
    
    processor = CSVToDeltaProcessor("CSV-Delta-Error-Demo")
    
    try:
        # Try to process non-existent file
        invalid_configs = [
            {
                "file_path": "data/nonexistent.csv",
                "delta_path": "delta/error_table",
                "mode": "overwrite"
            }
        ]
        
        processor.process_multiple_csvs(invalid_configs)
        
    except Exception as e:
        logging.info(f"Caught expected error: {e}")
    
    finally:
        processor.close()


def main():
    """Main function to run different processing examples."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== CSV to Delta Table Processing Examples ===\n")
    
    try:
        # Example 1: Process with configuration file
        print("1. Processing with config file...")
        process_with_config_file()
        
        print("\n" + "="*50 + "\n")
        
        # Example 2: Process with custom schemas (commented out as files may not exist)
        # print("2. Processing with custom schemas...")
        # process_with_custom_schemas()
        
        # print("\n" + "="*50 + "\n")
        
        # Example 3: Demonstrate error handling
        print("3. Demonstrating error handling...")
        demonstrate_error_handling()
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
    
    print("\nProcessing examples completed!")


if __name__ == "__main__":
    main()