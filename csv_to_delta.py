"""
CSV to Delta Table Processing Module

This module provides functionality to read various CSV files with different schemas
and write them to Delta tables using Apache Spark.
"""

import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp
from delta import configure_spark_with_delta_pip


class CSVToDeltaProcessor:
    """
    A processor for reading CSV files with various schemas and writing to Delta tables.
    """
    
    def __init__(self, app_name: str = "CSV-to-Delta-Processor"):
        """
        Initialize the CSV to Delta processor.
        
        Args:
            app_name: Name of the Spark application
        """
        self.logger = self._setup_logging()
        self.spark = self._create_spark_session(app_name)
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _create_spark_session(self, app_name: str) -> SparkSession:
        """
        Create and configure Spark session with Delta Lake support.
        
        Args:
            app_name: Name of the Spark application
            
        Returns:
            Configured SparkSession
        """
        try:
            builder = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info(f"Spark session created successfully: {app_name}")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def read_csv_with_schema_inference(self, 
                                     file_path: str, 
                                     options: Optional[Dict[str, Any]] = None) -> 'DataFrame':
        """
        Read CSV file with automatic schema inference.
        
        Args:
            file_path: Path to the CSV file
            options: Additional CSV reading options
            
        Returns:
            Spark DataFrame
        """
        default_options = {
            "header": "true",
            "inferSchema": "true",
            "multiline": "true",
            "escape": '"',
            "encoding": "UTF-8"
        }
        
        if options:
            default_options.update(options)
            
        try:
            self.logger.info(f"Reading CSV file: {file_path}")
            df = self.spark.read.options(**default_options).csv(file_path)
            
            self.logger.info(f"Successfully read CSV with {df.count()} rows and {len(df.columns)} columns")
            self.logger.info(f"Schema: {df.schema}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read CSV file {file_path}: {str(e)}")
            raise
    
    def read_csv_with_custom_schema(self, 
                                  file_path: str, 
                                  schema: StructType,
                                  options: Optional[Dict[str, Any]] = None) -> 'DataFrame':
        """
        Read CSV file with a predefined schema.
        
        Args:
            file_path: Path to the CSV file
            schema: Predefined schema for the CSV
            options: Additional CSV reading options
            
        Returns:
            Spark DataFrame
        """
        default_options = {
            "header": "true",
            "multiline": "true",
            "escape": '"',
            "encoding": "UTF-8"
        }
        
        if options:
            default_options.update(options)
            
        try:
            self.logger.info(f"Reading CSV file with custom schema: {file_path}")
            df = self.spark.read.options(**default_options).schema(schema).csv(file_path)
            
            self.logger.info(f"Successfully read CSV with custom schema")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read CSV file {file_path} with custom schema: {str(e)}")
            raise
    
    def write_to_delta_table(self, 
                           df: 'DataFrame', 
                           table_path: str,
                           mode: str = "overwrite",
                           partition_cols: Optional[List[str]] = None,
                           add_metadata: bool = True) -> None:
        """
        Write DataFrame to Delta table.
        
        Args:
            df: Spark DataFrame to write
            table_path: Path where Delta table will be stored
            mode: Write mode (overwrite, append, merge)
            partition_cols: Columns to partition by
            add_metadata: Whether to add processing metadata
        """
        try:
            # Add processing metadata if requested
            if add_metadata:
                df = df.withColumn("processed_timestamp", current_timestamp())
            
            writer = df.write.format("delta").mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
                self.logger.info(f"Partitioning by columns: {partition_cols}")
            
            writer.save(table_path)
            
            self.logger.info(f"Successfully wrote DataFrame to Delta table: {table_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to write to Delta table {table_path}: {str(e)}")
            raise
    
    def process_multiple_csvs(self, 
                            csv_configs: List[Dict[str, Any]]) -> None:
        """
        Process multiple CSV files based on configuration.
        
        Args:
            csv_configs: List of dictionaries containing CSV processing configuration
                        Each dict should have: file_path, delta_path, and optional schema/options
        """
        self.logger.info(f"Processing {len(csv_configs)} CSV files")
        
        for i, config in enumerate(csv_configs):
            try:
                file_path = config.get("file_path")
                delta_path = config.get("delta_path")
                schema = config.get("schema")
                options = config.get("options", {})
                partition_cols = config.get("partition_cols")
                mode = config.get("mode", "overwrite")
                
                if not file_path or not delta_path:
                    raise ValueError(f"Config {i}: file_path and delta_path are required")
                
                self.logger.info(f"Processing CSV {i+1}/{len(csv_configs)}: {file_path}")
                
                # Read CSV with or without custom schema
                if schema:
                    df = self.read_csv_with_custom_schema(file_path, schema, options)
                else:
                    df = self.read_csv_with_schema_inference(file_path, options)
                
                # Write to Delta table
                self.write_to_delta_table(df, delta_path, mode, partition_cols)
                
                self.logger.info(f"Completed processing: {file_path} -> {delta_path}")
                
            except Exception as e:
                self.logger.error(f"Failed to process CSV config {i}: {str(e)}")
                continue
    
    def get_table_info(self, table_path: str) -> Dict[str, Any]:
        """
        Get information about a Delta table.
        
        Args:
            table_path: Path to the Delta table
            
        Returns:
            Dictionary containing table information
        """
        try:
            df = self.spark.read.format("delta").load(table_path)
            
            info = {
                "row_count": df.count(),
                "column_count": len(df.columns),
                "columns": df.columns,
                "schema": df.schema.json(),
                "partitions": df.rdd.getNumPartitions()
            }
            
            self.logger.info(f"Retrieved info for Delta table: {table_path}")
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get info for Delta table {table_path}: {str(e)}")
            raise
    
    def close(self) -> None:
        """Close the Spark session."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session closed")


def create_sample_schema() -> StructType:
    """
    Create a sample schema for demonstration purposes.
    
    Returns:
        Sample StructType schema
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", StringType(), True),
        StructField("city", StringType(), True)
    ])


def main():
    """
    Main function demonstrating usage of CSVToDeltaProcessor.
    """
    processor = CSVToDeltaProcessor("CSV-Delta-Demo")
    
    try:
        # Example configuration for processing multiple CSV files
        csv_configs = [
            {
                "file_path": "data/users.csv",
                "delta_path": "delta/users_table",
                "mode": "overwrite",
                "options": {"delimiter": ",", "header": "true"}
            },
            {
                "file_path": "data/products.csv", 
                "delta_path": "delta/products_table",
                "mode": "overwrite",
                "partition_cols": ["category"]
            }
        ]
        
        # Process the CSV files
        processor.process_multiple_csvs(csv_configs)
        
        # Get table information
        for config in csv_configs:
            if Path(config["delta_path"]).exists():
                info = processor.get_table_info(config["delta_path"])
                print(f"Table info for {config['delta_path']}: {json.dumps(info, indent=2)}")
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
    
    finally:
        processor.close()


if __name__ == "__main__":
    main()