"""
Unit tests for CSV to Delta processing functionality.
"""

import unittest
import tempfile
import shutil
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import the module to test
from csv_to_delta import CSVToDeltaProcessor, create_sample_schema


class TestCSVToDeltaProcessor(unittest.TestCase):
    """Test cases for CSVToDeltaProcessor class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_dir = tempfile.mkdtemp()
        self.test_csv_path = Path(self.test_dir) / "test.csv"
        self.test_delta_path = Path(self.test_dir) / "delta_table"
        
        # Create a sample CSV file for testing
        csv_content = """id,name,email,age,city
1,John Doe,john@example.com,30,New York
2,Jane Smith,jane@example.com,25,Los Angeles
3,Bob Johnson,bob@example.com,35,Chicago"""
        
        with open(self.test_csv_path, 'w') as f:
            f.write(csv_content)
    
    def tearDown(self):
        """Clean up after each test method."""
        if Path(self.test_dir).exists():
            shutil.rmtree(self.test_dir)
    
    @patch('csv_to_delta.configure_spark_with_delta_pip')
    @patch('csv_to_delta.SparkSession')
    def test_processor_initialization(self, mock_spark_session, mock_configure):
        """Test processor initialization."""
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value = mock_spark_session.builder
        mock_configure.return_value.getOrCreate.return_value = mock_spark
        
        processor = CSVToDeltaProcessor("TestApp")
        
        self.assertIsNotNone(processor.spark)
        self.assertEqual(processor.logger.name, 'csv_to_delta')
    
    def test_create_sample_schema(self):
        """Test sample schema creation."""
        schema = create_sample_schema()
        
        self.assertEqual(len(schema.fields), 5)
        field_names = [field.name for field in schema.fields]
        expected_names = ["id", "name", "email", "age", "city"]
        
        self.assertEqual(field_names, expected_names)
    
    @patch('csv_to_delta.configure_spark_with_delta_pip')
    @patch('csv_to_delta.SparkSession')
    def test_csv_reading_configuration(self, mock_spark_session, mock_configure):
        """Test CSV reading with different options."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 3
        mock_df.columns = ["id", "name", "email", "age", "city"]
        mock_df.schema = create_sample_schema()
        
        mock_spark.read.options.return_value.csv.return_value = mock_df
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value = mock_spark_session.builder
        mock_configure.return_value.getOrCreate.return_value = mock_spark
        
        processor = CSVToDeltaProcessor("TestApp")
        
        # Test with default options
        result_df = processor.read_csv_with_schema_inference(str(self.test_csv_path))
        
        self.assertEqual(result_df.count(), 3)
        self.assertEqual(len(result_df.columns), 5)
        
        # Test with custom options
        custom_options = {"delimiter": ";", "encoding": "UTF-16"}
        result_df = processor.read_csv_with_schema_inference(
            str(self.test_csv_path), 
            custom_options
        )
        
        # Verify that custom options were applied
        mock_spark.read.options.assert_called()
    
    @patch('csv_to_delta.configure_spark_with_delta_pip')
    @patch('csv_to_delta.SparkSession')
    def test_delta_writing_configuration(self, mock_spark_session, mock_configure):
        """Test Delta table writing with different configurations."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_writer = MagicMock()
        
        mock_df.withColumn.return_value = mock_df
        mock_df.write.format.return_value.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.save = MagicMock()
        
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value = mock_spark_session.builder
        mock_configure.return_value.getOrCreate.return_value = mock_spark
        
        processor = CSVToDeltaProcessor("TestApp")
        
        # Test writing with default options
        processor.write_to_delta_table(mock_df, str(self.test_delta_path))
        mock_writer.save.assert_called_with(str(self.test_delta_path))
        
        # Test writing with partitioning
        processor.write_to_delta_table(
            mock_df, 
            str(self.test_delta_path), 
            partition_cols=["city"]
        )
        mock_writer.partitionBy.assert_called_with("city")
    
    @patch('csv_to_delta.configure_spark_with_delta_pip')
    @patch('csv_to_delta.SparkSession')
    def test_multiple_csv_processing(self, mock_spark_session, mock_configure):
        """Test processing multiple CSV files."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_df.columns = ["col1", "col2"]
        mock_df.schema = MagicMock()
        
        mock_spark.read.options.return_value.csv.return_value = mock_df
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value = mock_spark_session.builder
        mock_configure.return_value.getOrCreate.return_value = mock_spark
        
        processor = CSVToDeltaProcessor("TestApp")
        
        # Create test configurations
        configs = [
            {
                "file_path": str(self.test_csv_path),
                "delta_path": str(self.test_delta_path) + "_1",
                "mode": "overwrite"
            },
            {
                "file_path": str(self.test_csv_path),
                "delta_path": str(self.test_delta_path) + "_2",
                "mode": "append",
                "partition_cols": ["col1"]
            }
        ]
        
        # Process multiple CSVs
        processor.process_multiple_csvs(configs)
        
        # Verify that CSV reading was called for each config
        self.assertEqual(mock_spark.read.options.call_count, 2)
    
    @patch('csv_to_delta.configure_spark_with_delta_pip')
    @patch('csv_to_delta.SparkSession')
    def test_error_handling(self, mock_spark_session, mock_configure):
        """Test error handling in various scenarios."""
        mock_spark = MagicMock()
        mock_spark.read.options.return_value.csv.side_effect = Exception("File not found")
        
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value = mock_spark_session.builder
        mock_configure.return_value.getOrCreate.return_value = mock_spark
        
        processor = CSVToDeltaProcessor("TestApp")
        
        # Test error handling in CSV reading
        with self.assertRaises(Exception):
            processor.read_csv_with_schema_inference("nonexistent.csv")
        
        # Test error handling in multiple CSV processing
        invalid_configs = [
            {
                "file_path": "nonexistent1.csv",
                "delta_path": "delta1"
            },
            {
                "file_path": "nonexistent2.csv", 
                "delta_path": "delta2"
            }
        ]
        
        # Should not raise exception but should log errors
        processor.process_multiple_csvs(invalid_configs)
    
    @patch('csv_to_delta.configure_spark_with_delta_pip')
    @patch('csv_to_delta.SparkSession')
    def test_table_info_retrieval(self, mock_spark_session, mock_configure):
        """Test retrieving Delta table information."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "name", "value"]
        mock_df.schema.json.return_value = '{"type":"struct","fields":[]}'
        mock_df.rdd.getNumPartitions.return_value = 4
        
        mock_spark.read.format.return_value.load.return_value = mock_df
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value.config.return_value = mock_spark_session.builder
        mock_configure.return_value.getOrCreate.return_value = mock_spark
        
        processor = CSVToDeltaProcessor("TestApp")
        
        # Test table info retrieval
        info = processor.get_table_info("delta/test_table")
        
        expected_info = {
            "row_count": 100,
            "column_count": 3,
            "columns": ["id", "name", "value"],
            "schema": '{"type":"struct","fields":[]}',
            "partitions": 4
        }
        
        self.assertEqual(info, expected_info)
    
    def test_configuration_validation(self):
        """Test validation of configuration parameters."""
        # Test invalid configuration (missing required fields)
        with patch('csv_to_delta.configure_spark_with_delta_pip'), \
             patch('csv_to_delta.SparkSession'):
            
            processor = CSVToDeltaProcessor("TestApp")
            
            invalid_configs = [
                {
                    "file_path": "test.csv"
                    # Missing delta_path
                },
                {
                    "delta_path": "delta/test"
                    # Missing file_path
                }
            ]
            
            # Should handle invalid configurations gracefully
            processor.process_multiple_csvs(invalid_configs)


class TestUtilityFunctions(unittest.TestCase):
    """Test cases for utility functions."""
    
    def test_sample_schema_structure(self):
        """Test the structure of the sample schema."""
        schema = create_sample_schema()
        
        # Check that all fields are StringType (as defined in the function)
        for field in schema.fields:
            self.assertEqual(str(field.dataType), "StringType()")
            self.assertTrue(field.nullable)  # All fields are nullable in sample
        
        # Check specific field names
        field_names = [field.name for field in schema.fields]
        self.assertIn("id", field_names)
        self.assertIn("name", field_names)
        self.assertIn("email", field_names)


if __name__ == '__main__':
    # Set up test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_suite.addTest(unittest.makeSuite(TestCSVToDeltaProcessor))
    test_suite.addTest(unittest.makeSuite(TestUtilityFunctions))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    print(f"{'='*60}")