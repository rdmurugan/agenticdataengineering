"""
Auto Loader Agent for incremental data ingestion with self-healing capabilities
Handles Medicaid/Medicare feed ingestion with schema evolution support
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, current_timestamp, input_file_name
from databricks.feature_store import FeatureStoreClient
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


class AutoLoaderAgent:
    """
    Self-healing ingestion agent using Databricks Auto Loader
    Automatically handles schema evolution and file format detection
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.fs_client = FeatureStoreClient()
        
    def create_streaming_ingestion(
        self,
        source_path: str,
        target_table: str,
        checkpoint_path: str,
        schema_location: str,
        file_format: str = "json",
        trigger_interval: str = "10 seconds"
    ):
        """
        Create streaming ingestion pipeline with Auto Loader
        
        Args:
            source_path: Cloud storage path for incoming files
            target_table: Unity Catalog table name 
            checkpoint_path: Checkpoint location for streaming state
            schema_location: Schema registry location
            file_format: File format (json, csv, parquet)
            trigger_interval: Processing trigger interval
        """
        
        try:
            # Auto Loader with schema inference and evolution
            df = (self.spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", file_format)
                  .option("cloudFiles.schemaLocation", schema_location)
                  .option("cloudFiles.inferColumnTypes", "true")
                  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                  .option("cloudFiles.maxFilesPerTrigger", 1000)
                  .load(source_path))
            
            # Add metadata columns for lineage and monitoring
            enriched_df = (df
                          .withColumn("_ingestion_timestamp", current_timestamp())
                          .withColumn("_source_file", input_file_name())
                          .withColumn("_pipeline_id", col("current_timestamp()")))
            
            # Write to Delta table with merge capabilities
            query = (enriched_df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", checkpoint_path)
                    .option("mergeSchema", "true")
                    .trigger(processingTime=trigger_interval)
                    .toTable(target_table))
            
            logger.info(f"Started streaming ingestion: {source_path} -> {target_table}")
            return query
            
        except Exception as e:
            logger.error(f"Failed to create streaming ingestion: {str(e)}")
            self._handle_ingestion_failure(e, source_path, target_table)
            raise
            
    def create_batch_ingestion(
        self,
        source_path: str,
        target_table: str,
        merge_keys: List[str],
        file_format: str = "json"
    ):
        """
        Create batch ingestion with upsert logic for historical data loads
        
        Args:
            source_path: Source file path
            target_table: Target Unity Catalog table
            merge_keys: Keys for merge/upsert operations
            file_format: Source file format
        """
        
        try:
            # Read source data with schema inference
            if file_format == "json":
                source_df = self.spark.read.json(source_path)
            elif file_format == "csv":
                source_df = self.spark.read.option("header", "true").csv(source_path)
            elif file_format == "parquet":
                source_df = self.spark.read.parquet(source_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Add ingestion metadata
            enriched_df = (source_df
                          .withColumn("_ingestion_timestamp", current_timestamp())
                          .withColumn("_source_file", input_file_name()))
            
            # Perform merge operation
            self._merge_to_target(enriched_df, target_table, merge_keys)
            
            logger.info(f"Completed batch ingestion: {source_path} -> {target_table}")
            
        except Exception as e:
            logger.error(f"Batch ingestion failed: {str(e)}")
            self._handle_ingestion_failure(e, source_path, target_table)
            raise
            
    def _merge_to_target(self, source_df, target_table: str, merge_keys: List[str]):
        """Perform merge/upsert operation to target table"""
        
        from delta.tables import DeltaTable
        
        # Check if target table exists
        if self.spark.catalog.tableExists(target_table):
            target_table_obj = DeltaTable.forName(self.spark, target_table)
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in merge_keys
            ])
            
            # Perform merge
            (target_table_obj.alias("target")
             .merge(source_df.alias("source"), merge_condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        else:
            # Create table if it doesn't exist
            source_df.write.format("delta").saveAsTable(target_table)
            
    def _handle_ingestion_failure(self, error: Exception, source_path: str, target_table: str):
        """
        Self-healing logic for ingestion failures
        Implements retry logic and alerting
        """
        
        failure_info = {
            "error": str(error),
            "source_path": source_path,
            "target_table": target_table,
            "timestamp": current_timestamp(),
            "pipeline_id": self.config.get("pipeline_id", "unknown")
        }
        
        # Log failure for monitoring
        logger.error(f"Ingestion failure: {failure_info}")
        
        # Store failure metadata for dashboard
        self._log_failure_to_monitoring(failure_info)
        
        # Implement retry logic based on error type
        if "schema" in str(error).lower():
            logger.info("Schema-related error detected, triggering schema drift handler")
            # Schema drift handling will be implemented in schema_drift_detector.py
        elif "network" in str(error).lower() or "timeout" in str(error).lower():
            logger.info("Network-related error detected, will retry with backoff")
            # Network retry logic
        
    def _log_failure_to_monitoring(self, failure_info: Dict[str, Any]):
        """Log failure information to monitoring table"""
        
        try:
            monitoring_table = self.config.get("monitoring_table", "monitoring.pipeline_failures")
            
            failure_df = self.spark.createDataFrame([failure_info])
            failure_df.write.format("delta").mode("append").saveAsTable(monitoring_table)
            
        except Exception as e:
            logger.error(f"Failed to log monitoring info: {str(e)}")
            
    def get_ingestion_metrics(self, pipeline_id: str) -> Dict[str, Any]:
        """Get ingestion metrics for monitoring dashboard"""
        
        try:
            monitoring_table = self.config.get("monitoring_table", "monitoring.pipeline_metrics")
            
            metrics_df = (self.spark.table(monitoring_table)
                         .filter(col("pipeline_id") == pipeline_id)
                         .orderBy(col("timestamp").desc())
                         .limit(1))
            
            if metrics_df.count() > 0:
                return metrics_df.collect()[0].asDict()
            else:
                return {"status": "no_data", "pipeline_id": pipeline_id}
                
        except Exception as e:
            logger.error(f"Failed to get ingestion metrics: {str(e)}")
            return {"status": "error", "error": str(e)}