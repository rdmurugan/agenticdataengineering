"""
Schema Drift Detection and Auto-Healing for Healthcare Data Feeds
Monitors schema changes and automatically adapts ingestion pipelines
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DataType
from databricks.sdk import WorkspaceClient
import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class SchemaDriftDetector:
    """
    Detects schema drift in healthcare feeds and automatically adapts pipelines
    Handles both forward and backward compatibility scenarios
    """
    
    def __init__(self, spark: SparkSession, workspace_client: WorkspaceClient, config: Dict[str, Any]):
        self.spark = spark
        self.workspace_client = workspace_client
        self.config = config
        self.schema_registry_table = config.get("schema_registry_table", "schema_registry.table_schemas")
        
    def detect_schema_changes(self, table_name: str, new_schema: StructType) -> Dict[str, Any]:
        """
        Compare new schema with registered schema to detect drift
        
        Args:
            table_name: Unity Catalog table name
            new_schema: New schema from incoming data
            
        Returns:
            Dictionary with drift detection results
        """
        
        try:
            # Get current registered schema
            current_schema = self._get_registered_schema(table_name)
            
            if current_schema is None:
                # First time ingestion - register schema
                self._register_schema(table_name, new_schema)
                return {
                    "drift_detected": False,
                    "change_type": "initial_registration",
                    "changes": [],
                    "compatibility": "full"
                }
            
            # Compare schemas
            changes = self._compare_schemas(current_schema, new_schema)
            
            if not changes:
                return {
                    "drift_detected": False,
                    "change_type": "no_change",
                    "changes": [],
                    "compatibility": "full"
                }
            
            # Analyze compatibility
            compatibility = self._analyze_compatibility(changes)
            
            # Auto-register compatible changes
            if compatibility in ["forward", "full"]:
                self._register_schema(table_name, new_schema, changes)
                
            drift_result = {
                "drift_detected": True,
                "change_type": self._classify_change_type(changes),
                "changes": changes,
                "compatibility": compatibility,
                "timestamp": datetime.now().isoformat(),
                "table_name": table_name
            }
            
            # Log and handle the drift
            self._handle_schema_drift(drift_result)
            
            return drift_result
            
        except Exception as e:
            logger.error(f"Schema drift detection failed: {str(e)}")
            return {
                "drift_detected": False,
                "error": str(e),
                "table_name": table_name
            }
            
    def _get_registered_schema(self, table_name: str) -> Optional[StructType]:
        """Retrieve registered schema from schema registry"""
        
        try:
            schema_df = (self.spark.table(self.schema_registry_table)
                        .filter(f"table_name = '{table_name}'")
                        .filter("is_active = true")
                        .orderBy("version DESC")
                        .limit(1))
            
            if schema_df.count() == 0:
                return None
                
            schema_json = schema_df.select("schema_json").collect()[0][0]
            return StructType.fromJson(json.loads(schema_json))
            
        except Exception as e:
            logger.warning(f"Could not retrieve registered schema: {str(e)}")
            return None
            
    def _register_schema(self, table_name: str, schema: StructType, changes: List[Dict] = None):
        """Register new schema version in schema registry"""
        
        try:
            # Get next version number
            version_df = (self.spark.table(self.schema_registry_table)
                         .filter(f"table_name = '{table_name}'")
                         .agg({"version": "max"}))
            
            max_version = version_df.collect()[0][0]
            new_version = (max_version or 0) + 1
            
            # Deactivate previous versions
            if max_version is not None:
                self.spark.sql(f"""
                    UPDATE {self.schema_registry_table}
                    SET is_active = false
                    WHERE table_name = '{table_name}' AND is_active = true
                """)
            
            # Register new schema
            schema_record = {
                "table_name": table_name,
                "version": new_version,
                "schema_json": json.dumps(schema.jsonValue()),
                "changes": json.dumps(changes or []),
                "registered_at": datetime.now().isoformat(),
                "is_active": True,
                "registered_by": "schema_drift_detector"
            }
            
            schema_df = self.spark.createDataFrame([schema_record])
            schema_df.write.format("delta").mode("append").saveAsTable(self.schema_registry_table)
            
            logger.info(f"Registered schema version {new_version} for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to register schema: {str(e)}")
            
    def _compare_schemas(self, old_schema: StructType, new_schema: StructType) -> List[Dict[str, Any]]:
        """Compare two schemas and return list of changes"""
        
        changes = []
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # Check for added fields
        for field_name, field in new_fields.items():
            if field_name not in old_fields:
                changes.append({
                    "change_type": "field_added",
                    "field_name": field_name,
                    "data_type": str(field.dataType),
                    "nullable": field.nullable
                })
                
        # Check for removed fields
        for field_name, field in old_fields.items():
            if field_name not in new_fields:
                changes.append({
                    "change_type": "field_removed",
                    "field_name": field_name,
                    "data_type": str(field.dataType),
                    "nullable": field.nullable
                })
                
        # Check for modified fields
        for field_name in set(old_fields.keys()) & set(new_fields.keys()):
            old_field = old_fields[field_name]
            new_field = new_fields[field_name]
            
            if old_field.dataType != new_field.dataType:
                changes.append({
                    "change_type": "data_type_changed",
                    "field_name": field_name,
                    "old_data_type": str(old_field.dataType),
                    "new_data_type": str(new_field.dataType),
                    "old_nullable": old_field.nullable,
                    "new_nullable": new_field.nullable
                })
            elif old_field.nullable != new_field.nullable:
                changes.append({
                    "change_type": "nullability_changed",
                    "field_name": field_name,
                    "data_type": str(new_field.dataType),
                    "old_nullable": old_field.nullable,
                    "new_nullable": new_field.nullable
                })
                
        return changes
        
    def _analyze_compatibility(self, changes: List[Dict[str, Any]]) -> str:
        """
        Analyze schema compatibility based on changes
        Returns: full, forward, backward, breaking
        """
        
        if not changes:
            return "full"
            
        has_additions = any(c["change_type"] == "field_added" for c in changes)
        has_removals = any(c["change_type"] == "field_removed" for c in changes)
        has_type_changes = any(c["change_type"] == "data_type_changed" for c in changes)
        
        # Breaking changes
        if has_type_changes or has_removals:
            return "breaking"
            
        # Only additions - forward compatible
        if has_additions and not (has_removals or has_type_changes):
            return "forward"
            
        return "backward"
        
    def _classify_change_type(self, changes: List[Dict[str, Any]]) -> str:
        """Classify overall change type"""
        
        change_types = {c["change_type"] for c in changes}
        
        if "data_type_changed" in change_types:
            return "major_change"
        elif "field_removed" in change_types:
            return "field_removal"
        elif "field_added" in change_types:
            return "field_addition"
        else:
            return "minor_change"
            
    def _handle_schema_drift(self, drift_result: Dict[str, Any]):
        """Handle detected schema drift with appropriate actions"""
        
        table_name = drift_result["table_name"]
        compatibility = drift_result["compatibility"]
        changes = drift_result["changes"]
        
        # Log drift event
        logger.info(f"Schema drift detected for {table_name}: {compatibility}")
        
        # Store drift event for monitoring
        self._log_drift_event(drift_result)
        
        # Take action based on compatibility
        if compatibility == "breaking":
            self._handle_breaking_changes(table_name, changes)
        elif compatibility in ["forward", "backward"]:
            self._handle_compatible_changes(table_name, changes)
            
        # Trigger pipeline restart if needed
        if compatibility in ["breaking", "major_change"]:
            self._trigger_pipeline_restart(table_name)
            
    def _handle_breaking_changes(self, table_name: str, changes: List[Dict[str, Any]]):
        """Handle breaking schema changes"""
        
        logger.warning(f"Breaking changes detected for {table_name}")
        
        # Create versioned table for backward compatibility
        versioned_table = f"{table_name}_v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Create new versioned table
            self.spark.sql(f"CREATE TABLE {versioned_table} USING DELTA AS SELECT * FROM {table_name}")
            
            # Update table mapping
            self._update_table_mapping(table_name, versioned_table)
            
            logger.info(f"Created versioned table: {versioned_table}")
            
        except Exception as e:
            logger.error(f"Failed to handle breaking changes: {str(e)}")
            
    def _handle_compatible_changes(self, table_name: str, changes: List[Dict[str, Any]]):
        """Handle compatible schema changes"""
        
        logger.info(f"Compatible changes detected for {table_name}, auto-adapting")
        
        # For forward compatible changes, Delta Lake will handle schema evolution automatically
        # Log the successful adaptation
        adaptation_event = {
            "table_name": table_name,
            "changes": changes,
            "adaptation_status": "success",
            "timestamp": datetime.now().isoformat()
        }
        
        self._log_adaptation_event(adaptation_event)
        
    def _log_drift_event(self, drift_result: Dict[str, Any]):
        """Log schema drift event to monitoring table"""
        
        try:
            monitoring_table = self.config.get("drift_monitoring_table", "monitoring.schema_drift_events")
            
            drift_df = self.spark.createDataFrame([drift_result])
            drift_df.write.format("delta").mode("append").saveAsTable(monitoring_table)
            
        except Exception as e:
            logger.error(f"Failed to log drift event: {str(e)}")
            
    def _log_adaptation_event(self, adaptation_event: Dict[str, Any]):
        """Log successful adaptation event"""
        
        try:
            adaptation_table = self.config.get("adaptation_monitoring_table", "monitoring.schema_adaptations")
            
            adaptation_df = self.spark.createDataFrame([adaptation_event])
            adaptation_df.write.format("delta").mode("append").saveAsTable(adaptation_table)
            
        except Exception as e:
            logger.error(f"Failed to log adaptation event: {str(e)}")
            
    def _trigger_pipeline_restart(self, table_name: str):
        """Trigger pipeline restart for breaking changes"""
        
        try:
            pipeline_id = self.config.get("pipeline_mapping", {}).get(table_name)
            
            if pipeline_id:
                # This would integrate with the Orchestration Agent
                logger.info(f"Triggering restart for pipeline {pipeline_id}")
                # Implementation will be in orchestration agent
            
        except Exception as e:
            logger.error(f"Failed to trigger pipeline restart: {str(e)}")
            
    def get_schema_drift_metrics(self, table_name: str, days: int = 30) -> Dict[str, Any]:
        """Get schema drift metrics for monitoring dashboard"""
        
        try:
            monitoring_table = self.config.get("drift_monitoring_table", "monitoring.schema_drift_events")
            
            metrics_df = (self.spark.table(monitoring_table)
                         .filter(f"table_name = '{table_name}'")
                         .filter(f"timestamp >= current_date() - {days}"))
            
            total_events = metrics_df.count()
            compatibility_stats = (metrics_df.groupBy("compatibility")
                                  .count()
                                  .collect())
            
            return {
                "table_name": table_name,
                "total_drift_events": total_events,
                "compatibility_breakdown": {row["compatibility"]: row["count"] 
                                          for row in compatibility_stats},
                "period_days": days
            }
            
        except Exception as e:
            logger.error(f"Failed to get drift metrics: {str(e)}")
            return {"error": str(e), "table_name": table_name}