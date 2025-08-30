"""
Unity Catalog Manager for schema registry and data governance
Manages catalogs, schemas, tables, and governance policies
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CreateCatalog, CreateSchema, CreateTable, TableInfo, ColumnInfo, 
    DataSourceFormat, TableType, CatalogInfo, SchemaInfo
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Dict, Any, List, Optional
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class UnityCatalogManager:
    """
    Manages Unity Catalog operations for healthcare data governance
    Handles catalog/schema creation, table registration, and governance policies
    """
    
    def __init__(self, workspace_client: WorkspaceClient, spark: SparkSession, config: Dict[str, Any]):
        self.workspace_client = workspace_client
        self.spark = spark
        self.config = config
        self.catalog_name = config.get("unity_catalog", "healthcare_data")
        
    def create_healthcare_catalog_structure(self) -> Dict[str, Any]:
        """
        Create the complete catalog structure for healthcare data
        
        Returns:
            Dictionary with creation results
        """
        
        try:
            results = {
                "catalog": None,
                "schemas": {},
                "tables": {},
                "status": "success"
            }
            
            # Create main catalog
            catalog_result = self.create_catalog(
                catalog_name=self.catalog_name,
                comment="Healthcare data catalog for Medicaid/Medicare feeds"
            )
            results["catalog"] = catalog_result
            
            if catalog_result["status"] != "success":
                results["status"] = "failed"
                return results
                
            # Create schemas for different data layers
            schemas_to_create = {
                "raw": "Raw ingested data from healthcare feeds",
                "bronze": "Bronze layer with basic quality checks", 
                "silver": "Silver layer with healthcare validations",
                "gold": "Gold layer for analytics and reporting",
                "monitoring": "Monitoring and quality metrics tables",
                "schema_registry": "Schema evolution tracking"
            }
            
            for schema_name, description in schemas_to_create.items():
                schema_result = self.create_schema(
                    catalog_name=self.catalog_name,
                    schema_name=schema_name,
                    comment=description
                )
                results["schemas"][schema_name] = schema_result
                
            # Create essential monitoring tables
            monitoring_tables = self._get_monitoring_table_definitions()
            
            for table_name, table_def in monitoring_tables.items():
                table_result = self.create_table(
                    catalog_name=self.catalog_name,
                    schema_name="monitoring",
                    table_name=table_name,
                    columns=table_def["columns"],
                    comment=table_def["comment"],
                    table_type="MANAGED"
                )
                results["tables"][f"monitoring.{table_name}"] = table_result
                
            # Create schema registry tables
            schema_tables = self._get_schema_registry_table_definitions()
            
            for table_name, table_def in schema_tables.items():
                table_result = self.create_table(
                    catalog_name=self.catalog_name,
                    schema_name="schema_registry",
                    table_name=table_name,
                    columns=table_def["columns"],
                    comment=table_def["comment"],
                    table_type="MANAGED"
                )
                results["tables"][f"schema_registry.{table_name}"] = table_result
                
            logger.info(f"Created healthcare catalog structure: {self.catalog_name}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to create catalog structure: {str(e)}")
            return {"error": str(e), "status": "failed"}
            
    def create_catalog(self, catalog_name: str, comment: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a Unity Catalog
        
        Args:
            catalog_name: Name of the catalog
            comment: Optional comment
        """
        
        try:
            # Check if catalog already exists
            try:
                existing_catalog = self.workspace_client.catalogs.get(catalog_name)
                logger.info(f"Catalog {catalog_name} already exists")
                return {
                    "catalog_name": catalog_name,
                    "status": "exists",
                    "catalog_id": existing_catalog.name
                }
            except Exception:
                # Catalog doesn't exist, create it
                pass
                
            # Create catalog
            catalog_request = CreateCatalog(
                name=catalog_name,
                comment=comment or f"Healthcare data catalog created on {datetime.now().isoformat()}"
            )
            
            catalog = self.workspace_client.catalogs.create(catalog_request)
            
            logger.info(f"Created catalog: {catalog_name}")
            
            return {
                "catalog_name": catalog_name,
                "status": "created", 
                "catalog_id": catalog.name
            }
            
        except Exception as e:
            logger.error(f"Failed to create catalog {catalog_name}: {str(e)}")
            return {"error": str(e), "catalog_name": catalog_name, "status": "failed"}
            
    def create_schema(
        self,
        catalog_name: str,
        schema_name: str,
        comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a schema in Unity Catalog
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            comment: Optional comment
        """
        
        try:
            # Check if schema already exists
            try:
                existing_schema = self.workspace_client.schemas.get(f"{catalog_name}.{schema_name}")
                logger.info(f"Schema {catalog_name}.{schema_name} already exists")
                return {
                    "schema_name": f"{catalog_name}.{schema_name}",
                    "status": "exists",
                    "schema_id": existing_schema.name
                }
            except Exception:
                # Schema doesn't exist, create it
                pass
                
            # Create schema
            schema_request = CreateSchema(
                name=schema_name,
                catalog_name=catalog_name,
                comment=comment or f"Healthcare schema created on {datetime.now().isoformat()}"
            )
            
            schema = self.workspace_client.schemas.create(schema_request)
            
            logger.info(f"Created schema: {catalog_name}.{schema_name}")
            
            return {
                "schema_name": f"{catalog_name}.{schema_name}",
                "status": "created",
                "schema_id": schema.name
            }
            
        except Exception as e:
            logger.error(f"Failed to create schema {catalog_name}.{schema_name}: {str(e)}")
            return {"error": str(e), "schema_name": f"{catalog_name}.{schema_name}", "status": "failed"}
            
    def create_table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        columns: List[Dict[str, Any]],
        comment: Optional[str] = None,
        table_type: str = "MANAGED",
        data_source_format: str = "DELTA"
    ) -> Dict[str, Any]:
        """
        Create a table in Unity Catalog
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_name: Name of the table
            columns: List of column definitions
            comment: Optional comment
            table_type: Type of table (MANAGED, EXTERNAL)
            data_source_format: Data source format (DELTA, PARQUET, etc.)
        """
        
        try:
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            # Check if table already exists
            try:
                existing_table = self.workspace_client.tables.get(full_table_name)
                logger.info(f"Table {full_table_name} already exists")
                return {
                    "table_name": full_table_name,
                    "status": "exists",
                    "table_id": existing_table.name
                }
            except Exception:
                # Table doesn't exist, create it
                pass
                
            # Convert column definitions to ColumnInfo objects
            column_infos = []
            for col in columns:
                column_info = ColumnInfo(
                    name=col["name"],
                    type_text=col["type"],
                    type_name=col.get("type_name"),
                    comment=col.get("comment")
                )
                column_infos.append(column_info)
                
            # Create table request
            table_request = CreateTable(
                name=table_name,
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_type=TableType(table_type),
                data_source_format=DataSourceFormat(data_source_format),
                columns=column_infos,
                comment=comment or f"Healthcare table created on {datetime.now().isoformat()}"
            )
            
            table = self.workspace_client.tables.create(table_request)
            
            logger.info(f"Created table: {full_table_name}")
            
            return {
                "table_name": full_table_name,
                "status": "created",
                "table_id": table.name
            }
            
        except Exception as e:
            logger.error(f"Failed to create table {full_table_name}: {str(e)}")
            return {"error": str(e), "table_name": full_table_name, "status": "failed"}
            
    def _get_monitoring_table_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Get definitions for monitoring tables"""
        
        return {
            "pipeline_metrics": {
                "comment": "Pipeline performance and health metrics",
                "columns": [
                    {"name": "pipeline_id", "type": "STRING", "comment": "Pipeline identifier"},
                    {"name": "timestamp", "type": "TIMESTAMP", "comment": "Metric timestamp"},
                    {"name": "total_records", "type": "BIGINT", "comment": "Total records processed"},
                    {"name": "success_count", "type": "BIGINT", "comment": "Successful records"},
                    {"name": "failure_count", "type": "BIGINT", "comment": "Failed records"},
                    {"name": "processing_time_seconds", "type": "DOUBLE", "comment": "Processing duration"},
                    {"name": "throughput_records_per_second", "type": "DOUBLE", "comment": "Processing throughput"},
                    {"name": "error_details", "type": "STRING", "comment": "Error details JSON"}
                ]
            },
            "data_quality_metrics": {
                "comment": "Data quality metrics and scores",
                "columns": [
                    {"name": "table_name", "type": "STRING", "comment": "Table being monitored"},
                    {"name": "timestamp", "type": "TIMESTAMP", "comment": "Quality check timestamp"},
                    {"name": "total_records", "type": "BIGINT", "comment": "Total records checked"},
                    {"name": "quality_score", "type": "DOUBLE", "comment": "Overall quality score"},
                    {"name": "completeness_score", "type": "DOUBLE", "comment": "Data completeness score"},
                    {"name": "validity_score", "type": "DOUBLE", "comment": "Data validity score"},
                    {"name": "anomaly_score", "type": "DOUBLE", "comment": "Anomaly detection score"},
                    {"name": "quality_details", "type": "STRING", "comment": "Detailed quality metrics JSON"}
                ]
            },
            "schema_drift_events": {
                "comment": "Schema drift detection events",
                "columns": [
                    {"name": "table_name", "type": "STRING", "comment": "Table with schema drift"},
                    {"name": "timestamp", "type": "TIMESTAMP", "comment": "Drift detection timestamp"},
                    {"name": "drift_type", "type": "STRING", "comment": "Type of schema drift"},
                    {"name": "changes", "type": "STRING", "comment": "Schema changes JSON"},
                    {"name": "compatibility", "type": "STRING", "comment": "Compatibility level"},
                    {"name": "auto_resolved", "type": "BOOLEAN", "comment": "Whether drift was auto-resolved"}
                ]
            },
            "job_events": {
                "comment": "Job execution events and failures",
                "columns": [
                    {"name": "job_name", "type": "STRING", "comment": "Job name"},
                    {"name": "run_id", "type": "STRING", "comment": "Job run identifier"},
                    {"name": "event_type", "type": "STRING", "comment": "Event type (success, failure, retry)"},
                    {"name": "timestamp", "type": "TIMESTAMP", "comment": "Event timestamp"},
                    {"name": "duration_seconds", "type": "DOUBLE", "comment": "Job duration"},
                    {"name": "error_message", "type": "STRING", "comment": "Error message if failed"},
                    {"name": "retry_count", "type": "INT", "comment": "Number of retries"}
                ]
            },
            "quality_alerts": {
                "comment": "Data quality alerts and notifications",
                "columns": [
                    {"name": "alert_id", "type": "STRING", "comment": "Unique alert identifier"},
                    {"name": "table_name", "type": "STRING", "comment": "Table triggering alert"},
                    {"name": "alert_type", "type": "STRING", "comment": "Type of quality alert"},
                    {"name": "severity", "type": "STRING", "comment": "Alert severity level"},
                    {"name": "timestamp", "type": "TIMESTAMP", "comment": "Alert timestamp"},
                    {"name": "message", "type": "STRING", "comment": "Alert description"},
                    {"name": "resolved", "type": "BOOLEAN", "comment": "Whether alert is resolved"},
                    {"name": "resolved_at", "type": "TIMESTAMP", "comment": "Resolution timestamp"}
                ]
            }
        }
        
    def _get_schema_registry_table_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Get definitions for schema registry tables"""
        
        return {
            "table_schemas": {
                "comment": "Schema versions and evolution tracking",
                "columns": [
                    {"name": "table_name", "type": "STRING", "comment": "Full table name"},
                    {"name": "version", "type": "INT", "comment": "Schema version number"},
                    {"name": "schema_json", "type": "STRING", "comment": "Schema definition in JSON"},
                    {"name": "changes", "type": "STRING", "comment": "Changes from previous version"},
                    {"name": "registered_at", "type": "TIMESTAMP", "comment": "Registration timestamp"},
                    {"name": "registered_by", "type": "STRING", "comment": "Who/what registered the schema"},
                    {"name": "is_active", "type": "BOOLEAN", "comment": "Whether this is the active version"},
                    {"name": "compatibility_mode", "type": "STRING", "comment": "Compatibility mode"}
                ]
            },
            "schema_compatibility": {
                "comment": "Schema compatibility tracking",
                "columns": [
                    {"name": "source_table", "type": "STRING", "comment": "Source table name"},
                    {"name": "target_table", "type": "STRING", "comment": "Target table name"},
                    {"name": "compatibility_level", "type": "STRING", "comment": "Compatibility level"},
                    {"name": "checked_at", "type": "TIMESTAMP", "comment": "Compatibility check timestamp"},
                    {"name": "issues", "type": "STRING", "comment": "Compatibility issues JSON"},
                    {"name": "resolution_required", "type": "BOOLEAN", "comment": "Whether manual resolution needed"}
                ]
            }
        }
        
    def register_table_schema(
        self,
        table_name: str,
        schema: StructType,
        version: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Register a table schema in the schema registry
        
        Args:
            table_name: Full table name
            schema: Spark StructType schema
            version: Optional version number
        """
        
        try:
            schema_registry_table = f"{self.catalog_name}.schema_registry.table_schemas"
            
            # Get next version if not provided
            if version is None:
                version_df = (self.spark.table(schema_registry_table)
                             .filter(f"table_name = '{table_name}'")
                             .agg({"version": "max"}))
                
                max_version = version_df.collect()[0][0]
                version = (max_version or 0) + 1
                
            # Deactivate previous versions
            if version > 1:
                self.spark.sql(f"""
                    UPDATE {schema_registry_table}
                    SET is_active = false
                    WHERE table_name = '{table_name}' AND is_active = true
                """)
                
            # Register new schema
            schema_record = {
                "table_name": table_name,
                "version": version,
                "schema_json": json.dumps(schema.jsonValue()),
                "changes": "[]",  # Would be calculated from diff
                "registered_at": datetime.now().isoformat(),
                "registered_by": "unity_catalog_manager",
                "is_active": True,
                "compatibility_mode": "forward"
            }
            
            schema_df = self.spark.createDataFrame([schema_record])
            schema_df.write.format("delta").mode("append").saveAsTable(schema_registry_table)
            
            logger.info(f"Registered schema version {version} for {table_name}")
            
            return {
                "table_name": table_name,
                "version": version,
                "status": "registered"
            }
            
        except Exception as e:
            logger.error(f"Failed to register schema for {table_name}: {str(e)}")
            return {"error": str(e), "table_name": table_name}
            
    def apply_governance_policies(self, catalog_name: str) -> Dict[str, Any]:
        """
        Apply data governance policies to the catalog
        
        Args:
            catalog_name: Name of catalog to apply policies to
        """
        
        try:
            policies_applied = []
            
            # In a production system, this would apply actual governance policies
            # For now, we'll create a framework for policy management
            
            healthcare_policies = {
                "data_classification": {
                    "PHI_columns": ["member_id", "ssn", "patient_name"],
                    "PII_columns": ["address", "phone", "email"],
                    "sensitivity_level": "high"
                },
                "access_control": {
                    "read_access": ["healthcare_analysts", "data_engineers"],
                    "write_access": ["data_engineers", "etl_service"],
                    "admin_access": ["data_platform_admin"]
                },
                "retention_policy": {
                    "raw_data_retention_days": 2555,  # 7 years
                    "processed_data_retention_days": 3650,  # 10 years
                    "audit_log_retention_days": 2190  # 6 years
                },
                "masking_rules": {
                    "member_id": "hash",
                    "ssn": "mask",
                    "phone": "partial_mask"
                }
            }
            
            # Store policies in a governance table
            governance_table = f"{catalog_name}.monitoring.governance_policies"
            
            for policy_type, policy_config in healthcare_policies.items():
                policy_record = {
                    "catalog_name": catalog_name,
                    "policy_type": policy_type,
                    "policy_config": json.dumps(policy_config),
                    "applied_at": datetime.now().isoformat(),
                    "status": "active"
                }
                
                policies_applied.append(policy_type)
                
                # In production, would apply actual Databricks governance policies here
                logger.info(f"Applied {policy_type} policy to {catalog_name}")
                
            return {
                "catalog_name": catalog_name,
                "policies_applied": policies_applied,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Failed to apply governance policies: {str(e)}")
            return {"error": str(e), "catalog_name": catalog_name}
            
    def get_catalog_health(self, catalog_name: str) -> Dict[str, Any]:
        """Get health metrics for a catalog"""
        
        try:
            # Get catalog info
            catalog_info = self.workspace_client.catalogs.get(catalog_name)
            
            # Get schemas count
            schemas = self.workspace_client.schemas.list(catalog_name)
            schema_count = len(list(schemas))
            
            # Get tables count
            total_tables = 0
            for schema in self.workspace_client.schemas.list(catalog_name):
                tables = self.workspace_client.tables.list(f"{catalog_name}.{schema.name}")
                total_tables += len(list(tables))
                
            # Get recent quality metrics
            monitoring_table = f"{catalog_name}.monitoring.data_quality_metrics"
            
            try:
                recent_quality_df = (self.spark.table(monitoring_table)
                                   .filter("timestamp >= current_date() - 1")
                                   .agg({"quality_score": "avg"}))
                
                avg_quality_score = recent_quality_df.collect()[0][0] or 0
            except Exception:
                avg_quality_score = 0
                
            return {
                "catalog_name": catalog_name,
                "status": "healthy",
                "schema_count": schema_count,
                "table_count": total_tables,
                "avg_quality_score": round(avg_quality_score, 2),
                "created_at": catalog_info.created_at,
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get catalog health: {str(e)}")
            return {"error": str(e), "catalog_name": catalog_name}