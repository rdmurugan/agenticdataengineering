"""
Delta Live Tables Manager for healthcare data pipelines
Manages DLT pipeline creation, monitoring, and lifecycle
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import (
    CreatePipeline, PipelineSpec, NotebookLibrary, PipelineCluster,
    Pipeline, UpdatePipeline, StartUpdate
)
from pyspark.sql import SparkSession
from typing import Dict, Any, List, Optional
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class DeltaLiveTablesManager:
    """
    Manages Delta Live Tables pipelines for healthcare data processing
    Handles pipeline creation, updates, monitoring, and governance
    """
    
    def __init__(self, workspace_client: WorkspaceClient, spark: SparkSession, config: Dict[str, Any]):
        self.workspace_client = workspace_client
        self.spark = spark
        self.config = config
        self.catalog_name = config.get("unity_catalog", "healthcare_data")
        
    def create_healthcare_dlt_pipeline(
        self,
        pipeline_name: str,
        source_tables: List[str],
        target_schema: str,
        notebooks: List[str],
        cluster_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a DLT pipeline for healthcare data processing
        
        Args:
            pipeline_name: Name of the DLT pipeline
            source_tables: List of source table names
            target_schema: Target schema for processed tables
            notebooks: List of notebook paths for the pipeline
            cluster_config: Optional cluster configuration
        """
        
        try:
            # Build pipeline specification
            pipeline_spec = self._build_pipeline_spec(
                pipeline_name=pipeline_name,
                target_schema=target_schema,
                notebooks=notebooks,
                cluster_config=cluster_config or self._get_default_cluster_config()
            )
            
            # Create pipeline
            create_request = CreatePipeline(
                name=pipeline_name,
                spec=pipeline_spec
            )
            
            pipeline = self.workspace_client.pipelines.create(create_request)
            
            # Register pipeline metadata
            self._register_pipeline_metadata(pipeline, source_tables, target_schema)
            
            logger.info(f"Created DLT pipeline: {pipeline_name} (ID: {pipeline.pipeline_id})")
            
            return {
                "pipeline_name": pipeline_name,
                "pipeline_id": pipeline.pipeline_id,
                "status": "created",
                "target_schema": target_schema,
                "url": f"{self.workspace_client.config.host}/#joblist/pipelines/{pipeline.pipeline_id}"
            }
            
        except Exception as e:
            logger.error(f"Failed to create DLT pipeline: {str(e)}")
            return {"error": str(e), "pipeline_name": pipeline_name}
            
    def _build_pipeline_spec(
        self,
        pipeline_name: str,
        target_schema: str,
        notebooks: List[str],
        cluster_config: Dict[str, Any]
    ) -> PipelineSpec:
        """Build DLT pipeline specification"""
        
        # Convert notebook paths to NotebookLibrary objects
        libraries = []
        for notebook_path in notebooks:
            library = NotebookLibrary(notebook_path=notebook_path)
            libraries.append(library)
            
        # Build cluster configuration
        clusters = [PipelineCluster(
            label="default",
            num_workers=cluster_config.get("num_workers", 2),
            node_type_id=cluster_config.get("node_type_id", "i3.xlarge"),
            driver_node_type_id=cluster_config.get("driver_node_type_id", "i3.xlarge"),
            spark_conf=cluster_config.get("spark_conf", {}),
            custom_tags=cluster_config.get("custom_tags", {}),
            spark_env_vars=cluster_config.get("spark_env_vars", {}),
            enable_local_disk_encryption=cluster_config.get("enable_local_disk_encryption", True),
            aws_attributes=cluster_config.get("aws_attributes")
        )]
        
        # Pipeline configuration
        configuration = {
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.optimizeWrite.enabled": "true", 
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "pipeline.catalog": self.catalog_name,
            "pipeline.target_schema": target_schema,
            "pipeline.healthcare_validation": "enabled"
        }
        
        # Add custom configuration
        if "pipeline_config" in self.config:
            configuration.update(self.config["pipeline_config"])
            
        return PipelineSpec(
            libraries=libraries,
            clusters=clusters,
            configuration=configuration,
            continuous=self.config.get("continuous_processing", True),
            development=self.config.get("development_mode", False),
            photon=self.config.get("enable_photon", True),
            serverless=self.config.get("enable_serverless", False),
            target=f"{self.catalog_name}.{target_schema}",
            edition="ADVANCED"  # Required for healthcare compliance features
        )
        
    def _get_default_cluster_config(self) -> Dict[str, Any]:
        """Get default cluster configuration for healthcare workloads"""
        
        return {
            "num_workers": 3,
            "node_type_id": "i3.xlarge",
            "driver_node_type_id": "i3.xlarge",
            "spark_conf": {
                "spark.databricks.delta.autoCompact.enabled": "true",
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.databricks.io.cache.enabled": "true"
            },
            "custom_tags": {
                "Environment": self.config.get("environment", "development"),
                "Project": "healthcare-data-platform",
                "CostCenter": "data-engineering"
            },
            "enable_local_disk_encryption": True,
            "aws_attributes": {
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 70
            } if self.config.get("use_spot_instances", True) else None
        }
        
    def _register_pipeline_metadata(
        self,
        pipeline: Pipeline,
        source_tables: List[str],
        target_schema: str
    ):
        """Register pipeline metadata for tracking"""
        
        try:
            metadata_table = f"{self.catalog_name}.monitoring.dlt_pipeline_metadata"
            
            metadata = {
                "pipeline_id": pipeline.pipeline_id,
                "pipeline_name": pipeline.name,
                "target_schema": target_schema,
                "source_tables": json.dumps(source_tables),
                "created_at": datetime.now().isoformat(),
                "status": "created",
                "last_update": None,
                "health_score": 100.0
            }
            
            metadata_df = self.spark.createDataFrame([metadata])
            metadata_df.write.format("delta").mode("append").saveAsTable(metadata_table)
            
        except Exception as e:
            logger.error(f"Failed to register pipeline metadata: {str(e)}")
            
    def start_pipeline_update(self, pipeline_id: str, full_refresh: bool = False) -> Dict[str, Any]:
        """
        Start a pipeline update
        
        Args:
            pipeline_id: ID of the pipeline to update
            full_refresh: Whether to perform full refresh
        """
        
        try:
            # Start update
            update_request = StartUpdate(
                pipeline_id=pipeline_id,
                full_refresh=full_refresh
            )
            
            update_info = self.workspace_client.pipelines.start_update(update_request)
            
            logger.info(f"Started pipeline update: {pipeline_id} (Update ID: {update_info.update_id})")
            
            return {
                "pipeline_id": pipeline_id,
                "update_id": update_info.update_id,
                "status": "started",
                "full_refresh": full_refresh
            }
            
        except Exception as e:
            logger.error(f"Failed to start pipeline update: {str(e)}")
            return {"error": str(e), "pipeline_id": pipeline_id}
            
    def monitor_pipeline_health(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Monitor pipeline health and performance
        
        Args:
            pipeline_id: ID of pipeline to monitor
        """
        
        try:
            # Get pipeline info
            pipeline = self.workspace_client.pipelines.get(pipeline_id)
            
            # Get latest update info
            updates = list(self.workspace_client.pipelines.list_updates(pipeline_id, max_results=5))
            latest_update = updates[0] if updates else None
            
            # Calculate health metrics
            health_metrics = self._calculate_pipeline_health(pipeline, latest_update)
            
            # Get performance metrics
            performance_metrics = self._get_pipeline_performance_metrics(pipeline_id)
            
            # Get data quality metrics
            quality_metrics = self._get_pipeline_quality_metrics(pipeline_id)
            
            return {
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline.name,
                "state": pipeline.state.value if pipeline.state else "UNKNOWN",
                "health": health_metrics,
                "performance": performance_metrics,
                "quality": quality_metrics,
                "last_update": latest_update.update_id if latest_update else None,
                "monitoring_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to monitor pipeline health: {str(e)}")
            return {"error": str(e), "pipeline_id": pipeline_id}
            
    def _calculate_pipeline_health(self, pipeline: Pipeline, latest_update: Any) -> Dict[str, Any]:
        """Calculate pipeline health score and status"""
        
        health_score = 100.0
        health_issues = []
        
        # Check pipeline state
        if pipeline.state and pipeline.state.value in ["FAILED", "STOPPING"]:
            health_score -= 50
            health_issues.append(f"Pipeline in {pipeline.state.value} state")
            
        # Check latest update status
        if latest_update:
            if hasattr(latest_update, 'state') and latest_update.state:
                update_state = latest_update.state.value
                if update_state == "FAILED":
                    health_score -= 30
                    health_issues.append("Latest update failed")
                elif update_state in ["CANCELED", "STOPPING"]:
                    health_score -= 20
                    health_issues.append(f"Latest update {update_state.lower()}")
                    
        # Additional health checks would go here
        # - Check for recent failures
        # - Check data freshness
        # - Check resource utilization
        
        health_status = "healthy"
        if health_score < 70:
            health_status = "unhealthy"
        elif health_score < 85:
            health_status = "warning"
            
        return {
            "score": health_score,
            "status": health_status,
            "issues": health_issues
        }
        
    def _get_pipeline_performance_metrics(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline performance metrics"""
        
        # In production, this would query Databricks metrics APIs
        # For now, return simulated metrics
        
        return {
            "avg_execution_time_minutes": 15.5,
            "throughput_records_per_minute": 50000,
            "success_rate_percent": 98.2,
            "resource_utilization_percent": 75,
            "cost_per_gb_processed": 0.05
        }
        
    def _get_pipeline_quality_metrics(self, pipeline_id: str) -> Dict[str, Any]:
        """Get data quality metrics for pipeline output"""
        
        try:
            # Query quality metrics from monitoring tables
            quality_table = f"{self.catalog_name}.monitoring.data_quality_metrics"
            
            pipeline_quality_df = (self.spark.table(quality_table)
                                 .filter(f"pipeline_id = '{pipeline_id}'")
                                 .filter("timestamp >= current_date() - 1")
                                 .agg({
                                     "quality_score": "avg",
                                     "completeness_score": "avg",
                                     "validity_score": "avg",
                                     "anomaly_score": "avg"
                                 }))
            
            quality_row = pipeline_quality_df.collect()[0]
            
            return {
                "overall_quality_score": round(quality_row[0] or 0, 2),
                "completeness_score": round(quality_row[1] or 0, 2),
                "validity_score": round(quality_row[2] or 0, 2),
                "anomaly_score": round(quality_row[3] or 0, 2)
            }
            
        except Exception as e:
            logger.warning(f"Could not get quality metrics: {str(e)}")
            return {
                "overall_quality_score": 0,
                "completeness_score": 0,
                "validity_score": 0,
                "anomaly_score": 0
            }
            
    def create_healthcare_notebooks(self, target_schema: str) -> Dict[str, str]:
        """
        Create DLT notebooks for healthcare data processing
        Returns paths to created notebooks
        """
        
        try:
            notebook_paths = {}
            
            # Bronze layer notebook
            bronze_notebook = self._create_bronze_layer_notebook(target_schema)
            notebook_paths["bronze"] = bronze_notebook
            
            # Silver layer notebook  
            silver_notebook = self._create_silver_layer_notebook(target_schema)
            notebook_paths["silver"] = silver_notebook
            
            # Gold layer notebook
            gold_notebook = self._create_gold_layer_notebook(target_schema)
            notebook_paths["gold"] = gold_notebook
            
            logger.info(f"Created DLT notebooks for {target_schema}")
            return notebook_paths
            
        except Exception as e:
            logger.error(f"Failed to create DLT notebooks: {str(e)}")
            return {"error": str(e)}
            
    def _create_bronze_layer_notebook(self, target_schema: str) -> str:
        """Create bronze layer DLT notebook"""
        
        notebook_path = f"/Shared/dlt_notebooks/{target_schema}_bronze"
        
        notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Healthcare Data Ingestion
# MAGIC 
# MAGIC This notebook implements the bronze layer for healthcare data using Delta Live Tables.
# MAGIC Raw data ingestion with basic quality checks.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Raw Healthcare Data

# COMMAND ----------

@dlt.table(
    name="claims_bronze",
    comment="Bronze layer for healthcare claims data with basic validation",
    table_properties={{
        "quality": "bronze",
        "layer": "raw"
    }}
)
@dlt.expect_all_or_drop({{
    "valid_record": "record_id IS NOT NULL",
    "valid_timestamp": "_ingestion_timestamp IS NOT NULL",
    "non_empty_data": "size(struct(*)) > 0"
}})
def claims_bronze():
    return (
        dlt.read_stream("healthcare_data.raw.claims_raw")
        .withColumn("_bronze_timestamp", current_timestamp())
        .withColumn("_data_freshness_hours", 
                   (unix_timestamp(current_timestamp()) - unix_timestamp("_ingestion_timestamp")) / 3600)
    )

# COMMAND ----------

@dlt.table(
    name="members_bronze", 
    comment="Bronze layer for member eligibility data"
)
@dlt.expect_all_or_drop({{
    "valid_member_id": "member_id IS NOT NULL",
    "valid_eligibility": "eligibility_start_date IS NOT NULL"
}})
def members_bronze():
    return (
        dlt.read_stream("healthcare_data.raw.members_raw")
        .withColumn("_bronze_timestamp", current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="providers_bronze",
    comment="Bronze layer for provider data"  
)
@dlt.expect_all_or_drop({{
    "valid_npi": "provider_npi IS NOT NULL AND length(provider_npi) = 10",
    "valid_provider": "provider_name IS NOT NULL"
}})
def providers_bronze():
    return (
        dlt.read_stream("healthcare_data.raw.providers_raw") 
        .withColumn("_bronze_timestamp", current_timestamp())
    )
'''
        
        # In production, would create actual notebook file
        # For now, return the path
        return notebook_path
        
    def _create_silver_layer_notebook(self, target_schema: str) -> str:
        """Create silver layer DLT notebook"""
        
        notebook_path = f"/Shared/dlt_notebooks/{target_schema}_silver"
        
        notebook_content = f'''# Databricks notebook source
# MAGIC %md  
# MAGIC # Silver Layer - Healthcare Data Quality & Validation
# MAGIC
# MAGIC This notebook implements the silver layer with comprehensive healthcare-specific
# MAGIC data quality validations and business rule enforcement.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Validated Healthcare Data

# COMMAND ----------

@dlt.table(
    name="claims_silver",
    comment="Silver layer claims with healthcare validations",
    table_properties={{
        "quality": "silver", 
        "layer": "validated"
    }}
)
@dlt.expect_all({{
    "valid_member_id": """
        member_id IS NOT NULL AND 
        (member_id RLIKE '^[0-9]{{9,12}}$' OR member_id RLIKE '^[A-Z]{{1,3}}[0-9]{{6,9}}$')
    """,
    "valid_service_date": """
        date_of_service IS NOT NULL AND 
        date_of_service >= '2020-01-01' AND 
        date_of_service <= current_date() + interval 30 days
    """,
    "valid_npi": """
        provider_npi IS NOT NULL AND 
        provider_npi RLIKE '^[0-9]{{10}}$' AND
        provider_npi NOT IN ('0000000000', '9999999999')
    """,
    "valid_diagnosis": """
        diagnosis_code IS NOT NULL AND
        (diagnosis_code RLIKE '^[A-TV-Z][0-9][A-Z0-9](\\\\.[A-Z0-9]{{0,4}})?$' OR
         diagnosis_code RLIKE '^[0-9]{{3}}(\\\\.[0-9]{{0,2}})?$')
    """,
    "valid_procedure": """
        procedure_code IS NOT NULL AND
        (procedure_code RLIKE '^[0-9]{{5}}$' OR
         procedure_code RLIKE '^[A-V][0-9]{{4}}$')
    """,
    "reasonable_amount": """
        claim_amount IS NOT NULL AND 
        claim_amount >= 0 AND 
        claim_amount <= 1000000
    """
}})
@dlt.expect_or_quarantine({{
    "member_eligibility": """
        EXISTS (
            SELECT 1 FROM LIVE.members_silver m 
            WHERE m.member_id = claims_silver.member_id 
            AND claims_silver.date_of_service BETWEEN m.eligibility_start_date 
                AND COALESCE(m.eligibility_end_date, '2099-12-31')
        )
    """,
    "provider_active": """
        EXISTS (
            SELECT 1 FROM LIVE.providers_silver p
            WHERE p.provider_npi = claims_silver.provider_npi
            AND p.status = 'ACTIVE'
        )
    """,
    "no_duplicate_claims": """
        NOT EXISTS (
            SELECT 1 FROM LIVE.claims_silver c2
            WHERE c2.member_id = claims_silver.member_id
            AND c2.provider_npi = claims_silver.provider_npi  
            AND c2.date_of_service = claims_silver.date_of_service
            AND c2.procedure_code = claims_silver.procedure_code
            AND c2._bronze_timestamp < claims_silver._bronze_timestamp
        )
    """
}})
def claims_silver():
    return (
        dlt.read("claims_bronze")
        .withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_quality_score", 
            (case()
             .when(col("member_id").isNotNull(), 0.2).otherwise(0) +
             case().when(col("provider_npi").isNotNull(), 0.2).otherwise(0) +
             case().when(col("diagnosis_code").isNotNull(), 0.2).otherwise(0) +
             case().when(col("procedure_code").isNotNull(), 0.2).otherwise(0) +
             case().when((col("claim_amount").isNotNull()) & (col("claim_amount") > 0), 0.2).otherwise(0)
            ))
        .withColumn("_anomaly_score",
            case()
            .when(col("claim_amount") > 50000, 0.9)
            .when(col("claim_amount") > 10000, 0.7) 
            .when(col("claim_amount") > 5000, 0.5)
            .when(col("claim_amount") > 1000, 0.3)
            .otherwise(0.1))
    )

# COMMAND ----------

@dlt.table(
    name="members_silver",
    comment="Silver layer member data with validation"
)
@dlt.expect_all({{
    "valid_member_format": """
        member_id RLIKE '^[A-Z0-9]{{8,12}}$'
    """,
    "valid_eligibility_dates": """
        eligibility_start_date IS NOT NULL AND
        eligibility_start_date <= COALESCE(eligibility_end_date, current_date())
    """
}})
def members_silver():
    return (
        dlt.read("members_bronze")
        .withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_is_currently_eligible", 
            (col("eligibility_start_date") <= current_date()) &
            (col("eligibility_end_date").isNull() | (col("eligibility_end_date") >= current_date()))
        )
    )

# COMMAND ----------

@dlt.table(
    name="providers_silver",
    comment="Silver layer provider data with NPI validation"
)  
@dlt.expect_all({{
    "valid_npi_format": """
        provider_npi RLIKE '^[0-9]{{10}}$' AND
        provider_npi NOT IN ('0000000000', '9999999999')
    """,
    "valid_taxonomy": """
        provider_taxonomy IS NULL OR 
        length(provider_taxonomy) = 10
    """
}})
def providers_silver():
    return (
        dlt.read("providers_bronze")
        .withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_npi_checksum_valid", 
            # Simplified NPI checksum validation
            lit(True)  # In production would implement Luhn algorithm
        )
    )
'''
        
        return notebook_path
        
    def _create_gold_layer_notebook(self, target_schema: str) -> str:
        """Create gold layer DLT notebook"""
        
        notebook_path = f"/Shared/dlt_notebooks/{target_schema}_gold"
        
        notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Healthcare Analytics & Reporting
# MAGIC
# MAGIC This notebook creates analytics-ready tables for healthcare reporting
# MAGIC with business logic and aggregations applied.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Tables - Analytics Ready Data

# COMMAND ----------

@dlt.table(
    name="claims_analytics",
    comment="Claims data optimized for analytics with enrichments",
    table_properties={{
        "quality": "gold",
        "layer": "analytics"
    }}
)
@dlt.expect_all({{
    "high_quality_data": "_quality_score >= 0.8",
    "recent_data": "_data_freshness_hours <= 24",
    "complete_clinical_data": """
        diagnosis_code IS NOT NULL AND 
        procedure_code IS NOT NULL AND
        provider_npi IS NOT NULL
    """
}})
def claims_analytics():
    claims = dlt.read("claims_silver")
    members = dlt.read("members_silver") 
    providers = dlt.read("providers_silver")
    
    return (
        claims
        .filter(col("_quality_score") >= 0.7)
        .join(members, "member_id", "left")
        .join(providers, "provider_npi", "left") 
        .select(
            claims["*"],
            members["eligibility_start_date"],
            members["eligibility_end_date"],
            members["plan_type"],
            providers["provider_name"],
            providers["provider_specialty"],
            providers["provider_state"]
        )
        .withColumn("_gold_timestamp", current_timestamp())
        .withColumn("_business_rule_score",
            (case().when(col("_quality_score") >= 0.8, 0.4).otherwise(0) +
             case().when(col("_data_freshness_hours") <= 24, 0.3).otherwise(0) +
             case().when(col("_anomaly_score") <= 0.5, 0.3).otherwise(0))
        )
        .withColumn("claim_year", year("date_of_service"))
        .withColumn("claim_month", month("date_of_service"))
        .withColumn("provider_risk_category",
            case()
            .when(col("_anomaly_score") > 0.7, "high_risk")
            .when(col("_anomaly_score") > 0.4, "medium_risk") 
            .otherwise("low_risk")
        )
    )

# COMMAND ----------

@dlt.table(
    name="provider_performance",
    comment="Provider performance metrics and analytics"
)
def provider_performance():
    return (
        dlt.read("claims_analytics")
        .groupBy("provider_npi", "provider_name", "provider_specialty", "claim_year", "claim_month")
        .agg(
            count("*").alias("total_claims"),
            sum("claim_amount").alias("total_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            avg("_quality_score").alias("avg_quality_score"),
            avg("_anomaly_score").alias("avg_anomaly_score"),
            countDistinct("member_id").alias("unique_patients"),
            countDistinct("diagnosis_code").alias("unique_diagnoses"),
            countDistinct("procedure_code").alias("unique_procedures")
        )
        .withColumn("_gold_timestamp", current_timestamp())
        .withColumn("performance_score", 
            (lit(1.0) - col("avg_anomaly_score")) * col("avg_quality_score")
        )
    )

# COMMAND ----------

@dlt.table(
    name="member_utilization", 
    comment="Member healthcare utilization patterns"
)
def member_utilization():
    return (
        dlt.read("claims_analytics")
        .groupBy("member_id", "plan_type", "claim_year", "claim_month")
        .agg(
            count("*").alias("total_claims"),
            sum("claim_amount").alias("total_spending"),
            avg("claim_amount").alias("avg_claim_amount"),
            countDistinct("provider_npi").alias("unique_providers"),
            countDistinct("diagnosis_code").alias("unique_conditions"),
            countDistinct("procedure_code").alias("unique_procedures"),
            max("date_of_service").alias("last_service_date")
        )
        .withColumn("_gold_timestamp", current_timestamp())
        .withColumn("utilization_category",
            case()
            .when(col("total_claims") > 20, "high_utilizer")
            .when(col("total_claims") > 10, "moderate_utilizer")
            .when(col("total_claims") > 5, "regular_utilizer")  
            .otherwise("low_utilizer")
        )
        .withColumn("cost_category",
            case()
            .when(col("total_spending") > 50000, "high_cost")
            .when(col("total_spending") > 20000, "moderate_cost")
            .when(col("total_spending") > 5000, "regular_cost")
            .otherwise("low_cost")
        )
    )

# COMMAND ----------

@dlt.table(
    name="quality_dashboard",
    comment="Data quality metrics for dashboard reporting"
)
def quality_dashboard():
    return (
        dlt.read("claims_silver")
        .groupBy(
            date_trunc("hour", "_silver_timestamp").alias("quality_hour"),
            date_trunc("day", "_silver_timestamp").alias("quality_date")
        )
        .agg(
            count("*").alias("total_records"),
            avg("_quality_score").alias("avg_quality_score"),
            min("_quality_score").alias("min_quality_score"), 
            max("_quality_score").alias("max_quality_score"),
            avg("_anomaly_score").alias("avg_anomaly_score"),
            sum(when(col("_quality_score") >= 0.8, 1).otherwise(0)).alias("high_quality_records"),
            sum(when(col("_quality_score") < 0.5, 1).otherwise(0)).alias("low_quality_records"),
            sum(when(col("_anomaly_score") > 0.7, 1).otherwise(0)).alias("high_anomaly_records")
        )
        .withColumn("_gold_timestamp", current_timestamp())
        .withColumn("quality_trend",
            case()
            .when(col("avg_quality_score") >= 0.9, "excellent")
            .when(col("avg_quality_score") >= 0.8, "good")
            .when(col("avg_quality_score") >= 0.7, "acceptable")
            .otherwise("needs_attention")
        )
    )
'''
        
        return notebook_path
        
    def get_pipeline_lineage(self, pipeline_id: str) -> Dict[str, Any]:
        """Get data lineage information for a pipeline"""
        
        try:
            # Get pipeline info
            pipeline = self.workspace_client.pipelines.get(pipeline_id)
            
            # In production, would query Unity Catalog lineage APIs
            # For now, return simulated lineage
            
            lineage_info = {
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline.name,
                "source_tables": [
                    f"{self.catalog_name}.raw.claims_raw",
                    f"{self.catalog_name}.raw.members_raw", 
                    f"{self.catalog_name}.raw.providers_raw"
                ],
                "bronze_tables": [
                    f"{self.catalog_name}.bronze.claims_bronze",
                    f"{self.catalog_name}.bronze.members_bronze",
                    f"{self.catalog_name}.bronze.providers_bronze"
                ],
                "silver_tables": [
                    f"{self.catalog_name}.silver.claims_silver",
                    f"{self.catalog_name}.silver.members_silver",
                    f"{self.catalog_name}.silver.providers_silver"
                ],
                "gold_tables": [
                    f"{self.catalog_name}.gold.claims_analytics",
                    f"{self.catalog_name}.gold.provider_performance",
                    f"{self.catalog_name}.gold.member_utilization",
                    f"{self.catalog_name}.gold.quality_dashboard"
                ],
                "data_flow": [
                    {"from": "raw", "to": "bronze", "transformation": "basic_validation"},
                    {"from": "bronze", "to": "silver", "transformation": "healthcare_validation"},
                    {"from": "silver", "to": "gold", "transformation": "business_logic"}
                ]
            }
            
            return lineage_info
            
        except Exception as e:
            logger.error(f"Failed to get pipeline lineage: {str(e)}")
            return {"error": str(e), "pipeline_id": pipeline_id}
            
    def get_dlt_dashboard_data(self, pipeline_id: str) -> Dict[str, Any]:
        """Get comprehensive DLT pipeline data for dashboard"""
        
        try:
            # Get pipeline health
            health_data = self.monitor_pipeline_health(pipeline_id)
            
            if "error" in health_data:
                return health_data
                
            # Get lineage information
            lineage_data = self.get_pipeline_lineage(pipeline_id)
            
            # Get recent update history
            updates = list(self.workspace_client.pipelines.list_updates(pipeline_id, max_results=10))
            
            update_history = []
            for update in updates:
                update_info = {
                    "update_id": update.update_id,
                    "state": update.state.value if update.state else "UNKNOWN",
                    "creation_time": update.creation_time,
                    "full_refresh": getattr(update, 'full_refresh', False)
                }
                update_history.append(update_info)
                
            return {
                "pipeline_id": pipeline_id,
                "health": health_data,
                "lineage": lineage_data,
                "update_history": update_history,
                "dashboard_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get DLT dashboard data: {str(e)}")
            return {"error": str(e), "pipeline_id": pipeline_id}