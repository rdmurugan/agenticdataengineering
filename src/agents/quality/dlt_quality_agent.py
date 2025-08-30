"""
DLT Quality Agent for healthcare data quality enforcement
Implements data quality rules and expectations for Medicaid/Medicare feeds
"""

import dlt
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from pyspark.sql.types import StructType
from typing import Dict, Any, List, Optional, Callable
import logging
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class DLTQualityAgent:
    """
    Data Quality Agent using Delta Live Tables expectations
    Focuses on healthcare data quality requirements
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.quality_metrics_table = config.get("quality_metrics_table", "monitoring.data_quality_metrics")
        
    def create_bronze_quality_pipeline(self, source_table: str, target_table: str) -> DataFrame:
        """
        Create bronze layer with basic quality checks
        Raw data validation and cleansing
        """
        
        @dlt.table(
            name=target_table,
            comment="Bronze layer with basic quality validations for healthcare data"
        )
        @dlt.expect_all_or_drop({
            "valid_record_format": "record_id IS NOT NULL",
            "valid_timestamp": "_ingestion_timestamp IS NOT NULL",
            "non_empty_payload": "size(payload) > 0"
        })
        def bronze_quality():
            return (dlt.read_stream(source_table)
                   .withColumn("_quality_check_timestamp", col("current_timestamp()"))
                   .withColumn("_data_freshness_hours", 
                             (col("current_timestamp()").cast("long") - col("_ingestion_timestamp").cast("long")) / 3600))
        
        return bronze_quality()
        
    def create_silver_quality_pipeline(self, source_table: str, target_table: str) -> DataFrame:
        """
        Create silver layer with comprehensive healthcare-specific quality checks
        """
        
        @dlt.table(
            name=target_table,
            comment="Silver layer with healthcare-specific data quality validations"
        )
        @dlt.expect_all({
            "valid_member_id": self._validate_member_id_format(),
            "valid_date_of_service": self._validate_service_date(),
            "valid_provider_npi": self._validate_npi_format(),
            "valid_diagnosis_code": self._validate_diagnosis_code(),
            "valid_procedure_code": self._validate_procedure_code(),
            "reasonable_claim_amount": self._validate_claim_amount(),
        })
        @dlt.expect_or_quarantine({
            "member_eligibility_check": self._validate_member_eligibility(),
            "provider_credentialing_check": self._validate_provider_credentials(),
            "duplicate_claim_check": self._validate_duplicate_claims()
        })
        def silver_quality():
            return (dlt.read_stream(source_table)
                   .select("*")
                   .withColumn("_quality_score", self._calculate_quality_score())
                   .withColumn("_anomaly_score", self._calculate_anomaly_score()))
        
        return silver_quality()
        
    def create_gold_quality_pipeline(self, source_table: str, target_table: str) -> DataFrame:
        """
        Create gold layer with business rule validations
        """
        
        @dlt.table(
            name=target_table,
            comment="Gold layer with business rule validations for analytics"
        )
        @dlt.expect_all({
            "high_quality_score": "_quality_score >= 0.8",
            "recent_data": "_data_freshness_hours <= 24",
            "complete_clinical_data": self._validate_clinical_completeness()
        })
        def gold_quality():
            return (dlt.read_stream(source_table)
                   .filter(col("_quality_score") >= 0.7)
                   .withColumn("_business_rule_score", self._calculate_business_rule_score()))
        
        return gold_quality()
        
    def _validate_member_id_format(self) -> str:
        """Validate Medicaid/Medicare member ID format"""
        return """
        CASE 
            WHEN member_id RLIKE '^[0-9]{9,12}$' THEN true
            WHEN member_id RLIKE '^[A-Z]{1,3}[0-9]{6,9}$' THEN true
            ELSE false
        END
        """
        
    def _validate_service_date(self) -> str:
        """Validate service date is reasonable"""
        return """
        date_of_service IS NOT NULL 
        AND date_of_service >= '2020-01-01' 
        AND date_of_service <= current_date() + interval 30 days
        """
        
    def _validate_npi_format(self) -> str:
        """Validate NPI (National Provider Identifier) format"""
        return """
        provider_npi IS NOT NULL 
        AND provider_npi RLIKE '^[0-9]{10}$'
        AND provider_npi NOT IN ('0000000000', '9999999999')
        """
        
    def _validate_diagnosis_code(self) -> str:
        """Validate ICD-10 diagnosis code format"""
        return """
        diagnosis_code IS NOT NULL 
        AND (
            diagnosis_code RLIKE '^[A-TV-Z][0-9][A-Z0-9](\\.?[A-Z0-9]{0,4})?$'
            OR diagnosis_code RLIKE '^[0-9]{3}(\\.?[0-9]{0,2})?$'
        )
        """
        
    def _validate_procedure_code(self) -> str:
        """Validate CPT/HCPCS procedure code format"""
        return """
        procedure_code IS NOT NULL 
        AND (
            procedure_code RLIKE '^[0-9]{5}$'  -- CPT
            OR procedure_code RLIKE '^[A-V][0-9]{4}$'  -- HCPCS Level II
        )
        """
        
    def _validate_claim_amount(self) -> str:
        """Validate claim amount is reasonable"""
        return """
        claim_amount IS NOT NULL 
        AND claim_amount >= 0 
        AND claim_amount <= 1000000
        """
        
    def _validate_member_eligibility(self) -> str:
        """Check member eligibility (simplified)"""
        return """
        member_id IS NOT NULL 
        AND date_of_service BETWEEN eligibility_start_date AND COALESCE(eligibility_end_date, '2099-12-31')
        """
        
    def _validate_provider_credentials(self) -> str:
        """Validate provider credentials (simplified)"""
        return """
        provider_npi IS NOT NULL 
        AND provider_specialty IS NOT NULL
        AND provider_status = 'ACTIVE'
        """
        
    def _validate_duplicate_claims(self) -> str:
        """Check for duplicate claims (simplified)"""
        return """
        NOT EXISTS (
            SELECT 1 FROM claims_silver c2 
            WHERE c2.member_id = claims_silver.member_id 
            AND c2.provider_npi = claims_silver.provider_npi 
            AND c2.date_of_service = claims_silver.date_of_service 
            AND c2.procedure_code = claims_silver.procedure_code
            AND c2._ingestion_timestamp < claims_silver._ingestion_timestamp
        )
        """
        
    def _calculate_quality_score(self) -> str:
        """Calculate overall data quality score"""
        return """
        (
            CASE WHEN member_id IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN provider_npi IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN diagnosis_code IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN procedure_code IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN claim_amount IS NOT NULL AND claim_amount > 0 THEN 0.2 ELSE 0 END
        )
        """
        
    def _calculate_anomaly_score(self) -> str:
        """Calculate anomaly score for outlier detection"""
        return """
        CASE 
            WHEN claim_amount > 50000 THEN 0.9
            WHEN claim_amount > 10000 THEN 0.7
            WHEN claim_amount > 5000 THEN 0.5
            WHEN claim_amount > 1000 THEN 0.3
            ELSE 0.1
        END
        """
        
    def _calculate_business_rule_score(self) -> str:
        """Calculate business rule compliance score"""
        return """
        (
            CASE WHEN _quality_score >= 0.8 THEN 0.4 ELSE 0 END +
            CASE WHEN _data_freshness_hours <= 24 THEN 0.3 ELSE 0 END +
            CASE WHEN _anomaly_score <= 0.5 THEN 0.3 ELSE 0 END
        )
        """
        
    def _validate_clinical_completeness(self) -> str:
        """Validate clinical data completeness"""
        return """
        diagnosis_code IS NOT NULL 
        AND procedure_code IS NOT NULL 
        AND provider_specialty IS NOT NULL
        """
        
    def monitor_quality_metrics(self, table_name: str) -> Dict[str, Any]:
        """
        Monitor data quality metrics for a table
        Returns comprehensive quality metrics
        """
        
        try:
            df = self.spark.table(table_name)
            
            # Calculate basic metrics
            total_records = df.count()
            
            # Quality score distribution
            quality_metrics = df.agg(
                avg("_quality_score").alias("avg_quality_score"),
                spark_min("_quality_score").alias("min_quality_score"),
                spark_max("_quality_score").alias("max_quality_score"),
                count(when(col("_quality_score") >= 0.8, 1)).alias("high_quality_records"),
                count(when(col("_quality_score") < 0.5, 1)).alias("low_quality_records")
            ).collect()[0]
            
            # Anomaly detection
            anomaly_metrics = df.agg(
                count(when(col("_anomaly_score") > 0.7, 1)).alias("high_anomaly_records"),
                avg("_anomaly_score").alias("avg_anomaly_score")
            ).collect()[0]
            
            # Data freshness
            freshness_metrics = df.agg(
                avg("_data_freshness_hours").alias("avg_freshness_hours"),
                spark_max("_data_freshness_hours").alias("max_freshness_hours"),
                count(when(col("_data_freshness_hours") > 24, 1)).alias("stale_records")
            ).collect()[0]
            
            metrics = {
                "table_name": table_name,
                "timestamp": datetime.now().isoformat(),
                "total_records": total_records,
                "quality_metrics": {
                    "avg_quality_score": float(quality_metrics["avg_quality_score"] or 0),
                    "min_quality_score": float(quality_metrics["min_quality_score"] or 0),
                    "max_quality_score": float(quality_metrics["max_quality_score"] or 0),
                    "high_quality_percentage": (quality_metrics["high_quality_records"] / total_records) * 100 if total_records > 0 else 0,
                    "low_quality_percentage": (quality_metrics["low_quality_records"] / total_records) * 100 if total_records > 0 else 0
                },
                "anomaly_metrics": {
                    "high_anomaly_percentage": (anomaly_metrics["high_anomaly_records"] / total_records) * 100 if total_records > 0 else 0,
                    "avg_anomaly_score": float(anomaly_metrics["avg_anomaly_score"] or 0)
                },
                "freshness_metrics": {
                    "avg_freshness_hours": float(freshness_metrics["avg_freshness_hours"] or 0),
                    "max_freshness_hours": float(freshness_metrics["max_freshness_hours"] or 0),
                    "stale_percentage": (freshness_metrics["stale_records"] / total_records) * 100 if total_records > 0 else 0
                }
            }
            
            # Store metrics for dashboard
            self._store_quality_metrics(metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to monitor quality metrics: {str(e)}")
            return {"error": str(e), "table_name": table_name}
            
    def _store_quality_metrics(self, metrics: Dict[str, Any]):
        """Store quality metrics to monitoring table"""
        
        try:
            metrics_df = self.spark.createDataFrame([metrics])
            metrics_df.write.format("delta").mode("append").saveAsTable(self.quality_metrics_table)
            
        except Exception as e:
            logger.error(f"Failed to store quality metrics: {str(e)}")
            
    def create_quality_alerts(self, table_name: str, thresholds: Dict[str, float]) -> List[Dict[str, Any]]:
        """
        Create alerts based on quality thresholds
        
        Args:
            table_name: Table to monitor
            thresholds: Quality thresholds for alerting
        """
        
        alerts = []
        
        try:
            metrics = self.monitor_quality_metrics(table_name)
            
            # Check quality score threshold
            avg_quality = metrics.get("quality_metrics", {}).get("avg_quality_score", 1.0)
            if avg_quality < thresholds.get("min_quality_score", 0.7):
                alerts.append({
                    "alert_type": "low_quality_score",
                    "severity": "high",
                    "message": f"Average quality score {avg_quality:.2f} below threshold {thresholds['min_quality_score']}",
                    "table_name": table_name,
                    "timestamp": datetime.now().isoformat()
                })
                
            # Check anomaly threshold
            anomaly_pct = metrics.get("anomaly_metrics", {}).get("high_anomaly_percentage", 0)
            if anomaly_pct > thresholds.get("max_anomaly_percentage", 5.0):
                alerts.append({
                    "alert_type": "high_anomaly_rate",
                    "severity": "medium",
                    "message": f"High anomaly rate {anomaly_pct:.1f}% above threshold {thresholds['max_anomaly_percentage']}%",
                    "table_name": table_name,
                    "timestamp": datetime.now().isoformat()
                })
                
            # Check freshness threshold
            stale_pct = metrics.get("freshness_metrics", {}).get("stale_percentage", 0)
            if stale_pct > thresholds.get("max_stale_percentage", 10.0):
                alerts.append({
                    "alert_type": "stale_data",
                    "severity": "medium",
                    "message": f"Stale data percentage {stale_pct:.1f}% above threshold {thresholds['max_stale_percentage']}%",
                    "table_name": table_name,
                    "timestamp": datetime.now().isoformat()
                })
                
            # Store alerts
            if alerts:
                self._store_quality_alerts(alerts)
                
            return alerts
            
        except Exception as e:
            logger.error(f"Failed to create quality alerts: {str(e)}")
            return [{"error": str(e), "table_name": table_name}]
            
    def _store_quality_alerts(self, alerts: List[Dict[str, Any]]):
        """Store quality alerts to monitoring table"""
        
        try:
            alerts_table = self.config.get("quality_alerts_table", "monitoring.quality_alerts")
            alerts_df = self.spark.createDataFrame(alerts)
            alerts_df.write.format("delta").mode("append").saveAsTable(alerts_table)
            
        except Exception as e:
            logger.error(f"Failed to store quality alerts: {str(e)}")
            
    def get_quality_dashboard_data(self, table_name: str, days: int = 7) -> Dict[str, Any]:
        """Get quality metrics data for dashboard visualization"""
        
        try:
            # Get historical quality metrics
            historical_df = (self.spark.table(self.quality_metrics_table)
                           .filter(f"table_name = '{table_name}'")
                           .filter(f"timestamp >= current_date() - {days}")
                           .orderBy("timestamp"))
            
            historical_data = [row.asDict() for row in historical_df.collect()]
            
            # Get current alerts
            alerts_table = self.config.get("quality_alerts_table", "monitoring.quality_alerts")
            current_alerts_df = (self.spark.table(alerts_table)
                               .filter(f"table_name = '{table_name}'")
                               .filter("timestamp >= current_date()")
                               .orderBy("timestamp DESC"))
            
            current_alerts = [row.asDict() for row in current_alerts_df.collect()]
            
            return {
                "table_name": table_name,
                "historical_metrics": historical_data,
                "current_alerts": current_alerts,
                "summary": {
                    "total_metrics_points": len(historical_data),
                    "active_alerts": len(current_alerts),
                    "period_days": days
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get dashboard data: {str(e)}")
            return {"error": str(e), "table_name": table_name}