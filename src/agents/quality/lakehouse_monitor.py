"""
Lakehouse Monitor for comprehensive data quality and drift monitoring
Integrates with Databricks Lakehouse Monitoring for advanced analytics
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInfo, MonitorRefreshInfo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, count
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class LakehouseMonitor:
    """
    Advanced monitoring using Databricks Lakehouse Monitoring
    Provides statistical analysis, data drift detection, and model monitoring
    """
    
    def __init__(self, workspace_client: WorkspaceClient, spark: SparkSession, config: Dict[str, Any]):
        self.workspace_client = workspace_client
        self.spark = spark
        self.config = config
        
    def create_data_monitor(
        self,
        table_name: str,
        monitor_name: str,
        baseline_table: Optional[str] = None,
        granularities: List[str] = ["1 day", "1 week"],
        data_classification_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a comprehensive data monitor for healthcare tables
        
        Args:
            table_name: Unity Catalog table to monitor
            monitor_name: Name for the monitor
            baseline_table: Optional baseline table for comparison
            granularities: Time granularities for monitoring
            data_classification_config: PII/PHI classification config
        """
        
        try:
            # Define healthcare-specific monitoring configuration
            monitor_config = self._build_healthcare_monitor_config(
                table_name, 
                baseline_table, 
                granularities,
                data_classification_config
            )
            
            # Create the monitor using Databricks SDK
            monitor_info = self.workspace_client.quality_monitors.create(
                table_name=table_name,
                assets_dir=f"/Shared/lakehouse_monitors/{monitor_name}",
                data_classification_config=monitor_config.get("data_classification_config"),
                inference_log=monitor_config.get("inference_log"),
                notifications=monitor_config.get("notifications"),
                schedule=monitor_config.get("schedule"),
                snapshot=monitor_config.get("snapshot"),
                time_series=monitor_config.get("time_series"),
                custom_metrics=monitor_config.get("custom_metrics")
            )
            
            logger.info(f"Created lakehouse monitor: {monitor_name} for table {table_name}")
            
            # Store monitor metadata
            self._store_monitor_metadata(monitor_info, monitor_config)
            
            return {
                "monitor_name": monitor_name,
                "table_name": table_name,
                "status": "created",
                "monitor_id": monitor_info.monitor_name,
                "config": monitor_config
            }
            
        except Exception as e:
            logger.error(f"Failed to create lakehouse monitor: {str(e)}")
            return {"error": str(e), "monitor_name": monitor_name, "table_name": table_name}
            
    def _build_healthcare_monitor_config(
        self,
        table_name: str,
        baseline_table: Optional[str],
        granularities: List[str],
        data_classification_config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Build healthcare-specific monitoring configuration"""
        
        config = {
            "data_classification_config": data_classification_config or self._get_default_healthcare_classification(),
            "notifications": {
                "on_failure": {
                    "email_addresses": self.config.get("alert_emails", [])
                },
                "on_new_classification_tag_detected": {
                    "email_addresses": self.config.get("alert_emails", [])
                }
            },
            "schedule": {
                "quartz_cron_expression": "0 0 */6 * * ?",  # Every 6 hours
                "timezone_id": "UTC"
            },
            "snapshot": {
                "granularities": granularities
            }
        }
        
        # Add baseline comparison if provided
        if baseline_table:
            config["baseline_table_name"] = baseline_table
            
        # Add time series configuration for trend analysis
        config["time_series"] = {
            "timestamp_col": "_ingestion_timestamp",
            "granularities": granularities
        }
        
        # Add custom metrics for healthcare data
        config["custom_metrics"] = self._build_healthcare_custom_metrics()
        
        return config
        
    def _get_default_healthcare_classification(self) -> Dict[str, Any]:
        """Default data classification for healthcare data"""
        
        return {
            "enabled": True,
            "run_classifier": True,
            "classification_tags": [
                {
                    "name": "PHI",
                    "description": "Protected Health Information"
                },
                {
                    "name": "PII", 
                    "description": "Personally Identifiable Information"
                },
                {
                    "name": "FINANCIAL",
                    "description": "Financial Information"
                },
                {
                    "name": "CLINICAL",
                    "description": "Clinical Data"
                }
            ]
        }
        
    def _build_healthcare_custom_metrics(self) -> List[Dict[str, Any]]:
        """Build custom metrics specific to healthcare data"""
        
        return [
            {
                "name": "member_id_completeness",
                "type": "aggregate",
                "definition": "COUNT(member_id) / COUNT(*)",
                "output_data_type": "double"
            },
            {
                "name": "claim_amount_outliers",
                "type": "aggregate", 
                "definition": "COUNT(CASE WHEN claim_amount > 50000 THEN 1 END) / COUNT(*)",
                "output_data_type": "double"
            },
            {
                "name": "diagnosis_code_validity",
                "type": "aggregate",
                "definition": "COUNT(CASE WHEN diagnosis_code RLIKE '^[A-TV-Z][0-9][A-Z0-9]' THEN 1 END) / COUNT(*)",
                "output_data_type": "double"
            },
            {
                "name": "npi_format_compliance",
                "type": "aggregate",
                "definition": "COUNT(CASE WHEN provider_npi RLIKE '^[0-9]{10}$' THEN 1 END) / COUNT(*)",
                "output_data_type": "double"
            },
            {
                "name": "data_freshness_hours",
                "type": "aggregate",
                "definition": "AVG((unix_timestamp(current_timestamp()) - unix_timestamp(_ingestion_timestamp)) / 3600)",
                "output_data_type": "double"
            }
        ]
        
    def run_monitor_refresh(self, monitor_name: str, full_refresh: bool = False) -> Dict[str, Any]:
        """
        Trigger a monitor refresh
        
        Args:
            monitor_name: Name of the monitor to refresh
            full_refresh: Whether to perform full refresh
        """
        
        try:
            refresh_info = self.workspace_client.quality_monitors.run_refresh(
                table_name=monitor_name,
                run_immediately=True,
                full_refresh=full_refresh
            )
            
            logger.info(f"Started monitor refresh: {monitor_name}")
            
            return {
                "monitor_name": monitor_name,
                "refresh_id": refresh_info.refresh_id,
                "status": "started",
                "full_refresh": full_refresh
            }
            
        except Exception as e:
            logger.error(f"Failed to refresh monitor: {str(e)}")
            return {"error": str(e), "monitor_name": monitor_name}
            
    def get_monitor_metrics(self, monitor_name: str, start_time: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metrics from a lakehouse monitor
        
        Args:
            monitor_name: Name of the monitor
            start_time: Start time for metrics (ISO format)
        """
        
        try:
            # Get monitor info
            monitor_info = self.workspace_client.quality_monitors.get(table_name=monitor_name)
            
            # Get metrics table
            metrics_table = f"{monitor_info.drift_metrics_table_name}"
            
            # Query metrics
            metrics_query = f"""
            SELECT 
                window.start as window_start,
                window.end as window_end,
                column_name,
                data_type,
                null_count,
                null_percentage,
                distinct_count,
                mean,
                stddev,
                min,
                max,
                drift_score,
                js_divergence
            FROM {metrics_table}
            """
            
            if start_time:
                metrics_query += f" WHERE window.start >= '{start_time}'"
                
            metrics_query += " ORDER BY window.start DESC, column_name"
            
            metrics_df = self.spark.sql(metrics_query)
            metrics_data = [row.asDict() for row in metrics_df.collect()]
            
            # Get profile metrics
            profile_table = f"{monitor_info.profile_metrics_table_name}"
            profile_query = f"""
            SELECT 
                granularity,
                window.start as window_start,
                window.end as window_end,
                num_records,
                data_size_bytes
            FROM {profile_table}
            """
            
            if start_time:
                profile_query += f" WHERE window.start >= '{start_time}'"
                
            profile_query += " ORDER BY window.start DESC"
            
            profile_df = self.spark.sql(profile_query)
            profile_data = [row.asDict() for row in profile_df.collect()]
            
            return {
                "monitor_name": monitor_name,
                "metrics": metrics_data,
                "profile": profile_data,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Failed to get monitor metrics: {str(e)}")
            return {"error": str(e), "monitor_name": monitor_name}
            
    def detect_data_drift(self, monitor_name: str, drift_threshold: float = 0.1) -> Dict[str, Any]:
        """
        Detect data drift using monitor results
        
        Args:
            monitor_name: Monitor to analyze
            drift_threshold: Threshold for drift detection
        """
        
        try:
            monitor_metrics = self.get_monitor_metrics(monitor_name)
            
            if "error" in monitor_metrics:
                return monitor_metrics
                
            drift_alerts = []
            metrics = monitor_metrics.get("metrics", [])
            
            for metric in metrics:
                drift_score = metric.get("drift_score")
                js_divergence = metric.get("js_divergence")
                column_name = metric.get("column_name")
                
                if drift_score and drift_score > drift_threshold:
                    drift_alerts.append({
                        "column_name": column_name,
                        "drift_score": drift_score,
                        "js_divergence": js_divergence,
                        "severity": self._calculate_drift_severity(drift_score),
                        "window_start": metric.get("window_start"),
                        "window_end": metric.get("window_end")
                    })
                    
            # Store drift alerts
            if drift_alerts:
                self._store_drift_alerts(monitor_name, drift_alerts)
                
            return {
                "monitor_name": monitor_name,
                "drift_detected": len(drift_alerts) > 0,
                "total_drift_columns": len(drift_alerts),
                "drift_alerts": drift_alerts,
                "threshold": drift_threshold
            }
            
        except Exception as e:
            logger.error(f"Failed to detect data drift: {str(e)}")
            return {"error": str(e), "monitor_name": monitor_name}
            
    def _calculate_drift_severity(self, drift_score: float) -> str:
        """Calculate drift severity based on score"""
        
        if drift_score > 0.5:
            return "high"
        elif drift_score > 0.3:
            return "medium"
        else:
            return "low"
            
    def _store_drift_alerts(self, monitor_name: str, drift_alerts: List[Dict[str, Any]]):
        """Store drift alerts for monitoring"""
        
        try:
            alerts_table = self.config.get("drift_alerts_table", "monitoring.drift_alerts")
            
            for alert in drift_alerts:
                alert["monitor_name"] = monitor_name
                alert["timestamp"] = datetime.now().isoformat()
                
            alerts_df = self.spark.createDataFrame(drift_alerts)
            alerts_df.write.format("delta").mode("append").saveAsTable(alerts_table)
            
        except Exception as e:
            logger.error(f"Failed to store drift alerts: {str(e)}")
            
    def _store_monitor_metadata(self, monitor_info: MonitorInfo, config: Dict[str, Any]):
        """Store monitor metadata for tracking"""
        
        try:
            metadata_table = self.config.get("monitor_metadata_table", "monitoring.monitor_metadata")
            
            metadata = {
                "monitor_name": monitor_info.monitor_name,
                "table_name": monitor_info.table_name,
                "status": monitor_info.status,
                "assets_dir": monitor_info.assets_dir,
                "config": json.dumps(config),
                "created_at": datetime.now().isoformat()
            }
            
            metadata_df = self.spark.createDataFrame([metadata])
            metadata_df.write.format("delta").mode("append").saveAsTable(metadata_table)
            
        except Exception as e:
            logger.error(f"Failed to store monitor metadata: {str(e)}")
            
    def get_monitor_dashboard_data(self, monitor_name: str, days: int = 30) -> Dict[str, Any]:
        """Get comprehensive monitoring data for dashboard"""
        
        try:
            # Get current monitor metrics
            start_time = (datetime.now() - timedelta(days=days)).isoformat()
            metrics_data = self.get_monitor_metrics(monitor_name, start_time)
            
            # Get drift alerts
            drift_data = self.detect_data_drift(monitor_name)
            
            # Get recent drift alerts from storage
            drift_alerts_table = self.config.get("drift_alerts_table", "monitoring.drift_alerts")
            recent_alerts_df = (self.spark.table(drift_alerts_table)
                              .filter(f"monitor_name = '{monitor_name}'")
                              .filter(f"timestamp >= current_date() - {days}")
                              .orderBy("timestamp DESC"))
            
            recent_alerts = [row.asDict() for row in recent_alerts_df.collect()]
            
            # Calculate summary statistics
            summary_stats = self._calculate_monitor_summary(metrics_data.get("metrics", []))
            
            return {
                "monitor_name": monitor_name,
                "period_days": days,
                "metrics": metrics_data.get("metrics", []),
                "profile": metrics_data.get("profile", []),
                "current_drift": drift_data,
                "recent_alerts": recent_alerts,
                "summary": summary_stats,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Failed to get monitor dashboard data: {str(e)}")
            return {"error": str(e), "monitor_name": monitor_name}
            
    def _calculate_monitor_summary(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics from metrics"""
        
        if not metrics:
            return {"total_columns": 0, "avg_drift_score": 0}
            
        drift_scores = [m.get("drift_score", 0) for m in metrics if m.get("drift_score")]
        null_percentages = [m.get("null_percentage", 0) for m in metrics if m.get("null_percentage")]
        
        return {
            "total_columns": len(metrics),
            "avg_drift_score": sum(drift_scores) / len(drift_scores) if drift_scores else 0,
            "max_drift_score": max(drift_scores) if drift_scores else 0,
            "avg_null_percentage": sum(null_percentages) / len(null_percentages) if null_percentages else 0,
            "columns_with_drift": len([s for s in drift_scores if s > 0.1]),
            "high_drift_columns": len([s for s in drift_scores if s > 0.5])
        }