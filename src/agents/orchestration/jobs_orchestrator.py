"""
Jobs Orchestrator for self-healing pipeline management
Handles Databricks Jobs API operations with intelligent retry and scaling
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import CreateJob, JobSettings, Task, NotebookTask, SparkPythonTask
from databricks.sdk.service.compute import ClusterSpec, AutoScale
from pyspark.sql import SparkSession
from typing import Dict, Any, List, Optional, Tuple
import logging
import time
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class JobsOrchestrator:
    """
    Orchestrates data pipeline jobs with self-healing capabilities
    Manages job lifecycle, dependencies, and failure recovery
    """
    
    def __init__(self, workspace_client: WorkspaceClient, spark: SparkSession, config: Dict[str, Any]):
        self.workspace_client = workspace_client
        self.spark = spark
        self.config = config
        self.job_registry = {}
        
    def create_ingestion_job(
        self,
        job_name: str,
        source_path: str,
        target_table: str,
        cluster_config: Dict[str, Any],
        schedule: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an ingestion job with auto-loader
        
        Args:
            job_name: Name of the job
            source_path: Source data path
            target_table: Target Unity Catalog table
            cluster_config: Cluster configuration
            schedule: Cron schedule (optional)
        """
        
        try:
            # Build job tasks
            ingestion_task = self._build_ingestion_task(source_path, target_table)
            quality_task = self._build_quality_check_task(target_table)
            
            # Job settings with dependencies
            job_settings = JobSettings(
                name=job_name,
                tasks=[ingestion_task, quality_task],
                timeout_seconds=3600,  # 1 hour timeout
                max_concurrent_runs=1,
                format="MULTI_TASK"
            )
            
            # Add schedule if provided
            if schedule:
                from databricks.sdk.service.jobs import CronSchedule
                job_settings.schedule = CronSchedule(
                    quartz_cron_expression=schedule,
                    timezone_id="UTC"
                )
                
            # Create the job
            job = self.workspace_client.jobs.create(job_settings)
            
            # Register job in our tracking
            self.job_registry[job_name] = {
                "job_id": job.job_id,
                "type": "ingestion",
                "source_path": source_path,
                "target_table": target_table,
                "created_at": datetime.now().isoformat(),
                "status": "created"
            }
            
            logger.info(f"Created ingestion job: {job_name} (ID: {job.job_id})")
            
            return {
                "job_name": job_name,
                "job_id": job.job_id,
                "status": "created",
                "url": f"{self.workspace_client.config.host}/#job/{job.job_id}"
            }
            
        except Exception as e:
            logger.error(f"Failed to create ingestion job: {str(e)}")
            return {"error": str(e), "job_name": job_name}
            
    def create_quality_monitoring_job(
        self,
        job_name: str,
        tables_to_monitor: List[str],
        cluster_config: Dict[str, Any],
        schedule: str = "0 */6 * * *"  # Every 6 hours
    ) -> Dict[str, Any]:
        """
        Create a job for continuous quality monitoring
        
        Args:
            job_name: Name of the monitoring job
            tables_to_monitor: List of tables to monitor
            cluster_config: Cluster configuration
            schedule: Cron schedule for monitoring
        """
        
        try:
            # Build monitoring tasks
            tasks = []
            
            for i, table in enumerate(tables_to_monitor):
                task = Task(
                    task_key=f"monitor_{table.replace('.', '_')}",
                    notebook_task=NotebookTask(
                        notebook_path=self.config.get("quality_monitoring_notebook"),
                        base_parameters={
                            "table_name": table,
                            "config": json.dumps(self.config)
                        }
                    ),
                    new_cluster=ClusterSpec(**cluster_config),
                    timeout_seconds=1800  # 30 minutes per table
                )
                tasks.append(task)
                
            # Summary task that depends on all monitoring tasks
            summary_task = Task(
                task_key="quality_summary",
                depends_on=[{"task_key": task.task_key} for task in tasks],
                notebook_task=NotebookTask(
                    notebook_path=self.config.get("quality_summary_notebook"),
                    base_parameters={
                        "tables": json.dumps(tables_to_monitor),
                        "config": json.dumps(self.config)
                    }
                ),
                new_cluster=ClusterSpec(**cluster_config),
                timeout_seconds=600  # 10 minutes
            )
            tasks.append(summary_task)
            
            # Job settings
            job_settings = JobSettings(
                name=job_name,
                tasks=tasks,
                timeout_seconds=7200,  # 2 hours total
                max_concurrent_runs=1,
                format="MULTI_TASK",
                schedule=CronSchedule(
                    quartz_cron_expression=schedule,
                    timezone_id="UTC"
                )
            )
            
            # Create the job
            job = self.workspace_client.jobs.create(job_settings)
            
            # Register job
            self.job_registry[job_name] = {
                "job_id": job.job_id,
                "type": "quality_monitoring",
                "tables": tables_to_monitor,
                "created_at": datetime.now().isoformat(),
                "status": "created"
            }
            
            logger.info(f"Created quality monitoring job: {job_name} (ID: {job.job_id})")
            
            return {
                "job_name": job_name,
                "job_id": job.job_id,
                "status": "created",
                "monitored_tables": len(tables_to_monitor),
                "url": f"{self.workspace_client.config.host}/#job/{job.job_id}"
            }
            
        except Exception as e:
            logger.error(f"Failed to create quality monitoring job: {str(e)}")
            return {"error": str(e), "job_name": job_name}
            
    def _build_ingestion_task(self, source_path: str, target_table: str) -> Task:
        """Build ingestion task configuration"""
        
        return Task(
            task_key="data_ingestion",
            notebook_task=NotebookTask(
                notebook_path=self.config.get("ingestion_notebook", "/Shared/ingestion/auto_loader_notebook"),
                base_parameters={
                    "source_path": source_path,
                    "target_table": target_table,
                    "config": json.dumps(self.config)
                }
            ),
            new_cluster=self._get_ingestion_cluster_spec(),
            timeout_seconds=3600,
            retry_on_timeout=True,
            max_retries=3
        )
        
    def _build_quality_check_task(self, target_table: str) -> Task:
        """Build quality check task that depends on ingestion"""
        
        return Task(
            task_key="quality_check",
            depends_on=[{"task_key": "data_ingestion"}],
            notebook_task=NotebookTask(
                notebook_path=self.config.get("quality_check_notebook", "/Shared/quality/quality_check_notebook"),
                base_parameters={
                    "target_table": target_table,
                    "config": json.dumps(self.config)
                }
            ),
            new_cluster=self._get_quality_cluster_spec(),
            timeout_seconds=1800,
            retry_on_timeout=True,
            max_retries=2
        )
        
    def _get_ingestion_cluster_spec(self) -> ClusterSpec:
        """Get cluster specification for ingestion jobs"""
        
        return ClusterSpec(
            spark_version=self.config.get("spark_version", "13.3.x-scala2.12"),
            node_type_id=self.config.get("ingestion_node_type", "i3.xlarge"),
            driver_node_type_id=self.config.get("ingestion_driver_node_type", "i3.xlarge"),
            autoscale=AutoScale(
                min_workers=self.config.get("ingestion_min_workers", 1),
                max_workers=self.config.get("ingestion_max_workers", 8)
            ),
            spark_conf={
                "spark.databricks.delta.autoCompact.enabled": "true",
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            },
            aws_attributes={
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 70
            } if self.config.get("use_spot_instances", True) else None
        )
        
    def _get_quality_cluster_spec(self) -> ClusterSpec:
        """Get cluster specification for quality check jobs"""
        
        return ClusterSpec(
            spark_version=self.config.get("spark_version", "13.3.x-scala2.12"),
            node_type_id=self.config.get("quality_node_type", "i3.large"),
            driver_node_type_id=self.config.get("quality_driver_node_type", "i3.large"),
            autoscale=AutoScale(
                min_workers=self.config.get("quality_min_workers", 1),
                max_workers=self.config.get("quality_max_workers", 4)
            ),
            spark_conf={
                "spark.sql.adaptive.enabled": "true"
            }
        )
        
    def trigger_job_run(self, job_name: str, parameters: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Trigger a job run manually
        
        Args:
            job_name: Name of the job to run
            parameters: Optional parameters to override
        """
        
        try:
            job_info = self.job_registry.get(job_name)
            if not job_info:
                return {"error": f"Job not found: {job_name}"}
                
            job_id = job_info["job_id"]
            
            # Trigger run
            run = self.workspace_client.jobs.run_now(
                job_id=job_id,
                notebook_params=parameters or {}
            )
            
            logger.info(f"Triggered job run: {job_name} (Run ID: {run.run_id})")
            
            return {
                "job_name": job_name,
                "job_id": job_id,
                "run_id": run.run_id,
                "status": "triggered",
                "url": f"{self.workspace_client.config.host}/#job/{job_id}/run/{run.run_id}"
            }
            
        except Exception as e:
            logger.error(f"Failed to trigger job run: {str(e)}")
            return {"error": str(e), "job_name": job_name}
            
    def monitor_job_run(self, job_name: str, run_id: int, timeout: int = 3600) -> Dict[str, Any]:
        """
        Monitor a job run until completion or timeout
        
        Args:
            job_name: Name of the job
            run_id: Run ID to monitor
            timeout: Timeout in seconds
        """
        
        try:
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                run_info = self.workspace_client.jobs.get_run(run_id)
                
                if run_info.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                    result_state = run_info.state.result_state
                    
                    run_result = {
                        "job_name": job_name,
                        "run_id": run_id,
                        "status": result_state.value if result_state else "UNKNOWN",
                        "duration_seconds": (run_info.end_time - run_info.start_time) / 1000 if run_info.end_time else None,
                        "tasks": []
                    }
                    
                    # Get task results
                    if hasattr(run_info, 'tasks') and run_info.tasks:
                        for task in run_info.tasks:
                            task_result = {
                                "task_key": task.task_key,
                                "status": task.state.result_state.value if task.state.result_state else "UNKNOWN",
                                "duration_seconds": (task.end_time - task.start_time) / 1000 if task.end_time and task.start_time else None
                            }
                            run_result["tasks"].append(task_result)
                            
                    # Handle failures
                    if result_state and result_state.value == "FAILED":
                        self._handle_job_failure(job_name, run_id, run_info)
                        
                    return run_result
                    
                time.sleep(30)  # Check every 30 seconds
                
            # Timeout reached
            return {
                "job_name": job_name,
                "run_id": run_id,
                "status": "TIMEOUT",
                "message": f"Monitoring timeout after {timeout} seconds"
            }
            
        except Exception as e:
            logger.error(f"Failed to monitor job run: {str(e)}")
            return {"error": str(e), "job_name": job_name, "run_id": run_id}
            
    def _handle_job_failure(self, job_name: str, run_id: int, run_info: Any):
        """Handle job failure with self-healing logic"""
        
        logger.warning(f"Job failure detected: {job_name} (Run: {run_id})")
        
        # Analyze failure cause
        failure_analysis = self._analyze_job_failure(run_info)
        
        # Log failure for monitoring
        failure_event = {
            "job_name": job_name,
            "run_id": run_id,
            "failure_type": failure_analysis["type"],
            "failure_reason": failure_analysis["reason"],
            "timestamp": datetime.now().isoformat(),
            "retry_recommended": failure_analysis["can_retry"]
        }
        
        self._log_job_failure(failure_event)
        
        # Implement self-healing actions
        if failure_analysis["can_retry"]:
            self._initiate_retry_with_backoff(job_name, failure_analysis)
        elif failure_analysis["type"] == "resource_constraint":
            self._scale_up_cluster(job_name)
        elif failure_analysis["type"] == "data_quality":
            self._trigger_data_repair(job_name)
            
    def _analyze_job_failure(self, run_info: Any) -> Dict[str, Any]:
        """Analyze job failure to determine cause and recovery action"""
        
        # Extract error messages from tasks
        error_messages = []
        
        if hasattr(run_info, 'tasks') and run_info.tasks:
            for task in run_info.tasks:
                if task.state.result_state and task.state.result_state.value == "FAILED":
                    if hasattr(task, 'state') and hasattr(task.state, 'state_message'):
                        error_messages.append(task.state.state_message)
                        
        combined_error = " ".join(error_messages).lower()
        
        # Categorize failure type
        if "memory" in combined_error or "heap space" in combined_error:
            return {
                "type": "resource_constraint",
                "reason": "Out of memory",
                "can_retry": True,
                "recovery_action": "scale_up"
            }
        elif "timeout" in combined_error or "deadline exceeded" in combined_error:
            return {
                "type": "timeout",
                "reason": "Job timeout",
                "can_retry": True,
                "recovery_action": "retry_with_longer_timeout"
            }
        elif "schema" in combined_error or "column" in combined_error:
            return {
                "type": "schema_drift",
                "reason": "Schema-related error",
                "can_retry": False,
                "recovery_action": "schema_adaptation"
            }
        elif "quality" in combined_error or "expectation" in combined_error:
            return {
                "type": "data_quality",
                "reason": "Data quality violation",
                "can_retry": False,
                "recovery_action": "data_repair"
            }
        else:
            return {
                "type": "unknown",
                "reason": combined_error[:200] if combined_error else "Unknown error",
                "can_retry": True,
                "recovery_action": "simple_retry"
            }
            
    def _initiate_retry_with_backoff(self, job_name: str, failure_analysis: Dict[str, Any]):
        """Initiate retry with exponential backoff"""
        
        retry_count = self._get_recent_retry_count(job_name)
        
        if retry_count < 3:  # Max 3 retries
            backoff_delay = min(300, 60 * (2 ** retry_count))  # Exponential backoff, max 5 minutes
            
            logger.info(f"Scheduling retry for {job_name} in {backoff_delay} seconds (attempt {retry_count + 1})")
            
            # In a production system, this would schedule the retry
            # For now, we'll log the retry intention
            retry_event = {
                "job_name": job_name,
                "retry_attempt": retry_count + 1,
                "backoff_delay": backoff_delay,
                "scheduled_time": (datetime.now() + timedelta(seconds=backoff_delay)).isoformat(),
                "failure_reason": failure_analysis["reason"]
            }
            
            self._log_retry_event(retry_event)
            
    def _get_recent_retry_count(self, job_name: str) -> int:
        """Get retry count for recent failures"""
        
        try:
            # Query monitoring table for recent retries
            monitoring_table = self.config.get("job_monitoring_table", "monitoring.job_events")
            
            retry_count_df = (self.spark.table(monitoring_table)
                            .filter(f"job_name = '{job_name}'")
                            .filter("event_type = 'retry'")
                            .filter("timestamp >= current_timestamp() - interval 1 hour")
                            .count())
            
            return retry_count_df
            
        except Exception as e:
            logger.warning(f"Could not get retry count: {str(e)}")
            return 0
            
    def _log_job_failure(self, failure_event: Dict[str, Any]):
        """Log job failure to monitoring table"""
        
        try:
            monitoring_table = self.config.get("job_monitoring_table", "monitoring.job_events")
            
            failure_event["event_type"] = "failure"
            failure_df = self.spark.createDataFrame([failure_event])
            failure_df.write.format("delta").mode("append").saveAsTable(monitoring_table)
            
        except Exception as e:
            logger.error(f"Failed to log job failure: {str(e)}")
            
    def _log_retry_event(self, retry_event: Dict[str, Any]):
        """Log retry event to monitoring table"""
        
        try:
            monitoring_table = self.config.get("job_monitoring_table", "monitoring.job_events")
            
            retry_event["event_type"] = "retry"
            retry_event["timestamp"] = datetime.now().isoformat()
            
            retry_df = self.spark.createDataFrame([retry_event])
            retry_df.write.format("delta").mode("append").saveAsTable(monitoring_table)
            
        except Exception as e:
            logger.error(f"Failed to log retry event: {str(e)}")
            
    def get_job_health_metrics(self, job_name: str, days: int = 7) -> Dict[str, Any]:
        """Get job health metrics for monitoring dashboard"""
        
        try:
            monitoring_table = self.config.get("job_monitoring_table", "monitoring.job_events")
            
            # Get job runs from Databricks
            job_info = self.job_registry.get(job_name)
            if not job_info:
                return {"error": f"Job not found: {job_name}"}
                
            job_id = job_info["job_id"]
            runs = self.workspace_client.jobs.list_runs(job_id=job_id, limit=100)
            
            # Calculate metrics
            total_runs = len(runs)
            successful_runs = sum(1 for run in runs if run.state.result_state and run.state.result_state.value == "SUCCESS")
            failed_runs = sum(1 for run in runs if run.state.result_state and run.state.result_state.value == "FAILED")
            
            # Average duration
            completed_runs = [run for run in runs if run.end_time and run.start_time]
            avg_duration = sum((run.end_time - run.start_time) / 1000 for run in completed_runs) / len(completed_runs) if completed_runs else 0
            
            # Recent failure analysis
            recent_failures = [run for run in runs if run.state.result_state and run.state.result_state.value == "FAILED"][:5]
            
            return {
                "job_name": job_name,
                "job_id": job_id,
                "period_days": days,
                "total_runs": total_runs,
                "successful_runs": successful_runs,
                "failed_runs": failed_runs,
                "success_rate": (successful_runs / total_runs) * 100 if total_runs > 0 else 0,
                "average_duration_minutes": avg_duration / 60,
                "recent_failures": len(recent_failures),
                "last_run_status": runs[0].state.result_state.value if runs and runs[0].state.result_state else "UNKNOWN"
            }
            
        except Exception as e:
            logger.error(f"Failed to get job health metrics: {str(e)}")
            return {"error": str(e), "job_name": job_name}