"""
Adaptive Cluster Manager for cost-optimized, performance-aware cluster scaling
Dynamically adjusts cluster resources based on workload patterns and costs
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec, AutoScale, CreateCluster, EditCluster
from pyspark.sql import SparkSession
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, timedelta
import math

logger = logging.getLogger(__name__)


class AdaptiveClusterManager:
    """
    Manages cluster lifecycle with adaptive scaling based on:
    - Workload patterns
    - Cost optimization
    - Performance requirements
    - Resource utilization
    """
    
    def __init__(self, workspace_client: WorkspaceClient, spark: SparkSession, config: Dict[str, Any]):
        self.workspace_client = workspace_client
        self.spark = spark
        self.config = config
        self.cluster_registry = {}
        
    def create_adaptive_cluster(
        self,
        cluster_name: str,
        workload_type: str,
        initial_size: Optional[Dict[str, int]] = None,
        cost_optimization_level: str = "balanced"
    ) -> Dict[str, Any]:
        """
        Create an adaptive cluster optimized for specific workload type
        
        Args:
            cluster_name: Name of the cluster
            workload_type: Type of workload (ingestion, quality, analytics)
            initial_size: Initial cluster size {min_workers, max_workers}
            cost_optimization_level: aggressive, balanced, performance
        """
        
        try:
            # Get optimal cluster configuration
            cluster_config = self._get_workload_optimized_config(
                workload_type, 
                initial_size, 
                cost_optimization_level
            )
            
            # Create cluster
            cluster = self.workspace_client.clusters.create(**cluster_config)
            
            # Register cluster for monitoring
            self.cluster_registry[cluster_name] = {
                "cluster_id": cluster.cluster_id,
                "workload_type": workload_type,
                "cost_optimization": cost_optimization_level,
                "created_at": datetime.now().isoformat(),
                "initial_config": cluster_config,
                "scaling_history": []
            }
            
            logger.info(f"Created adaptive cluster: {cluster_name} (ID: {cluster.cluster_id})")
            
            return {
                "cluster_name": cluster_name,
                "cluster_id": cluster.cluster_id,
                "workload_type": workload_type,
                "status": "created",
                "config": cluster_config
            }
            
        except Exception as e:
            logger.error(f"Failed to create adaptive cluster: {str(e)}")
            return {"error": str(e), "cluster_name": cluster_name}
            
    def _get_workload_optimized_config(
        self,
        workload_type: str,
        initial_size: Optional[Dict[str, int]],
        cost_optimization: str
    ) -> Dict[str, Any]:
        """Get cluster configuration optimized for workload type"""
        
        base_configs = {
            "ingestion": {
                "node_type_id": "i3.xlarge",
                "driver_node_type_id": "i3.xlarge",
                "min_workers": 2,
                "max_workers": 10,
                "spark_conf": {
                    "spark.databricks.delta.autoCompact.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728"  # 128MB
                }
            },
            "quality": {
                "node_type_id": "i3.large",
                "driver_node_type_id": "i3.large", 
                "min_workers": 1,
                "max_workers": 6,
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.execution.arrow.pyspark.enabled": "true"
                }
            },
            "analytics": {
                "node_type_id": "r5.xlarge",
                "driver_node_type_id": "r5.xlarge",
                "min_workers": 2,
                "max_workers": 20,
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.localShuffleReader.enabled": "true",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                }
            }
        }
        
        config = base_configs.get(workload_type, base_configs["ingestion"]).copy()
        
        # Override with initial size if provided
        if initial_size:
            config.update(initial_size)
            
        # Apply cost optimization
        config = self._apply_cost_optimization(config, cost_optimization)
        
        # Build final cluster spec
        cluster_config = {
            "cluster_name": f"{workload_type}_cluster_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "spark_version": self.config.get("spark_version", "13.3.x-scala2.12"),
            "node_type_id": config["node_type_id"],
            "driver_node_type_id": config.get("driver_node_type_id", config["node_type_id"]),
            "autoscale": AutoScale(
                min_workers=config["min_workers"],
                max_workers=config["max_workers"]
            ),
            "spark_conf": config["spark_conf"],
            "enable_elastic_disk": True,
            "runtime_engine": "PHOTON"
        }
        
        # Add AWS-specific optimizations
        if cost_optimization in ["aggressive", "balanced"]:
            cluster_config["aws_attributes"] = {
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 70 if cost_optimization == "aggressive" else 80,
                "zone_id": "auto"
            }
            
        return cluster_config
        
    def _apply_cost_optimization(self, config: Dict[str, Any], optimization_level: str) -> Dict[str, Any]:
        """Apply cost optimization strategies"""
        
        if optimization_level == "aggressive":
            # Use smaller instance types and lower max workers
            node_type_mapping = {
                "i3.xlarge": "i3.large",
                "i3.large": "m5.large", 
                "r5.xlarge": "r5.large"
            }
            
            config["node_type_id"] = node_type_mapping.get(config["node_type_id"], config["node_type_id"])
            config["max_workers"] = max(2, int(config["max_workers"] * 0.7))
            
        elif optimization_level == "performance":
            # Use larger instance types and higher max workers
            node_type_mapping = {
                "i3.large": "i3.xlarge",
                "i3.xlarge": "i3.2xlarge",
                "r5.large": "r5.xlarge",
                "r5.xlarge": "r5.2xlarge"
            }
            
            config["node_type_id"] = node_type_mapping.get(config["node_type_id"], config["node_type_id"])
            config["max_workers"] = int(config["max_workers"] * 1.5)
            
        return config
        
    def monitor_cluster_utilization(self, cluster_name: str) -> Dict[str, Any]:
        """Monitor cluster resource utilization and performance"""
        
        try:
            cluster_info = self.cluster_registry.get(cluster_name)
            if not cluster_info:
                return {"error": f"Cluster not found: {cluster_name}"}
                
            cluster_id = cluster_info["cluster_id"]
            
            # Get cluster details
            cluster = self.workspace_client.clusters.get(cluster_id)
            
            # Get cluster metrics (simplified - in production would use Metrics API)
            utilization_metrics = self._calculate_cluster_utilization(cluster_id)
            
            # Performance metrics
            performance_metrics = self._get_cluster_performance_metrics(cluster_id)
            
            # Cost metrics
            cost_metrics = self._calculate_cluster_costs(cluster, utilization_metrics)
            
            return {
                "cluster_name": cluster_name,
                "cluster_id": cluster_id,
                "state": cluster.state.value,
                "current_workers": cluster.num_workers if hasattr(cluster, 'num_workers') else 0,
                "utilization": utilization_metrics,
                "performance": performance_metrics,
                "cost": cost_metrics,
                "recommendations": self._generate_scaling_recommendations(
                    utilization_metrics, performance_metrics, cost_metrics
                )
            }
            
        except Exception as e:
            logger.error(f"Failed to monitor cluster utilization: {str(e)}")
            return {"error": str(e), "cluster_name": cluster_name}
            
    def _calculate_cluster_utilization(self, cluster_id: str) -> Dict[str, Any]:
        """Calculate cluster resource utilization"""
        
        # In production, this would query Databricks metrics API or Ganglia
        # For now, simulate realistic utilization metrics
        
        try:
            # Query recent job runs to estimate utilization
            recent_runs = self.workspace_client.jobs.list_runs(
                limit=50,
                start_time_from=int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)
            )
            
            active_runs = sum(1 for run in recent_runs if run.state.life_cycle_state == "RUNNING")
            
            # Simulate utilization based on active runs
            cpu_utilization = min(95, max(10, active_runs * 20 + 30))
            memory_utilization = min(90, max(15, active_runs * 15 + 25))
            
            return {
                "cpu_utilization_percent": cpu_utilization,
                "memory_utilization_percent": memory_utilization,
                "disk_utilization_percent": 45,  # Simulated
                "network_utilization_percent": 25,  # Simulated
                "active_jobs": active_runs
            }
            
        except Exception as e:
            logger.warning(f"Could not calculate utilization: {str(e)}")
            return {
                "cpu_utilization_percent": 50,
                "memory_utilization_percent": 40,
                "disk_utilization_percent": 30,
                "network_utilization_percent": 20,
                "active_jobs": 1
            }
            
    def _get_cluster_performance_metrics(self, cluster_id: str) -> Dict[str, Any]:
        """Get cluster performance metrics"""
        
        # Simulated performance metrics - in production would use Spark metrics
        return {
            "avg_task_duration_seconds": 45,
            "task_success_rate_percent": 98.5,
            "shuffle_read_mb_per_second": 150,
            "shuffle_write_mb_per_second": 120,
            "gc_time_percent": 8
        }
        
    def _calculate_cluster_costs(self, cluster: Any, utilization: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cluster cost metrics"""
        
        # Simplified cost calculation - in production would use actual pricing
        base_hourly_rate = self._get_node_hourly_rate(cluster.node_type_id)
        current_workers = getattr(cluster, 'num_workers', 2)
        
        hourly_cost = base_hourly_rate * (current_workers + 1)  # +1 for driver
        
        # Efficiency score based on utilization
        avg_utilization = (utilization["cpu_utilization_percent"] + utilization["memory_utilization_percent"]) / 2
        efficiency_score = min(100, avg_utilization)
        
        return {
            "hourly_cost_usd": round(hourly_cost, 2),
            "daily_cost_estimate_usd": round(hourly_cost * 24, 2),
            "efficiency_score_percent": round(efficiency_score, 1),
            "cost_per_efficiency_point": round(hourly_cost / max(1, efficiency_score), 3),
            "spot_savings_percent": 30 if hasattr(cluster, 'aws_attributes') else 0
        }
        
    def _get_node_hourly_rate(self, node_type: str) -> float:
        """Get approximate hourly rate for node type"""
        
        # Simplified pricing - in production would use actual AWS/Azure pricing
        pricing = {
            "i3.large": 0.156,
            "i3.xlarge": 0.312,
            "i3.2xlarge": 0.624,
            "m5.large": 0.096,
            "m5.xlarge": 0.192,
            "r5.large": 0.126,
            "r5.xlarge": 0.252,
            "r5.2xlarge": 0.504
        }
        
        return pricing.get(node_type, 0.2)  # Default rate
        
    def _generate_scaling_recommendations(
        self,
        utilization: Dict[str, Any],
        performance: Dict[str, Any], 
        cost: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate intelligent scaling recommendations"""
        
        recommendations = []
        
        # High utilization - scale up
        if utilization["cpu_utilization_percent"] > 80 or utilization["memory_utilization_percent"] > 85:
            recommendations.append({
                "action": "scale_up",
                "priority": "high",
                "reason": "High resource utilization detected",
                "suggested_change": "Increase max_workers by 2-4 nodes",
                "expected_impact": "Improved performance, higher cost"
            })
            
        # Low utilization - scale down
        elif utilization["cpu_utilization_percent"] < 30 and utilization["memory_utilization_percent"] < 35:
            recommendations.append({
                "action": "scale_down", 
                "priority": "medium",
                "reason": "Low resource utilization",
                "suggested_change": "Decrease max_workers or use smaller instances",
                "expected_impact": "Cost savings, maintained performance"
            })
            
        # Poor efficiency - optimize
        if cost["efficiency_score_percent"] < 60:
            recommendations.append({
                "action": "optimize",
                "priority": "medium",
                "reason": "Low efficiency score",
                "suggested_change": "Enable autoscaling, use spot instances",
                "expected_impact": "Better cost efficiency"
            })
            
        # Performance issues
        if performance["task_success_rate_percent"] < 95:
            recommendations.append({
                "action": "troubleshoot",
                "priority": "high", 
                "reason": "Low task success rate",
                "suggested_change": "Investigate failures, consider larger instances",
                "expected_impact": "Improved reliability"
            })
            
        return recommendations
        
    def auto_scale_cluster(self, cluster_name: str, scaling_decision: str) -> Dict[str, Any]:
        """
        Automatically scale cluster based on intelligent decision
        
        Args:
            cluster_name: Name of cluster to scale
            scaling_decision: scale_up, scale_down, optimize
        """
        
        try:
            cluster_info = self.cluster_registry.get(cluster_name)
            if not cluster_info:
                return {"error": f"Cluster not found: {cluster_name}"}
                
            cluster_id = cluster_info["cluster_id"]
            cluster = self.workspace_client.clusters.get(cluster_id)
            
            # Current autoscale settings
            current_min = cluster.autoscale.min_workers
            current_max = cluster.autoscale.max_workers
            
            # Calculate new settings based on decision
            if scaling_decision == "scale_up":
                new_min = min(current_min + 1, current_max)
                new_max = current_max + 3
            elif scaling_decision == "scale_down":
                new_min = max(1, current_min - 1)
                new_max = max(new_min + 1, current_max - 2)
            else:  # optimize
                # Keep current settings but enable additional optimizations
                new_min = current_min
                new_max = current_max
                
            # Apply scaling
            edit_request = EditCluster(
                cluster_id=cluster_id,
                autoscale=AutoScale(
                    min_workers=new_min,
                    max_workers=new_max
                )
            )
            
            self.workspace_client.clusters.edit(**edit_request)
            
            # Record scaling event
            scaling_event = {
                "cluster_name": cluster_name,
                "scaling_action": scaling_decision,
                "old_min_workers": current_min,
                "old_max_workers": current_max,
                "new_min_workers": new_min,
                "new_max_workers": new_max,
                "timestamp": datetime.now().isoformat()
            }
            
            self.cluster_registry[cluster_name]["scaling_history"].append(scaling_event)
            self._log_scaling_event(scaling_event)
            
            logger.info(f"Auto-scaled cluster {cluster_name}: {scaling_decision}")
            
            return {
                "cluster_name": cluster_name,
                "action": scaling_decision,
                "old_range": f"{current_min}-{current_max}",
                "new_range": f"{new_min}-{new_max}",
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Failed to auto-scale cluster: {str(e)}")
            return {"error": str(e), "cluster_name": cluster_name}
            
    def _log_scaling_event(self, scaling_event: Dict[str, Any]):
        """Log scaling event for monitoring"""
        
        try:
            monitoring_table = self.config.get("cluster_monitoring_table", "monitoring.cluster_scaling_events")
            
            scaling_df = self.spark.createDataFrame([scaling_event])
            scaling_df.write.format("delta").mode("append").saveAsTable(monitoring_table)
            
        except Exception as e:
            logger.error(f"Failed to log scaling event: {str(e)}")
            
    def get_cluster_cost_analysis(self, cluster_name: str, days: int = 30) -> Dict[str, Any]:
        """Get comprehensive cost analysis for cluster"""
        
        try:
            cluster_info = self.cluster_registry.get(cluster_name)
            if not cluster_info:
                return {"error": f"Cluster not found: {cluster_name}"}
                
            # Get scaling history
            scaling_history = cluster_info.get("scaling_history", [])
            
            # Calculate cost trends
            current_metrics = self.monitor_cluster_utilization(cluster_name)
            
            if "error" in current_metrics:
                return current_metrics
                
            hourly_cost = current_metrics["cost"]["hourly_cost_usd"]
            efficiency_score = current_metrics["cost"]["efficiency_score_percent"]
            
            # Estimate monthly costs
            monthly_cost_estimate = hourly_cost * 24 * 30
            
            # Optimization potential
            optimization_savings = self._calculate_optimization_potential(current_metrics)
            
            return {
                "cluster_name": cluster_name,
                "period_days": days,
                "current_hourly_cost": hourly_cost,
                "monthly_cost_estimate": round(monthly_cost_estimate, 2),
                "efficiency_score": efficiency_score,
                "optimization_potential": optimization_savings,
                "scaling_events": len(scaling_history),
                "cost_trend": "stable",  # Would calculate from historical data
                "recommendations": current_metrics.get("recommendations", [])
            }
            
        except Exception as e:
            logger.error(f"Failed to get cost analysis: {str(e)}")
            return {"error": str(e), "cluster_name": cluster_name}
            
    def _calculate_optimization_potential(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate potential cost savings from optimization"""
        
        current_cost = metrics["cost"]["hourly_cost_usd"]
        efficiency = metrics["cost"]["efficiency_score_percent"]
        
        # Potential savings from spot instances
        spot_savings = current_cost * 0.3 if metrics["cost"]["spot_savings_percent"] == 0 else 0
        
        # Potential savings from right-sizing
        if efficiency < 60:
            rightsizing_savings = current_cost * 0.2
        elif efficiency < 40:
            rightsizing_savings = current_cost * 0.4
        else:
            rightsizing_savings = 0
            
        total_potential = spot_savings + rightsizing_savings
        
        return {
            "total_monthly_savings": round(total_potential * 24 * 30, 2),
            "spot_instance_savings": round(spot_savings * 24 * 30, 2),
            "rightsizing_savings": round(rightsizing_savings * 24 * 30, 2),
            "savings_percentage": round((total_potential / current_cost) * 100, 1) if current_cost > 0 else 0
        }