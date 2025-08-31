"""
Monitoring and Observability Service for SaaS Platform
Comprehensive monitoring, alerting, and health checks across control and data planes
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import json
import uuid
from pathlib import Path
import aiohttp

logger = logging.getLogger(__name__)

class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class AlertStatus(Enum):
    """Alert lifecycle status"""
    TRIGGERED = "triggered"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"

class HealthStatus(Enum):
    """Component health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass
class Alert:
    """Alert instance"""
    alert_id: str
    tenant_id: Optional[str]
    alert_type: str
    severity: AlertSeverity
    title: str
    description: str
    status: AlertStatus
    
    # Metadata
    component: str
    metric_name: Optional[str]
    metric_value: Optional[float]
    threshold: Optional[float]
    
    # Timestamps
    triggered_at: datetime
    acknowledged_at: Optional[datetime]
    resolved_at: Optional[datetime]
    
    # Context
    context: Dict[str, Any]
    tags: List[str]

@dataclass
class HealthCheck:
    """Health check definition"""
    check_id: str
    name: str
    component: str
    check_function: str  # Function name to call
    interval_seconds: int
    timeout_seconds: int
    enabled: bool
    
    # Thresholds
    warning_threshold: Optional[float]
    error_threshold: Optional[float]
    
    # State
    last_run_at: Optional[datetime]
    last_status: HealthStatus
    last_result: Optional[Dict[str, Any]]
    consecutive_failures: int

@dataclass
class Metric:
    """Metric data point"""
    metric_name: str
    value: float
    timestamp: datetime
    tenant_id: Optional[str]
    component: str
    tags: Dict[str, str]
    unit: Optional[str]

class MonitoringService:
    """Comprehensive monitoring and observability service"""
    
    def __init__(self, storage_path: str = "data/monitoring"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # State management
        self.alerts: Dict[str, Alert] = {}
        self.health_checks: Dict[str, HealthCheck] = {}
        self.metrics: List[Metric] = []  # In production, use time-series DB
        self.alert_rules: Dict[str, Dict[str, Any]] = {}
        
        # Notification handlers
        self.notification_handlers: Dict[str, Callable] = {}
        
        # Background tasks
        self._health_check_task: Optional[asyncio.Task] = None
        self._alert_processor_task: Optional[asyncio.Task] = None
        self._metric_cleanup_task: Optional[asyncio.Task] = None
        
        # Initialize default health checks and alert rules
        self._initialize_default_monitoring()
        
        # Load existing data
        self._load_monitoring_data()
    
    def _initialize_default_monitoring(self):
        """Initialize default health checks and alert rules"""
        
        # Default health checks
        default_health_checks = [
            HealthCheck(
                check_id="control_plane_api",
                name="Control Plane API Health",
                component="control_plane",
                check_function="check_api_health",
                interval_seconds=30,
                timeout_seconds=10,
                enabled=True,
                warning_threshold=1000.0,  # Response time in ms
                error_threshold=3000.0,
                last_run_at=None,
                last_status=HealthStatus.UNKNOWN,
                last_result=None,
                consecutive_failures=0
            ),
            
            HealthCheck(
                check_id="data_plane_processing",
                name="Data Plane Processing Health",
                component="data_plane",
                check_function="check_processing_health",
                interval_seconds=60,
                timeout_seconds=15,
                enabled=True,
                warning_threshold=None,
                error_threshold=None,
                last_run_at=None,
                last_status=HealthStatus.UNKNOWN,
                last_result=None,
                consecutive_failures=0
            ),
            
            HealthCheck(
                check_id="billing_service",
                name="Billing Service Health",
                component="billing",
                check_function="check_billing_health",
                interval_seconds=120,
                timeout_seconds=10,
                enabled=True,
                warning_threshold=None,
                error_threshold=None,
                last_run_at=None,
                last_status=HealthStatus.UNKNOWN,
                last_result=None,
                consecutive_failures=0
            )
        ]
        
        for check in default_health_checks:
            self.health_checks[check.check_id] = check
        
        # Default alert rules
        self.alert_rules = {
            "high_usage_warning": {
                "metric": "tenant_usage_percentage",
                "threshold": 80.0,
                "operator": "greater_than",
                "severity": AlertSeverity.WARNING,
                "title": "High Resource Usage",
                "description_template": "Tenant {tenant_id} usage is at {value:.1f}%"
            },
            
            "usage_limit_critical": {
                "metric": "tenant_usage_percentage", 
                "threshold": 95.0,
                "operator": "greater_than",
                "severity": AlertSeverity.CRITICAL,
                "title": "Critical Resource Usage",
                "description_template": "Tenant {tenant_id} approaching usage limit at {value:.1f}%"
            },
            
            "pipeline_failure_rate": {
                "metric": "pipeline_failure_rate",
                "threshold": 20.0,
                "operator": "greater_than",
                "severity": AlertSeverity.ERROR,
                "title": "High Pipeline Failure Rate",
                "description_template": "Pipeline failure rate is {value:.1f}% for tenant {tenant_id}"
            },
            
            "api_response_time": {
                "metric": "api_response_time",
                "threshold": 2000.0,
                "operator": "greater_than",
                "severity": AlertSeverity.WARNING,
                "title": "Slow API Response",
                "description_template": "API response time is {value:.0f}ms"
            }
        }
    
    def _load_monitoring_data(self):
        """Load existing monitoring data from storage"""
        # Load alerts
        alerts_file = self.storage_path / "alerts.json"
        if alerts_file.exists():
            try:
                with open(alerts_file, 'r') as f:
                    data = json.load(f)
                    for alert_id, alert_data in data.items():
                        # Convert datetime strings and enums
                        alert_data['triggered_at'] = datetime.fromisoformat(alert_data['triggered_at'])
                        if alert_data['acknowledged_at']:
                            alert_data['acknowledged_at'] = datetime.fromisoformat(alert_data['acknowledged_at'])
                        if alert_data['resolved_at']:
                            alert_data['resolved_at'] = datetime.fromisoformat(alert_data['resolved_at'])
                        
                        alert_data['severity'] = AlertSeverity(alert_data['severity'])
                        alert_data['status'] = AlertStatus(alert_data['status'])
                        
                        self.alerts[alert_id] = Alert(**alert_data)
                        
            except Exception as e:
                logger.error(f"Failed to load alerts: {e}")
        
        # Load health check states
        health_file = self.storage_path / "health_checks.json"
        if health_file.exists():
            try:
                with open(health_file, 'r') as f:
                    data = json.load(f)
                    for check_id, check_data in data.items():
                        if check_id in self.health_checks:
                            # Update state of existing checks
                            check = self.health_checks[check_id]
                            if check_data['last_run_at']:
                                check.last_run_at = datetime.fromisoformat(check_data['last_run_at'])
                            check.last_status = HealthStatus(check_data['last_status'])
                            check.last_result = check_data['last_result']
                            check.consecutive_failures = check_data['consecutive_failures']
                        
            except Exception as e:
                logger.error(f"Failed to load health checks: {e}")
    
    def _save_monitoring_data(self):
        """Persist monitoring data to storage"""
        # Save alerts
        alerts_data = {}
        for alert_id, alert in self.alerts.items():
            alert_dict = asdict(alert)
            # Convert datetime objects and enums
            alert_dict['triggered_at'] = alert.triggered_at.isoformat()
            if alert.acknowledged_at:
                alert_dict['acknowledged_at'] = alert.acknowledged_at.isoformat()
            if alert.resolved_at:
                alert_dict['resolved_at'] = alert.resolved_at.isoformat()
            
            alert_dict['severity'] = alert.severity.value
            alert_dict['status'] = alert.status.value
            
            alerts_data[alert_id] = alert_dict
        
        with open(self.storage_path / "alerts.json", 'w') as f:
            json.dump(alerts_data, f, indent=2)
        
        # Save health check states
        health_data = {}
        for check_id, check in self.health_checks.items():
            health_data[check_id] = {
                "last_run_at": check.last_run_at.isoformat() if check.last_run_at else None,
                "last_status": check.last_status.value,
                "last_result": check.last_result,
                "consecutive_failures": check.consecutive_failures
            }
        
        with open(self.storage_path / "health_checks.json", 'w') as f:
            json.dump(health_data, f, indent=2)
    
    async def start_monitoring(self):
        """Start all monitoring background tasks"""
        if self._health_check_task and not self._health_check_task.done():
            logger.warning("Health check monitoring already running")
        else:
            self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        if self._alert_processor_task and not self._alert_processor_task.done():
            logger.warning("Alert processor already running")
        else:
            self._alert_processor_task = asyncio.create_task(self._alert_processor_loop())
        
        if self._metric_cleanup_task and not self._metric_cleanup_task.done():
            logger.warning("Metric cleanup already running")
        else:
            self._metric_cleanup_task = asyncio.create_task(self._metric_cleanup_loop())
        
        logger.info("Started monitoring services")
    
    async def stop_monitoring(self):
        """Stop all monitoring background tasks"""
        if self._health_check_task:
            self._health_check_task.cancel()
        if self._alert_processor_task:
            self._alert_processor_task.cancel()
        if self._metric_cleanup_task:
            self._metric_cleanup_task.cancel()
        
        logger.info("Stopped monitoring services")
    
    async def _health_check_loop(self):
        """Main health check monitoring loop"""
        while True:
            try:
                await self._run_health_checks()
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(30)
    
    async def _run_health_checks(self):
        """Execute all enabled health checks"""
        now = datetime.utcnow()
        
        for check in self.health_checks.values():
            if not check.enabled:
                continue
            
            # Check if it's time to run this check
            if check.last_run_at:
                next_run = check.last_run_at + timedelta(seconds=check.interval_seconds)
                if now < next_run:
                    continue
            
            # Execute health check
            try:
                result = await self._execute_health_check(check)
                
                # Determine status
                status = HealthStatus.HEALTHY
                if check.error_threshold and result.get("response_time", 0) > check.error_threshold:
                    status = HealthStatus.UNHEALTHY
                elif check.warning_threshold and result.get("response_time", 0) > check.warning_threshold:
                    status = HealthStatus.DEGRADED
                elif result.get("error"):
                    status = HealthStatus.UNHEALTHY
                
                # Update check state
                previous_status = check.last_status
                check.last_run_at = now
                check.last_status = status
                check.last_result = result
                
                if status == HealthStatus.HEALTHY:
                    check.consecutive_failures = 0
                else:
                    check.consecutive_failures += 1
                
                # Generate alerts for status changes
                await self._handle_health_status_change(check, previous_status, status)
                
            except Exception as e:
                logger.error(f"Health check {check.check_id} failed: {e}")
                check.last_status = HealthStatus.UNHEALTHY
                check.consecutive_failures += 1
                check.last_result = {"error": str(e)}
        
        self._save_monitoring_data()
    
    async def _execute_health_check(self, check: HealthCheck) -> Dict[str, Any]:
        """Execute individual health check"""
        start_time = datetime.utcnow()
        
        try:
            if check.check_function == "check_api_health":
                result = await self._check_api_health()
            elif check.check_function == "check_processing_health":
                result = await self._check_processing_health()
            elif check.check_function == "check_billing_health":
                result = await self._check_billing_health()
            else:
                result = {"error": f"Unknown check function: {check.check_function}"}
            
            # Add response time
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            result["response_time"] = response_time
            
            return result
            
        except asyncio.TimeoutError:
            return {"error": "Health check timed out"}
        except Exception as e:
            return {"error": str(e)}
    
    async def _check_api_health(self) -> Dict[str, Any]:
        """Check control plane API health"""
        # Simulate API health check
        await asyncio.sleep(0.1)  # Simulate network call
        
        return {
            "status": "healthy",
            "endpoints_checked": 5,
            "all_endpoints_responsive": True
        }
    
    async def _check_processing_health(self) -> Dict[str, Any]:
        """Check data plane processing health"""
        # Simulate data plane health check
        await asyncio.sleep(0.2)
        
        return {
            "status": "healthy",
            "active_pipelines": 25,
            "processing_capacity": 85.5,
            "queue_depth": 12
        }
    
    async def _check_billing_health(self) -> Dict[str, Any]:
        """Check billing service health"""
        # Simulate billing health check
        await asyncio.sleep(0.1)
        
        return {
            "status": "healthy",
            "pending_invoices": 3,
            "billing_cycle_status": "on_schedule"
        }
    
    async def _handle_health_status_change(self, check: HealthCheck, previous_status: HealthStatus, current_status: HealthStatus):
        """Handle health status changes and generate alerts"""
        
        if previous_status == current_status:
            return
        
        # Generate alert for status degradation
        if current_status in [HealthStatus.DEGRADED, HealthStatus.UNHEALTHY]:
            severity = AlertSeverity.ERROR if current_status == HealthStatus.UNHEALTHY else AlertSeverity.WARNING
            
            await self._create_alert(
                alert_type="health_check_failure",
                severity=severity,
                title=f"{check.name} Status Changed",
                description=f"Health check {check.name} changed from {previous_status.value} to {current_status.value}",
                component=check.component,
                context={
                    "check_id": check.check_id,
                    "previous_status": previous_status.value,
                    "current_status": current_status.value,
                    "consecutive_failures": check.consecutive_failures,
                    "last_result": check.last_result
                }
            )
        
        # Resolve alert when health improves
        elif current_status == HealthStatus.HEALTHY and previous_status != HealthStatus.HEALTHY:
            await self._resolve_alerts_by_context({"check_id": check.check_id})
    
    async def _alert_processor_loop(self):
        """Main alert processing loop"""
        while True:
            try:
                await self._process_alerts()
                await asyncio.sleep(30)  # Process every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in alert processor loop: {e}")
                await asyncio.sleep(60)
    
    async def _process_alerts(self):
        """Process triggered alerts and send notifications"""
        triggered_alerts = [
            alert for alert in self.alerts.values() 
            if alert.status == AlertStatus.TRIGGERED
        ]
        
        for alert in triggered_alerts:
            try:
                await self._send_alert_notifications(alert)
            except Exception as e:
                logger.error(f"Failed to send notifications for alert {alert.alert_id}: {e}")
    
    async def _metric_cleanup_loop(self):
        """Clean up old metrics data"""
        while True:
            try:
                cutoff = datetime.utcnow() - timedelta(days=30)
                original_count = len(self.metrics)
                
                self.metrics = [
                    metric for metric in self.metrics 
                    if metric.timestamp >= cutoff
                ]
                
                removed_count = original_count - len(self.metrics)
                if removed_count > 0:
                    logger.info(f"Cleaned up {removed_count} old metrics")
                
                # Run every 6 hours
                await asyncio.sleep(21600)
                
            except Exception as e:
                logger.error(f"Error in metric cleanup loop: {e}")
                await asyncio.sleep(3600)
    
    async def record_metric(self, 
                          metric_name: str,
                          value: float,
                          tenant_id: Optional[str] = None,
                          component: str = "unknown",
                          tags: Optional[Dict[str, str]] = None,
                          unit: Optional[str] = None):
        """Record a metric data point"""
        
        metric = Metric(
            metric_name=metric_name,
            value=value,
            timestamp=datetime.utcnow(),
            tenant_id=tenant_id,
            component=component,
            tags=tags or {},
            unit=unit
        )
        
        self.metrics.append(metric)
        
        # Check alert rules
        await self._check_alert_rules(metric)
        
        logger.debug(f"Recorded metric {metric_name}: {value} for tenant {tenant_id}")
    
    async def _check_alert_rules(self, metric: Metric):
        """Check if metric violates any alert rules"""
        
        for rule_name, rule in self.alert_rules.items():
            if rule["metric"] != metric.metric_name:
                continue
            
            threshold = rule["threshold"]
            operator = rule["operator"]
            
            # Evaluate rule
            triggered = False
            if operator == "greater_than" and metric.value > threshold:
                triggered = True
            elif operator == "less_than" and metric.value < threshold:
                triggered = True
            elif operator == "equals" and metric.value == threshold:
                triggered = True
            
            if triggered:
                # Create alert
                description = rule["description_template"].format(
                    tenant_id=metric.tenant_id or "platform",
                    value=metric.value,
                    threshold=threshold
                )
                
                await self._create_alert(
                    alert_type=rule_name,
                    severity=rule["severity"],
                    title=rule["title"],
                    description=description,
                    component=metric.component,
                    tenant_id=metric.tenant_id,
                    metric_name=metric.metric_name,
                    metric_value=metric.value,
                    threshold=threshold,
                    context={
                        "rule_name": rule_name,
                        "metric_tags": metric.tags
                    }
                )
    
    async def _create_alert(self, 
                          alert_type: str,
                          severity: AlertSeverity,
                          title: str,
                          description: str,
                          component: str,
                          tenant_id: Optional[str] = None,
                          metric_name: Optional[str] = None,
                          metric_value: Optional[float] = None,
                          threshold: Optional[float] = None,
                          context: Optional[Dict[str, Any]] = None) -> str:
        """Create a new alert"""
        
        alert_id = f"alert_{uuid.uuid4().hex[:8]}"
        
        alert = Alert(
            alert_id=alert_id,
            tenant_id=tenant_id,
            alert_type=alert_type,
            severity=severity,
            title=title,
            description=description,
            status=AlertStatus.TRIGGERED,
            
            component=component,
            metric_name=metric_name,
            metric_value=metric_value,
            threshold=threshold,
            
            triggered_at=datetime.utcnow(),
            acknowledged_at=None,
            resolved_at=None,
            
            context=context or {},
            tags=[]
        )
        
        self.alerts[alert_id] = alert
        self._save_monitoring_data()
        
        logger.info(f"Created {severity.value} alert: {title} ({alert_id})")
        return alert_id
    
    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert"""
        if alert_id not in self.alerts:
            return False
        
        alert = self.alerts[alert_id]
        if alert.status == AlertStatus.TRIGGERED:
            alert.status = AlertStatus.ACKNOWLEDGED
            alert.acknowledged_at = datetime.utcnow()
            alert.context["acknowledged_by"] = acknowledged_by
            
            self._save_monitoring_data()
            logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
            return True
        
        return False
    
    async def resolve_alert(self, alert_id: str, resolved_by: str) -> bool:
        """Resolve an alert"""
        if alert_id not in self.alerts:
            return False
        
        alert = self.alerts[alert_id]
        if alert.status in [AlertStatus.TRIGGERED, AlertStatus.ACKNOWLEDGED]:
            alert.status = AlertStatus.RESOLVED
            alert.resolved_at = datetime.utcnow()
            alert.context["resolved_by"] = resolved_by
            
            self._save_monitoring_data()
            logger.info(f"Alert {alert_id} resolved by {resolved_by}")
            return True
        
        return False
    
    async def _resolve_alerts_by_context(self, context_match: Dict[str, Any]):
        """Resolve alerts matching context criteria"""
        for alert in self.alerts.values():
            if alert.status in [AlertStatus.TRIGGERED, AlertStatus.ACKNOWLEDGED]:
                # Check if alert context matches criteria
                match = all(
                    alert.context.get(key) == value 
                    for key, value in context_match.items()
                )
                
                if match:
                    alert.status = AlertStatus.RESOLVED
                    alert.resolved_at = datetime.utcnow()
                    alert.context["auto_resolved"] = True
        
        self._save_monitoring_data()
    
    def register_notification_handler(self, name: str, handler: Callable):
        """Register a notification handler"""
        self.notification_handlers[name] = handler
        logger.info(f"Registered notification handler: {name}")
    
    async def _send_alert_notifications(self, alert: Alert):
        """Send notifications for an alert"""
        for name, handler in self.notification_handlers.items():
            try:
                await handler(alert)
                logger.debug(f"Sent alert {alert.alert_id} via {name}")
            except Exception as e:
                logger.error(f"Failed to send alert via {name}: {e}")
    
    def get_active_alerts(self, tenant_id: Optional[str] = None, severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get active alerts with optional filtering"""
        active_alerts = [
            alert for alert in self.alerts.values()
            if alert.status in [AlertStatus.TRIGGERED, AlertStatus.ACKNOWLEDGED]
        ]
        
        if tenant_id:
            active_alerts = [alert for alert in active_alerts if alert.tenant_id == tenant_id]
        
        if severity:
            active_alerts = [alert for alert in active_alerts if alert.severity == severity]
        
        return sorted(active_alerts, key=lambda a: a.triggered_at, reverse=True)
    
    def get_system_health_summary(self) -> Dict[str, Any]:
        """Get overall system health summary"""
        
        # Health check status summary
        health_summary = {}
        for check in self.health_checks.values():
            component = check.component
            if component not in health_summary:
                health_summary[component] = {
                    "healthy": 0,
                    "degraded": 0,
                    "unhealthy": 0,
                    "unknown": 0
                }
            
            health_summary[component][check.last_status.value] += 1
        
        # Active alerts summary
        active_alerts = self.get_active_alerts()
        alert_summary = {
            "total": len(active_alerts),
            "critical": len([a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]),
            "error": len([a for a in active_alerts if a.severity == AlertSeverity.ERROR]),
            "warning": len([a for a in active_alerts if a.severity == AlertSeverity.WARNING]),
            "info": len([a for a in active_alerts if a.severity == AlertSeverity.INFO])
        }
        
        # Overall system status
        if alert_summary["critical"] > 0:
            overall_status = HealthStatus.UNHEALTHY
        elif alert_summary["error"] > 0 or any(
            summary["unhealthy"] > 0 for summary in health_summary.values()
        ):
            overall_status = HealthStatus.DEGRADED
        elif alert_summary["warning"] > 0 or any(
            summary["degraded"] > 0 for summary in health_summary.values()
        ):
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.HEALTHY
        
        return {
            "overall_status": overall_status.value,
            "health_checks": health_summary,
            "active_alerts": alert_summary,
            "last_updated": datetime.utcnow().isoformat()
        }
    
    def get_metrics_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get metrics summary for specified time period"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        recent_metrics = [m for m in self.metrics if m.timestamp >= cutoff]
        
        # Group by metric name
        metrics_by_name = {}
        for metric in recent_metrics:
            name = metric.metric_name
            if name not in metrics_by_name:
                metrics_by_name[name] = []
            metrics_by_name[name].append(metric.value)
        
        # Calculate summaries
        summary = {}
        for name, values in metrics_by_name.items():
            if values:
                summary[name] = {
                    "count": len(values),
                    "min": min(values),
                    "max": max(values),
                    "avg": sum(values) / len(values),
                    "latest": values[-1] if values else None
                }
        
        return {
            "period_hours": hours,
            "total_metrics": len(recent_metrics),
            "unique_metrics": len(metrics_by_name),
            "metrics": summary,
            "generated_at": datetime.utcnow().isoformat()
        }

# Global monitoring service instance
monitoring_service = MonitoringService()