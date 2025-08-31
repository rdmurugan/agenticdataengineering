"""
Usage Tracking Service for SaaS Platform
Tracks tenant resource consumption for billing and monitoring
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from decimal import Decimal
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

@dataclass
class UsageEvent:
    """Individual usage event for tracking"""
    tenant_id: str
    event_type: str  # gb_processed, compute_hours, storage_gb, quality_checks
    quantity: Decimal
    timestamp: datetime
    pipeline_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class UsageTracker:
    """Tracks and aggregates resource usage across all tenants"""
    
    def __init__(self):
        # In-memory usage tracking (replace with time-series DB in production)
        self.usage_events: List[UsageEvent] = []
        self.tenant_usage_cache: Dict[str, Dict[str, Decimal]] = defaultdict(
            lambda: {
                "gb_processed": Decimal("0"),
                "compute_hours": Decimal("0"),
                "storage_gb": Decimal("0"),
                "quality_checks": Decimal("0")
            }
        )
        
        # Real-time metrics
        self.real_time_metrics: Dict[str, Any] = {}
        
        # Background monitoring task
        self._monitoring_task: Optional[asyncio.Task] = None
    
    async def track_usage(self, 
                         tenant_id: str, 
                         event_type: str, 
                         quantity: Decimal,
                         pipeline_id: Optional[str] = None,
                         metadata: Optional[Dict[str, Any]] = None):
        """Track a usage event"""
        
        usage_event = UsageEvent(
            tenant_id=tenant_id,
            event_type=event_type,
            quantity=quantity,
            timestamp=datetime.utcnow(),
            pipeline_id=pipeline_id,
            metadata=metadata or {}
        )
        
        # Store event
        self.usage_events.append(usage_event)
        
        # Update cache
        self.tenant_usage_cache[tenant_id][event_type] += quantity
        
        # Update real-time metrics
        await self._update_real_time_metrics(usage_event)
        
        logger.debug(f"Tracked {quantity} {event_type} for tenant {tenant_id}")
    
    async def _update_real_time_metrics(self, usage_event: UsageEvent):
        """Update real-time metrics dashboard"""
        now = datetime.utcnow()
        hour_key = now.strftime("%Y-%m-%d-%H")
        
        if hour_key not in self.real_time_metrics:
            self.real_time_metrics[hour_key] = {
                "timestamp": now,
                "tenant_activity": defaultdict(lambda: defaultdict(Decimal)),
                "platform_totals": defaultdict(Decimal)
            }
        
        metrics = self.real_time_metrics[hour_key]
        
        # Update tenant-specific metrics
        metrics["tenant_activity"][usage_event.tenant_id][usage_event.event_type] += usage_event.quantity
        
        # Update platform totals
        metrics["platform_totals"][usage_event.event_type] += usage_event.quantity
    
    async def get_tenant_usage(self, 
                              tenant_id: str,
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get detailed usage information for a tenant"""
        
        # Use current month if no dates provided
        if not start_date:
            now = datetime.utcnow()
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if not end_date:
            end_date = datetime.utcnow()
        
        # Filter events for tenant and time period
        tenant_events = [
            event for event in self.usage_events
            if (event.tenant_id == tenant_id and 
                start_date <= event.timestamp <= end_date)
        ]
        
        # Aggregate usage by type
        usage_by_type = defaultdict(Decimal)
        usage_by_pipeline = defaultdict(lambda: defaultdict(Decimal))
        usage_by_day = defaultdict(lambda: defaultdict(Decimal))
        
        for event in tenant_events:
            usage_by_type[event.event_type] += event.quantity
            
            if event.pipeline_id:
                usage_by_pipeline[event.pipeline_id][event.event_type] += event.quantity
            
            day_key = event.timestamp.strftime("%Y-%m-%d")
            usage_by_day[day_key][event.event_type] += event.quantity
        
        # Calculate costs (mock pricing)
        pricing = {
            "gb_processed": Decimal("0.10"),
            "compute_hours": Decimal("2.50"),
            "storage_gb": Decimal("0.05"),
            "quality_checks": Decimal("0.001")
        }
        
        total_cost = sum(
            usage_by_type[metric] * price 
            for metric, price in pricing.items()
        )
        
        return {
            "tenant_id": tenant_id,
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "usage_summary": {k: str(v) for k, v in usage_by_type.items()},
            "estimated_cost": str(total_cost),
            "usage_by_pipeline": {
                pipeline_id: {k: str(v) for k, v in metrics.items()}
                for pipeline_id, metrics in usage_by_pipeline.items()
            },
            "daily_usage": {
                day: {k: str(v) for k, v in metrics.items()}
                for day, metrics in usage_by_day.items()
            },
            "event_count": len(tenant_events),
            "generated_at": datetime.utcnow().isoformat()
        }
    
    async def start_monitoring(self):
        """Start background monitoring and aggregation"""
        if self._monitoring_task and not self._monitoring_task.done():
            logger.warning("Usage monitoring already running")
            return
        
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Started usage monitoring")
    
    async def _monitoring_loop(self):
        """Main monitoring loop for real-time tracking"""
        while True:
            try:
                await self._aggregate_usage_data()
                await self._cleanup_old_data()
                
                # Run every 5 minutes
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Error in usage monitoring loop: {e}")
                await asyncio.sleep(300)
    
    async def _aggregate_usage_data(self):
        """Aggregate usage data for efficient querying"""
        # Clean up real-time metrics older than 24 hours
        cutoff = datetime.utcnow() - timedelta(hours=24)
        
        keys_to_remove = []
        for hour_key, metrics in self.real_time_metrics.items():
            if metrics["timestamp"] < cutoff:
                keys_to_remove.append(hour_key)
        
        for key in keys_to_remove:
            del self.real_time_metrics[key]
    
    async def _cleanup_old_data(self):
        """Clean up old usage events (keep last 90 days)"""
        cutoff = datetime.utcnow() - timedelta(days=90)
        
        original_count = len(self.usage_events)
        self.usage_events = [
            event for event in self.usage_events 
            if event.timestamp >= cutoff
        ]
        
        removed_count = original_count - len(self.usage_events)
        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old usage events")
    
    async def track_pipeline_creation(self, tenant_id: str, pipeline_id: str):
        """Track pipeline creation event"""
        await self.track_usage(
            tenant_id=tenant_id,
            event_type="pipeline_creation",
            quantity=Decimal("1"),
            pipeline_id=pipeline_id,
            metadata={"action": "create_pipeline"}
        )
    
    async def track_pipeline_execution(self, 
                                     tenant_id: str, 
                                     pipeline_id: str,
                                     execution_result: Dict[str, Any]):
        """Track pipeline execution usage"""
        
        # Track data processing
        if "gb_processed" in execution_result:
            await self.track_usage(
                tenant_id=tenant_id,
                event_type="gb_processed",
                quantity=Decimal(str(execution_result["gb_processed"])),
                pipeline_id=pipeline_id,
                metadata={"execution_id": execution_result.get("execution_id")}
            )
        
        # Track compute time
        if "execution_time_seconds" in execution_result:
            compute_hours = Decimal(str(execution_result["execution_time_seconds"])) / Decimal("3600")
            await self.track_usage(
                tenant_id=tenant_id,
                event_type="compute_hours",
                quantity=compute_hours,
                pipeline_id=pipeline_id,
                metadata={"execution_id": execution_result.get("execution_id")}
            )
        
        # Track quality checks
        if "quality_checks_performed" in execution_result:
            await self.track_usage(
                tenant_id=tenant_id,
                event_type="quality_checks",
                quantity=Decimal(str(execution_result["quality_checks_performed"])),
                pipeline_id=pipeline_id,
                metadata={"execution_id": execution_result.get("execution_id")}
            )
    
    async def track_storage_usage(self, tenant_id: str, storage_gb: Decimal):
        """Track storage usage"""
        await self.track_usage(
            tenant_id=tenant_id,
            event_type="storage_gb",
            quantity=storage_gb,
            metadata={"measurement_type": "current_storage"}
        )
    
    async def get_platform_analytics(self) -> Dict[str, Any]:
        """Get platform-wide usage analytics"""
        
        # Calculate totals across all tenants
        total_usage = defaultdict(Decimal)
        tenant_count = len(self.tenant_usage_cache)
        
        for tenant_usage in self.tenant_usage_cache.values():
            for metric, value in tenant_usage.items():
                total_usage[metric] += value
        
        # Get recent activity (last 24 hours)
        recent_cutoff = datetime.utcnow() - timedelta(hours=24)
        recent_events = [
            event for event in self.usage_events
            if event.timestamp >= recent_cutoff
        ]
        
        recent_usage = defaultdict(Decimal)
        for event in recent_events:
            recent_usage[event.event_type] += event.quantity
        
        # Get top tenants by usage
        tenant_usage_totals = {}
        for tenant_id, usage in self.tenant_usage_cache.items():
            total = sum(usage.values())
            tenant_usage_totals[tenant_id] = total
        
        top_tenants = sorted(
            tenant_usage_totals.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        # Get usage trends (last 7 days)
        daily_usage = defaultdict(lambda: defaultdict(Decimal))
        week_cutoff = datetime.utcnow() - timedelta(days=7)
        
        for event in self.usage_events:
            if event.timestamp >= week_cutoff:
                day_key = event.timestamp.strftime("%Y-%m-%d")
                daily_usage[day_key][event.event_type] += event.quantity
        
        return {
            "platform_totals": {k: str(v) for k, v in total_usage.items()},
            "tenant_count": tenant_count,
            "recent_activity_24h": {k: str(v) for k, v in recent_usage.items()},
            "top_tenants_by_usage": [
                {"tenant_id": tenant_id, "total_usage": str(usage)}
                for tenant_id, usage in top_tenants
            ],
            "daily_trends": {
                day: {k: str(v) for k, v in metrics.items()}
                for day, metrics in daily_usage.items()
            },
            "real_time_metrics": {
                hour: {
                    "timestamp": metrics["timestamp"].isoformat(),
                    "platform_totals": {k: str(v) for k, v in metrics["platform_totals"].items()},
                    "active_tenants": len(metrics["tenant_activity"])
                }
                for hour, metrics in list(self.real_time_metrics.items())[-24:]  # Last 24 hours
            },
            "generated_at": datetime.utcnow().isoformat()
        }
    
    def get_current_tenant_usage(self, tenant_id: str) -> Dict[str, str]:
        """Get current cached usage for tenant"""
        usage = self.tenant_usage_cache.get(tenant_id, {})
        return {k: str(v) for k, v in usage.items()}
    
    async def get_usage_alerts(self) -> List[Dict[str, Any]]:
        """Get usage-based alerts for tenants approaching limits"""
        alerts = []
        
        # Mock tenant limits (would come from billing service in production)
        tenant_limits = {
            "starter": {
                "gb_processed": 100,
                "compute_hours": 50,
                "storage_gb": 500,
                "quality_checks": 10000
            },
            "professional": {
                "gb_processed": 1000,
                "compute_hours": 200,
                "storage_gb": 2000,
                "quality_checks": 50000
            }
        }
        
        for tenant_id, usage in self.tenant_usage_cache.items():
            # Assume starter limits for all tenants (would lookup actual plan)
            limits = tenant_limits["starter"]
            
            for metric, used_amount in usage.items():
                if metric in limits:
                    limit = Decimal(str(limits[metric]))
                    usage_percentage = (used_amount / limit) * 100
                    
                    if usage_percentage > 80:  # Alert at 80% usage
                        alerts.append({
                            "tenant_id": tenant_id,
                            "metric": metric,
                            "used_amount": str(used_amount),
                            "limit": str(limit),
                            "usage_percentage": float(usage_percentage),
                            "severity": "high" if usage_percentage > 95 else "medium",
                            "timestamp": datetime.utcnow().isoformat()
                        })
        
        return alerts