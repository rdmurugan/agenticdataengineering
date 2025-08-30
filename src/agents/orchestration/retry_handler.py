"""
Intelligent Retry Handler with exponential backoff and failure analysis
Implements self-healing retry logic for pipeline failures
"""

import time
import random
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Retry strategy types"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_INTERVAL = "fixed_interval"
    ADAPTIVE = "adaptive"


class FailureCategory(Enum):
    """Categories of failures for targeted retry strategies"""
    TRANSIENT_NETWORK = "transient_network"
    RESOURCE_CONSTRAINT = "resource_constraint" 
    SCHEMA_DRIFT = "schema_drift"
    DATA_QUALITY = "data_quality"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_retries: int = 3
    base_delay: int = 60  # seconds
    max_delay: int = 1800  # 30 minutes
    jitter: bool = True
    backoff_multiplier: float = 2.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF


@dataclass
class FailureEvent:
    """Represents a failure event"""
    timestamp: datetime
    error_message: str
    error_type: str
    category: FailureCategory
    context: Dict[str, Any]
    retry_count: int = 0
    resolved: bool = False


class RetryHandler:
    """
    Intelligent retry handler with failure analysis and adaptive strategies
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.failure_history: List[FailureEvent] = []
        self.retry_patterns = self._initialize_retry_patterns()
        
    def _initialize_retry_patterns(self) -> Dict[FailureCategory, RetryConfig]:
        """Initialize retry patterns for different failure categories"""
        
        return {
            FailureCategory.TRANSIENT_NETWORK: RetryConfig(
                max_retries=5,
                base_delay=30,
                max_delay=300,
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF
            ),
            FailureCategory.RESOURCE_CONSTRAINT: RetryConfig(
                max_retries=3,
                base_delay=120,
                max_delay=1800,
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
                backoff_multiplier=1.5
            ),
            FailureCategory.SCHEMA_DRIFT: RetryConfig(
                max_retries=1,  # Schema issues need manual intervention
                base_delay=300,
                max_delay=300,
                strategy=RetryStrategy.FIXED_INTERVAL
            ),
            FailureCategory.DATA_QUALITY: RetryConfig(
                max_retries=2,
                base_delay=180,
                max_delay=900,
                strategy=RetryStrategy.LINEAR_BACKOFF
            ),
            FailureCategory.CONFIGURATION: RetryConfig(
                max_retries=1,  # Config issues need fixing
                base_delay=60,
                max_delay=60,
                strategy=RetryStrategy.FIXED_INTERVAL
            ),
            FailureCategory.UNKNOWN: RetryConfig(
                max_retries=3,
                base_delay=90,
                max_delay=720,
                strategy=RetryStrategy.ADAPTIVE
            )
        }
        
    def analyze_failure(self, error_message: str, context: Dict[str, Any]) -> FailureEvent:
        """
        Analyze failure to categorize and determine retry strategy
        
        Args:
            error_message: Error message from failure
            context: Additional context about the failure
            
        Returns:
            FailureEvent with categorized failure information
        """
        
        category = self._categorize_failure(error_message, context)
        
        failure_event = FailureEvent(
            timestamp=datetime.now(),
            error_message=error_message,
            error_type=context.get("error_type", "unknown"),
            category=category,
            context=context
        )
        
        self.failure_history.append(failure_event)
        
        logger.info(f"Categorized failure as {category.value}: {error_message[:100]}...")
        
        return failure_event
        
    def _categorize_failure(self, error_message: str, context: Dict[str, Any]) -> FailureCategory:
        """Categorize failure based on error message and context"""
        
        error_lower = error_message.lower()
        
        # Network/connectivity issues
        if any(keyword in error_lower for keyword in [
            "connection", "timeout", "network", "unreachable", "socket", 
            "dns", "ssl", "certificate", "handshake"
        ]):
            return FailureCategory.TRANSIENT_NETWORK
            
        # Resource constraints
        if any(keyword in error_lower for keyword in [
            "memory", "heap", "out of memory", "disk space", "cpu", 
            "resource", "quota", "limit exceeded", "capacity"
        ]):
            return FailureCategory.RESOURCE_CONSTRAINT
            
        # Schema-related issues
        if any(keyword in error_lower for keyword in [
            "schema", "column", "field", "type mismatch", "parse error",
            "serialization", "deserialization", "json", "avro", "parquet"
        ]):
            return FailureCategory.SCHEMA_DRIFT
            
        # Data quality issues
        if any(keyword in error_lower for keyword in [
            "quality", "validation", "constraint", "expectation", "null",
            "duplicate", "invalid", "format", "range", "integrity"
        ]):
            return FailureCategory.DATA_QUALITY
            
        # Configuration issues
        if any(keyword in error_lower for keyword in [
            "config", "permission", "access", "credential", "authentication",
            "authorization", "key", "token", "missing parameter"
        ]):
            return FailureCategory.CONFIGURATION
            
        return FailureCategory.UNKNOWN
        
    def should_retry(self, failure_event: FailureEvent) -> bool:
        """
        Determine if a failure should be retried based on failure analysis
        
        Args:
            failure_event: The failure to analyze
            
        Returns:
            Boolean indicating whether to retry
        """
        
        retry_config = self.retry_patterns[failure_event.category]
        
        # Check if we've exceeded max retries
        if failure_event.retry_count >= retry_config.max_retries:
            logger.info(f"Max retries ({retry_config.max_retries}) exceeded for {failure_event.category.value}")
            return False
            
        # Check for patterns that shouldn't be retried
        if self._is_permanent_failure(failure_event):
            logger.info(f"Permanent failure detected, not retrying: {failure_event.error_message[:50]}...")
            return False
            
        # Check recent failure patterns
        if self._has_repeated_failures(failure_event):
            logger.warning(f"Repeated failures detected for {failure_event.category.value}, adjusting strategy")
            return self._should_retry_repeated_failure(failure_event)
            
        return True
        
    def _is_permanent_failure(self, failure_event: FailureEvent) -> bool:
        """Check if this is a permanent failure that shouldn't be retried"""
        
        permanent_patterns = [
            "file not found",
            "table does not exist",
            "invalid credentials",
            "access denied",
            "syntax error",
            "illegal argument"
        ]
        
        error_lower = failure_event.error_message.lower()
        return any(pattern in error_lower for pattern in permanent_patterns)
        
    def _has_repeated_failures(self, current_failure: FailureEvent) -> bool:
        """Check for pattern of repeated failures"""
        
        recent_failures = [
            f for f in self.failure_history[-10:]  # Last 10 failures
            if f.category == current_failure.category
            and f.timestamp > datetime.now() - timedelta(hours=1)
        ]
        
        return len(recent_failures) >= 3
        
    def _should_retry_repeated_failure(self, failure_event: FailureEvent) -> bool:
        """Determine retry strategy for repeated failures"""
        
        if failure_event.category in [FailureCategory.SCHEMA_DRIFT, FailureCategory.CONFIGURATION]:
            # These typically need human intervention
            return False
            
        if failure_event.category == FailureCategory.RESOURCE_CONSTRAINT:
            # For resource issues, retry with longer delays
            return failure_event.retry_count < 2
            
        # For other categories, use standard retry logic
        return failure_event.retry_count < self.retry_patterns[failure_event.category].max_retries
        
    def calculate_delay(self, failure_event: FailureEvent) -> int:
        """
        Calculate delay before next retry based on strategy
        
        Args:
            failure_event: The failure event
            
        Returns:
            Delay in seconds before next retry
        """
        
        retry_config = self.retry_patterns[failure_event.category]
        
        if retry_config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = retry_config.base_delay * (retry_config.backoff_multiplier ** failure_event.retry_count)
        elif retry_config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = retry_config.base_delay * (1 + failure_event.retry_count)
        elif retry_config.strategy == RetryStrategy.FIXED_INTERVAL:
            delay = retry_config.base_delay
        else:  # ADAPTIVE
            delay = self._calculate_adaptive_delay(failure_event, retry_config)
            
        # Cap at max delay
        delay = min(delay, retry_config.max_delay)
        
        # Add jitter to prevent thundering herd
        if retry_config.jitter:
            jitter_range = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_range, jitter_range)
            
        return int(max(1, delay))
        
    def _calculate_adaptive_delay(self, failure_event: FailureEvent, retry_config: RetryConfig) -> int:
        """Calculate adaptive delay based on failure patterns"""
        
        # Get recent failures of same category
        recent_failures = [
            f for f in self.failure_history[-20:]
            if f.category == failure_event.category
            and f.timestamp > datetime.now() - timedelta(hours=2)
        ]
        
        if len(recent_failures) > 5:
            # Frequent failures - increase delay
            multiplier = 1.5 + (len(recent_failures) - 5) * 0.2
        else:
            # Infrequent failures - use standard backoff
            multiplier = retry_config.backoff_multiplier ** failure_event.retry_count
            
        return int(retry_config.base_delay * multiplier)
        
    def retry_with_backoff(
        self,
        operation: Callable,
        operation_name: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute operation with intelligent retry logic
        
        Args:
            operation: Function to execute
            operation_name: Name of operation for logging
            context: Additional context for failure analysis
            
        Returns:
            Result of operation or error information
        """
        
        context = context or {}
        
        for attempt in range(1, 6):  # Maximum 5 attempts total
            try:
                logger.info(f"Executing {operation_name} (attempt {attempt})")
                result = operation()
                
                # Mark any previous failures as resolved
                self._mark_failures_resolved(operation_name)
                
                return {
                    "success": True,
                    "result": result,
                    "attempts": attempt,
                    "operation": operation_name
                }
                
            except Exception as e:
                error_message = str(e)
                logger.warning(f"Attempt {attempt} failed for {operation_name}: {error_message}")
                
                # Analyze failure
                failure_context = {**context, "operation": operation_name, "attempt": attempt}
                failure_event = self.analyze_failure(error_message, failure_context)
                failure_event.retry_count = attempt - 1
                
                # Determine if we should retry
                if not self.should_retry(failure_event):
                    logger.error(f"Not retrying {operation_name} after {attempt} attempts")
                    return {
                        "success": False,
                        "error": error_message,
                        "attempts": attempt,
                        "operation": operation_name,
                        "failure_category": failure_event.category.value,
                        "permanent_failure": True
                    }
                    
                # Calculate delay for next attempt
                if attempt < 5:  # Don't delay after last attempt
                    delay = self.calculate_delay(failure_event)
                    logger.info(f"Retrying {operation_name} in {delay} seconds...")
                    time.sleep(delay)
                    
        # All attempts failed
        return {
            "success": False,
            "error": f"All retry attempts exhausted for {operation_name}",
            "attempts": 5,
            "operation": operation_name,
            "failure_category": failure_event.category.value if 'failure_event' in locals() else "unknown"
        }
        
    def _mark_failures_resolved(self, operation_name: str):
        """Mark previous failures for this operation as resolved"""
        
        for failure in self.failure_history:
            if (failure.context.get("operation") == operation_name and 
                not failure.resolved and
                failure.timestamp > datetime.now() - timedelta(hours=1)):
                failure.resolved = True
                
    def get_failure_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get failure statistics for monitoring
        
        Args:
            hours: Time window for statistics
            
        Returns:
            Dictionary with failure statistics
        """
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_failures = [f for f in self.failure_history if f.timestamp > cutoff_time]
        
        if not recent_failures:
            return {
                "total_failures": 0,
                "period_hours": hours,
                "categories": {},
                "resolution_rate": 100.0
            }
            
        # Count by category
        category_counts = {}
        resolved_count = 0
        
        for failure in recent_failures:
            category = failure.category.value
            category_counts[category] = category_counts.get(category, 0) + 1
            if failure.resolved:
                resolved_count += 1
                
        # Calculate resolution rate
        resolution_rate = (resolved_count / len(recent_failures)) * 100
        
        # Most common failures
        most_common_category = max(category_counts.items(), key=lambda x: x[1])[0] if category_counts else None
        
        return {
            "total_failures": len(recent_failures),
            "period_hours": hours,
            "categories": category_counts,
            "resolution_rate": round(resolution_rate, 1),
            "most_common_category": most_common_category,
            "avg_retries_per_failure": round(
                sum(f.retry_count for f in recent_failures) / len(recent_failures), 1
            )
        }
        
    def get_retry_recommendations(self) -> List[Dict[str, Any]]:
        """Get recommendations for improving retry strategies"""
        
        recommendations = []
        stats = self.get_failure_statistics(hours=168)  # 1 week
        
        # Analyze patterns and suggest improvements
        if stats["resolution_rate"] < 70:
            recommendations.append({
                "type": "resolution_rate",
                "priority": "high",
                "message": f"Low resolution rate ({stats['resolution_rate']}%). Consider investigating root causes.",
                "action": "Review failure logs and improve error handling"
            })
            
        if stats.get("most_common_category") == "resource_constraint":
            recommendations.append({
                "type": "resource_scaling",
                "priority": "medium", 
                "message": "Frequent resource constraint failures detected",
                "action": "Consider implementing auto-scaling or using larger cluster sizes"
            })
            
        if stats.get("avg_retries_per_failure", 0) > 2:
            recommendations.append({
                "type": "retry_optimization",
                "priority": "medium",
                "message": "High average retry count suggests retry strategy tuning needed",
                "action": "Review and optimize retry configurations"
            })
            
        return recommendations
        
    def reset_failure_history(self, older_than_hours: int = 168):
        """Reset failure history older than specified hours (default 1 week)"""
        
        cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
        self.failure_history = [f for f in self.failure_history if f.timestamp > cutoff_time]
        
        logger.info(f"Reset failure history older than {older_than_hours} hours")