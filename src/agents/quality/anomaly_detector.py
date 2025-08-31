"""
Advanced Anomaly Detection System for Healthcare Data Quality
Multi-method anomaly detection with ML models and statistical analysis
"""

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, sum as spark_sum, avg, stddev, max as spark_max, min as spark_min
from pyspark.sql.functions import abs as spark_abs, percentile_approx, lag, lead, expr
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import json

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.cluster import DBSCAN
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not available, ML-based anomaly detection disabled")

logger = logging.getLogger(__name__)


class AnomalyType(Enum):
    """Types of anomalies detected"""
    STATISTICAL_OUTLIER = "statistical_outlier"
    PATTERN_DEVIATION = "pattern_deviation"
    VOLUME_ANOMALY = "volume_anomaly"
    TEMPORAL_ANOMALY = "temporal_anomaly"
    MULTIVARIATE_ANOMALY = "multivariate_anomaly"
    BUSINESS_RULE_VIOLATION = "business_rule_violation"
    DATA_DRIFT = "data_drift"
    HEALTHCARE_SPECIFIC = "healthcare_specific"


class AnomalySeverity(Enum):
    """Severity levels for anomalies"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AnomalyResult:
    """Represents a detected anomaly"""
    anomaly_id: str
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    field_name: str
    description: str
    score: float
    affected_records: int
    threshold: float
    detection_method: str
    context: Dict[str, Any]
    timestamp: str
    recommendations: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'anomaly_id': self.anomaly_id,
            'anomaly_type': self.anomaly_type.value,
            'severity': self.severity.value,
            'field_name': self.field_name,
            'description': self.description,
            'score': self.score,
            'affected_records': self.affected_records,
            'threshold': self.threshold,
            'detection_method': self.detection_method,
            'context': self.context,
            'timestamp': self.timestamp,
            'recommendations': self.recommendations
        }


class StatisticalAnomalyDetector:
    """Statistical methods for anomaly detection"""
    
    @staticmethod
    def z_score_detection(df: DataFrame, field_name: str, threshold: float = 3.0) -> List[AnomalyResult]:
        """Detect outliers using Z-score method"""
        
        anomalies = []
        
        try:
            # Calculate mean and standard deviation
            stats = df.select(
                avg(col(field_name)).alias('mean'),
                stddev(col(field_name)).alias('stddev'),
                count(col(field_name)).alias('count')
            ).collect()[0]
            
            if stats['stddev'] is None or stats['stddev'] == 0:
                return anomalies  # No variance, no outliers
                
            mean_val = stats['mean']
            std_val = stats['stddev']
            total_count = stats['count']
            
            # Find outliers
            outlier_condition = (
                spark_abs((col(field_name) - mean_val) / std_val) > threshold
            )
            
            outlier_count = df.filter(outlier_condition).count()
            
            if outlier_count > 0:
                outlier_percentage = (outlier_count / total_count) * 100
                
                # Determine severity based on percentage of outliers
                if outlier_percentage > 10:
                    severity = AnomalySeverity.CRITICAL
                elif outlier_percentage > 5:
                    severity = AnomalySeverity.HIGH
                elif outlier_percentage > 1:
                    severity = AnomalySeverity.MEDIUM
                else:
                    severity = AnomalySeverity.LOW
                    
                anomaly = AnomalyResult(
                    anomaly_id=f"zscore_{field_name}_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                    severity=severity,
                    field_name=field_name,
                    description=f"Z-score outliers detected in {field_name} ({outlier_count} records, {outlier_percentage:.1f}%)",
                    score=outlier_percentage / 10.0,  # Normalize to 0-10 scale
                    affected_records=outlier_count,
                    threshold=threshold,
                    detection_method="z_score",
                    context={
                        'mean': float(mean_val),
                        'stddev': float(std_val),
                        'outlier_percentage': outlier_percentage
                    },
                    timestamp=datetime.now().isoformat(),
                    recommendations=[
                        "Investigate extreme values in the data",
                        "Check for data entry errors",
                        "Consider if outliers represent legitimate edge cases"
                    ]
                )
                
                anomalies.append(anomaly)
                
        except Exception as e:
            logger.error(f"Error in Z-score detection for {field_name}: {str(e)}")
            
        return anomalies
        
    @staticmethod
    def iqr_detection(df: DataFrame, field_name: str, multiplier: float = 1.5) -> List[AnomalyResult]:
        """Detect outliers using Interquartile Range (IQR) method"""
        
        anomalies = []
        
        try:
            # Calculate quartiles
            quartiles = df.select(
                percentile_approx(col(field_name), 0.25).alias('q1'),
                percentile_approx(col(field_name), 0.75).alias('q3'),
                count(col(field_name)).alias('count')
            ).collect()[0]
            
            q1 = quartiles['q1']
            q3 = quartiles['q3']
            total_count = quartiles['count']
            
            if q1 is None or q3 is None:
                return anomalies
                
            iqr = q3 - q1
            lower_bound = q1 - (multiplier * iqr)
            upper_bound = q3 + (multiplier * iqr)
            
            # Find outliers
            outlier_condition = (
                (col(field_name) < lower_bound) | (col(field_name) > upper_bound)
            )
            
            outlier_count = df.filter(outlier_condition).count()
            
            if outlier_count > 0:
                outlier_percentage = (outlier_count / total_count) * 100
                
                # Determine severity
                if outlier_percentage > 15:
                    severity = AnomalySeverity.CRITICAL
                elif outlier_percentage > 8:
                    severity = AnomalySeverity.HIGH
                elif outlier_percentage > 3:
                    severity = AnomalySeverity.MEDIUM
                else:
                    severity = AnomalySeverity.LOW
                    
                anomaly = AnomalyResult(
                    anomaly_id=f"iqr_{field_name}_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                    severity=severity,
                    field_name=field_name,
                    description=f"IQR outliers detected in {field_name} ({outlier_count} records, {outlier_percentage:.1f}%)",
                    score=min(10.0, outlier_percentage / 2.0),  # Cap at 10
                    affected_records=outlier_count,
                    threshold=multiplier,
                    detection_method="iqr",
                    context={
                        'q1': float(q1),
                        'q3': float(q3),
                        'iqr': float(iqr),
                        'lower_bound': float(lower_bound),
                        'upper_bound': float(upper_bound),
                        'outlier_percentage': outlier_percentage
                    },
                    timestamp=datetime.now().isoformat(),
                    recommendations=[
                        "Review data distribution and identify outlier causes",
                        "Consider data transformation or normalization",
                        "Validate extreme values with business users"
                    ]
                )
                
                anomalies.append(anomaly)
                
        except Exception as e:
            logger.error(f"Error in IQR detection for {field_name}: {str(e)}")
            
        return anomalies


class TemporalAnomalyDetector:
    """Detect temporal patterns and anomalies"""
    
    @staticmethod
    def volume_change_detection(
        df: DataFrame, 
        timestamp_col: str, 
        threshold: float = 0.5
    ) -> List[AnomalyResult]:
        """Detect unusual volume changes over time"""
        
        anomalies = []
        
        try:
            # Group by hour/day to detect volume changes
            window_spec = Window.partitionBy().orderBy(col(timestamp_col))
            
            # Calculate rolling volume
            volume_df = (df
                        .groupBy(expr(f"date_trunc('hour', {timestamp_col})").alias("time_window"))
                        .count()
                        .withColumnRenamed("count", "volume")
                        .orderBy("time_window"))
            
            # Calculate volume changes
            volume_with_change = (volume_df
                                .withColumn("prev_volume", 
                                          lag("volume").over(Window.orderBy("time_window")))
                                .withColumn("volume_change",
                                          when(col("prev_volume") > 0,
                                               spark_abs(col("volume") - col("prev_volume")) / col("prev_volume"))
                                          .otherwise(0)))
            
            # Find significant volume changes
            anomalous_periods = (volume_with_change
                               .filter(col("volume_change") > threshold)
                               .collect())
            
            for period in anomalous_periods:
                volume_change = period['volume_change']
                current_volume = period['volume']
                prev_volume = period['prev_volume']
                time_window = period['time_window']
                
                # Determine severity based on magnitude of change
                if volume_change > 0.8:
                    severity = AnomalySeverity.CRITICAL
                elif volume_change > 0.6:
                    severity = AnomalySeverity.HIGH
                elif volume_change > 0.4:
                    severity = AnomalySeverity.MEDIUM
                else:
                    severity = AnomalySeverity.LOW
                    
                change_type = "increase" if current_volume > prev_volume else "decrease"
                
                anomaly = AnomalyResult(
                    anomaly_id=f"volume_{time_window}_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.VOLUME_ANOMALY,
                    severity=severity,
                    field_name="record_volume",
                    description=f"Significant volume {change_type} detected at {time_window} ({volume_change:.1%} change)",
                    score=min(10.0, volume_change * 10),
                    affected_records=int(current_volume),
                    threshold=threshold,
                    detection_method="volume_change",
                    context={
                        'time_window': str(time_window),
                        'current_volume': int(current_volume),
                        'previous_volume': int(prev_volume),
                        'change_percentage': float(volume_change),
                        'change_type': change_type
                    },
                    timestamp=datetime.now().isoformat(),
                    recommendations=[
                        f"Investigate cause of volume {change_type}",
                        "Check upstream data sources for issues",
                        "Verify data processing pipeline health"
                    ]
                )
                
                anomalies.append(anomaly)
                
        except Exception as e:
            logger.error(f"Error in volume change detection: {str(e)}")
            
        return anomalies
        
    @staticmethod
    def seasonal_anomaly_detection(
        df: DataFrame,
        timestamp_col: str,
        value_col: str,
        seasonal_period: int = 24  # hours
    ) -> List[AnomalyResult]:
        """Detect seasonal pattern anomalies"""
        
        anomalies = []
        
        try:
            # Extract hour/day patterns
            pattern_df = (df
                         .withColumn("hour", expr(f"hour({timestamp_col})"))
                         .withColumn("day_of_week", expr(f"dayofweek({timestamp_col})"))
                         .groupBy("hour", "day_of_week")
                         .agg(avg(col(value_col)).alias("avg_value"),
                              stddev(col(value_col)).alias("stddev_value"),
                              count(col(value_col)).alias("count_value"))
                         .collect())
            
            # Build expected patterns
            expected_patterns = {}
            for row in pattern_df:
                key = (row['hour'], row['day_of_week'])
                expected_patterns[key] = {
                    'avg': row['avg_value'],
                    'stddev': row['stddev_value'] or 0,
                    'count': row['count_value']
                }
                
            # Find current anomalies
            current_data = (df
                           .withColumn("hour", expr(f"hour({timestamp_col})"))
                           .withColumn("day_of_week", expr(f"dayofweek({timestamp_col})"))
                           .select("hour", "day_of_week", value_col)
                           .collect())
            
            anomaly_count = 0
            total_deviations = 0
            
            for row in current_data:
                key = (row['hour'], row['day_of_week'])
                actual_value = row[value_col]
                
                if key in expected_patterns and actual_value is not None:
                    expected = expected_patterns[key]
                    if expected['stddev'] > 0:
                        z_score = abs(actual_value - expected['avg']) / expected['stddev']
                        if z_score > 2.5:  # Seasonal anomaly threshold
                            anomaly_count += 1
                            total_deviations += z_score
                            
            if anomaly_count > 0:
                avg_deviation = total_deviations / anomaly_count
                anomaly_percentage = (anomaly_count / len(current_data)) * 100
                
                # Determine severity
                if avg_deviation > 5 or anomaly_percentage > 10:
                    severity = AnomalySeverity.HIGH
                elif avg_deviation > 3 or anomaly_percentage > 5:
                    severity = AnomalySeverity.MEDIUM
                else:
                    severity = AnomalySeverity.LOW
                    
                anomaly = AnomalyResult(
                    anomaly_id=f"seasonal_{value_col}_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.TEMPORAL_ANOMALY,
                    severity=severity,
                    field_name=value_col,
                    description=f"Seasonal pattern anomalies detected in {value_col} ({anomaly_count} records, {anomaly_percentage:.1f}%)",
                    score=min(10.0, avg_deviation),
                    affected_records=anomaly_count,
                    threshold=2.5,
                    detection_method="seasonal_analysis",
                    context={
                        'anomaly_count': anomaly_count,
                        'anomaly_percentage': anomaly_percentage,
                        'avg_deviation': avg_deviation,
                        'seasonal_period': seasonal_period
                    },
                    timestamp=datetime.now().isoformat(),
                    recommendations=[
                        "Analyze seasonal patterns and expected variations",
                        "Check for external factors affecting data patterns",
                        "Consider adjusting seasonal baselines"
                    ]
                )
                
                anomalies.append(anomaly)
                
        except Exception as e:
            logger.error(f"Error in seasonal anomaly detection: {str(e)}")
            
        return anomalies


class HealthcareAnomalyDetector:
    """Healthcare-specific anomaly detection"""
    
    @staticmethod
    def claim_amount_anomalies(df: DataFrame) -> List[AnomalyResult]:
        """Detect anomalous claim amounts"""
        
        anomalies = []
        
        try:
            # Analyze by procedure code
            claim_stats = (df
                          .groupBy("procedure_code")
                          .agg(avg("claim_amount").alias("avg_amount"),
                               stddev("claim_amount").alias("stddev_amount"),
                               count("claim_amount").alias("count_claims"),
                               spark_max("claim_amount").alias("max_amount"))
                          .filter(col("count_claims") >= 10)  # Minimum sample size
                          .collect())
            
            high_amount_anomalies = 0
            zero_amount_claims = df.filter(col("claim_amount") == 0).count()
            total_claims = df.count()
            
            for stat in claim_stats:
                procedure_code = stat['procedure_code']
                avg_amount = stat['avg_amount']
                stddev_amount = stat['stddev_amount'] or 0
                max_amount = stat['max_amount']
                
                if stddev_amount > 0:
                    # Find claims that are >3 standard deviations from mean
                    threshold = avg_amount + (3 * stddev_amount)
                    high_claims = df.filter(
                        (col("procedure_code") == procedure_code) & 
                        (col("claim_amount") > threshold)
                    ).count()
                    
                    if high_claims > 0:
                        high_amount_anomalies += high_claims
                        
            # Zero dollar claims
            if zero_amount_claims > 0:
                zero_percentage = (zero_amount_claims / total_claims) * 100
                
                if zero_percentage > 5:  # > 5% zero claims is unusual
                    anomaly = AnomalyResult(
                        anomaly_id=f"zero_claims_{int(datetime.now().timestamp())}",
                        anomaly_type=AnomalyType.HEALTHCARE_SPECIFIC,
                        severity=AnomalySeverity.HIGH,
                        field_name="claim_amount",
                        description=f"High number of zero-dollar claims detected ({zero_amount_claims} claims, {zero_percentage:.1f}%)",
                        score=min(10.0, zero_percentage / 2),
                        affected_records=zero_amount_claims,
                        threshold=5.0,
                        detection_method="zero_claims_analysis",
                        context={
                            'zero_claims_count': zero_amount_claims,
                            'zero_claims_percentage': zero_percentage,
                            'total_claims': total_claims
                        },
                        timestamp=datetime.now().isoformat(),
                        recommendations=[
                            "Review zero-dollar claims for billing errors",
                            "Check for missing claim amount data",
                            "Investigate processing rules for zero amounts"
                        ]
                    )
                    
                    anomalies.append(anomaly)
                    
            # High amount anomalies
            if high_amount_anomalies > 0:
                high_percentage = (high_amount_anomalies / total_claims) * 100
                
                if high_percentage > 1:  # > 1% high-value claims is unusual
                    anomaly = AnomalyResult(
                        anomaly_id=f"high_claims_{int(datetime.now().timestamp())}",
                        anomaly_type=AnomalyType.HEALTHCARE_SPECIFIC,
                        severity=AnomalySeverity.MEDIUM,
                        field_name="claim_amount",
                        description=f"Unusually high claim amounts detected ({high_amount_anomalies} claims, {high_percentage:.1f}%)",
                        score=min(10.0, high_percentage * 2),
                        affected_records=high_amount_anomalies,
                        threshold=3.0,
                        detection_method="high_claims_analysis",
                        context={
                            'high_claims_count': high_amount_anomalies,
                            'high_claims_percentage': high_percentage,
                            'total_claims': total_claims
                        },
                        timestamp=datetime.now().isoformat(),
                        recommendations=[
                            "Review high-value claims for accuracy",
                            "Check for data entry errors or fraud",
                            "Validate against procedure-specific benchmarks"
                        ]
                    )
                    
                    anomalies.append(anomaly)
                    
        except Exception as e:
            logger.error(f"Error in claim amount anomaly detection: {str(e)}")
            
        return anomalies
        
    @staticmethod
    def utilization_anomalies(df: DataFrame) -> List[AnomalyResult]:
        """Detect unusual healthcare utilization patterns"""
        
        anomalies = []
        
        try:
            # Analyze member utilization patterns
            member_utilization = (df
                                .groupBy("member_id")
                                .agg(count("claim_id").alias("claim_count"),
                                     spark_sum("claim_amount").alias("total_amount"),
                                     expr("count(distinct provider_npi)").alias("provider_count"),
                                     expr("count(distinct procedure_code)").alias("procedure_count"))
                                .collect())
            
            excessive_utilization = 0
            provider_shopping = 0
            high_cost_members = 0
            
            for member in member_utilization:
                member_id = member['member_id']
                claim_count = member['claim_count']
                total_amount = member['total_amount']
                provider_count = member['provider_count']
                procedure_count = member['procedure_count']
                
                # Excessive utilization (>50 claims per month)
                if claim_count > 50:
                    excessive_utilization += 1
                    
                # Provider shopping (>10 different providers)
                if provider_count > 10:
                    provider_shopping += 1
                    
                # High cost members (>$50,000 total)
                if total_amount > 50000:
                    high_cost_members += 1
                    
            total_members = len(member_utilization)
            
            # Create anomalies for significant patterns
            if excessive_utilization > 0:
                utilization_percentage = (excessive_utilization / total_members) * 100
                
                if utilization_percentage > 2:  # > 2% excessive utilization
                    anomaly = AnomalyResult(
                        anomaly_id=f"excessive_utilization_{int(datetime.now().timestamp())}",
                        anomaly_type=AnomalyType.HEALTHCARE_SPECIFIC,
                        severity=AnomalySeverity.HIGH,
                        field_name="member_utilization",
                        description=f"Excessive utilization detected ({excessive_utilization} members, {utilization_percentage:.1f}%)",
                        score=min(10.0, utilization_percentage),
                        affected_records=excessive_utilization,
                        threshold=50,
                        detection_method="utilization_analysis",
                        context={
                            'excessive_utilization_count': excessive_utilization,
                            'utilization_percentage': utilization_percentage,
                            'total_members': total_members,
                            'threshold_claims': 50
                        },
                        timestamp=datetime.now().isoformat(),
                        recommendations=[
                            "Review members with excessive utilization",
                            "Check for potential fraud or abuse",
                            "Investigate medical necessity"
                        ]
                    )
                    
                    anomalies.append(anomaly)
                    
            if provider_shopping > 0:
                shopping_percentage = (provider_shopping / total_members) * 100
                
                if shopping_percentage > 1:  # > 1% provider shopping
                    anomaly = AnomalyResult(
                        anomaly_id=f"provider_shopping_{int(datetime.now().timestamp())}",
                        anomaly_type=AnomalyType.HEALTHCARE_SPECIFIC,
                        severity=AnomalySeverity.MEDIUM,
                        field_name="provider_utilization",
                        description=f"Provider shopping behavior detected ({provider_shopping} members, {shopping_percentage:.1f}%)",
                        score=min(10.0, shopping_percentage * 2),
                        affected_records=provider_shopping,
                        threshold=10,
                        detection_method="provider_shopping_analysis",
                        context={
                            'provider_shopping_count': provider_shopping,
                            'shopping_percentage': shopping_percentage,
                            'total_members': total_members,
                            'threshold_providers': 10
                        },
                        timestamp=datetime.now().isoformat(),
                        recommendations=[
                            "Analyze provider shopping patterns",
                            "Check for coordination of benefits issues",
                            "Review for potential fraud indicators"
                        ]
                    )
                    
                    anomalies.append(anomaly)
                    
        except Exception as e:
            logger.error(f"Error in utilization anomaly detection: {str(e)}")
            
        return anomalies


class MLAnomalyDetector:
    """Machine Learning based anomaly detection"""
    
    def __init__(self):
        self.models = {}
        self.scalers = {}
        
    def isolation_forest_detection(
        self, 
        df: DataFrame, 
        numeric_fields: List[str],
        contamination: float = 0.1
    ) -> List[AnomalyResult]:
        """Use Isolation Forest for multivariate anomaly detection"""
        
        anomalies = []
        
        if not SKLEARN_AVAILABLE:
            logger.warning("scikit-learn not available, skipping Isolation Forest detection")
            return anomalies
            
        try:
            # Convert to pandas for sklearn processing
            pandas_df = df.select(*numeric_fields).toPandas()
            
            # Remove null values
            pandas_df = pandas_df.dropna()
            
            if len(pandas_df) < 100:  # Need minimum samples
                logger.warning("Insufficient data for Isolation Forest (< 100 samples)")
                return anomalies
                
            # Scale the features
            scaler = StandardScaler()
            scaled_data = scaler.fit_transform(pandas_df)
            
            # Fit Isolation Forest
            iso_forest = IsolationForest(
                contamination=contamination,
                random_state=42,
                n_estimators=100
            )
            
            # Predict anomalies (-1 for anomalies, 1 for normal)
            anomaly_labels = iso_forest.fit_predict(scaled_data)
            anomaly_scores = iso_forest.decision_function(scaled_data)
            
            # Count anomalies
            anomaly_count = np.sum(anomaly_labels == -1)
            total_count = len(pandas_df)
            anomaly_percentage = (anomaly_count / total_count) * 100
            
            if anomaly_count > 0:
                # Determine severity
                if anomaly_percentage > 15:
                    severity = AnomalySeverity.CRITICAL
                elif anomaly_percentage > 10:
                    severity = AnomalySeverity.HIGH
                elif anomaly_percentage > 5:
                    severity = AnomalySeverity.MEDIUM
                else:
                    severity = AnomalySeverity.LOW
                    
                anomaly = AnomalyResult(
                    anomaly_id=f"isolation_forest_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.MULTIVARIATE_ANOMALY,
                    severity=severity,
                    field_name=",".join(numeric_fields),
                    description=f"Multivariate anomalies detected using Isolation Forest ({anomaly_count} records, {anomaly_percentage:.1f}%)",
                    score=min(10.0, anomaly_percentage / 2),
                    affected_records=anomaly_count,
                    threshold=contamination,
                    detection_method="isolation_forest",
                    context={
                        'contamination': contamination,
                        'anomaly_count': anomaly_count,
                        'anomaly_percentage': anomaly_percentage,
                        'total_records': total_count,
                        'features': numeric_fields,
                        'avg_anomaly_score': float(np.mean(anomaly_scores[anomaly_labels == -1]))
                    },
                    timestamp=datetime.now().isoformat(),
                    recommendations=[
                        "Investigate records with high anomaly scores",
                        "Check for data quality issues in multiple fields",
                        "Consider if anomalies represent valid edge cases"
                    ]
                )
                
                anomalies.append(anomaly)
                
        except Exception as e:
            logger.error(f"Error in Isolation Forest detection: {str(e)}")
            
        return anomalies
        
    def dbscan_anomaly_detection(
        self,
        df: DataFrame,
        numeric_fields: List[str],
        eps: float = 0.5,
        min_samples: int = 5
    ) -> List[AnomalyResult]:
        """Use DBSCAN for density-based anomaly detection"""
        
        anomalies = []
        
        if not SKLEARN_AVAILABLE:
            return anomalies
            
        try:
            # Convert to pandas
            pandas_df = df.select(*numeric_fields).toPandas()
            pandas_df = pandas_df.dropna()
            
            if len(pandas_df) < 100:
                return anomalies
                
            # Scale features
            scaler = StandardScaler()
            scaled_data = scaler.fit_transform(pandas_df)
            
            # Apply DBSCAN
            dbscan = DBSCAN(eps=eps, min_samples=min_samples)
            cluster_labels = dbscan.fit_predict(scaled_data)
            
            # Points labeled as -1 are anomalies
            anomaly_count = np.sum(cluster_labels == -1)
            total_count = len(pandas_df)
            anomaly_percentage = (anomaly_count / total_count) * 100
            
            if anomaly_count > 0:
                severity = AnomalySeverity.MEDIUM if anomaly_percentage > 5 else AnomalySeverity.LOW
                
                anomaly = AnomalyResult(
                    anomaly_id=f"dbscan_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.MULTIVARIATE_ANOMALY,
                    severity=severity,
                    field_name=",".join(numeric_fields),
                    description=f"Density-based anomalies detected using DBSCAN ({anomaly_count} records, {anomaly_percentage:.1f}%)",
                    score=min(10.0, anomaly_percentage / 3),
                    affected_records=anomaly_count,
                    threshold=eps,
                    detection_method="dbscan",
                    context={
                        'eps': eps,
                        'min_samples': min_samples,
                        'anomaly_count': anomaly_count,
                        'anomaly_percentage': anomaly_percentage,
                        'total_records': total_count,
                        'features': numeric_fields,
                        'n_clusters': len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
                    },
                    timestamp=datetime.now().isoformat(),
                    recommendations=[
                        "Review density-based anomalies for patterns",
                        "Check if anomalies form meaningful clusters",
                        "Consider adjusting clustering parameters"
                    ]
                )
                
                anomalies.append(anomaly)
                
        except Exception as e:
            logger.error(f"Error in DBSCAN anomaly detection: {str(e)}")
            
        return anomalies


class AnomalyDetectionEngine:
    """Main anomaly detection engine coordinating all detection methods"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.statistical_detector = StatisticalAnomalyDetector()
        self.temporal_detector = TemporalAnomalyDetector()
        self.healthcare_detector = HealthcareAnomalyDetector()
        self.ml_detector = MLAnomalyDetector() if SKLEARN_AVAILABLE else None
        
    def detect_all_anomalies(
        self, 
        df: DataFrame, 
        table_name: str,
        timestamp_col: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run comprehensive anomaly detection across all methods
        
        Args:
            df: DataFrame to analyze
            table_name: Name of the table
            timestamp_col: Timestamp column for temporal analysis
            
        Returns:
            Dictionary containing all detected anomalies
        """
        
        logger.info(f"Starting comprehensive anomaly detection for {table_name}")
        
        all_anomalies = []
        detection_summary = {
            'table_name': table_name,
            'timestamp': datetime.now().isoformat(),
            'total_records': df.count(),
            'detection_methods_used': [],
            'anomalies_by_type': {},
            'anomalies_by_severity': {},
            'total_anomalies': 0,
            'anomaly_details': []
        }
        
        # Get numeric and string fields
        numeric_fields = [field.name for field in df.schema.fields 
                         if str(field.dataType) in ['int', 'bigint', 'float', 'double', 'decimal']]
        string_fields = [field.name for field in df.schema.fields 
                        if str(field.dataType) == 'string']
        
        try:
            # Statistical anomaly detection
            for field in numeric_fields:
                # Z-score detection
                z_score_anomalies = self.statistical_detector.z_score_detection(df, field)
                all_anomalies.extend(z_score_anomalies)
                
                # IQR detection
                iqr_anomalies = self.statistical_detector.iqr_detection(df, field)
                all_anomalies.extend(iqr_anomalies)
                
            detection_summary['detection_methods_used'].append('statistical')
            
            # Temporal anomaly detection
            if timestamp_col and timestamp_col in [field.name for field in df.schema.fields]:
                volume_anomalies = self.temporal_detector.volume_change_detection(df, timestamp_col)
                all_anomalies.extend(volume_anomalies)
                
                # Seasonal analysis for numeric fields
                for field in numeric_fields[:3]:  # Limit to first 3 fields to avoid timeout
                    seasonal_anomalies = self.temporal_detector.seasonal_anomaly_detection(
                        df, timestamp_col, field
                    )
                    all_anomalies.extend(seasonal_anomalies)
                    
                detection_summary['detection_methods_used'].append('temporal')
                
            # Healthcare-specific detection
            if self._is_healthcare_table(table_name, df.schema.fieldNames()):
                claim_anomalies = self.healthcare_detector.claim_amount_anomalies(df)
                all_anomalies.extend(claim_anomalies)
                
                utilization_anomalies = self.healthcare_detector.utilization_anomalies(df)
                all_anomalies.extend(utilization_anomalies)
                
                detection_summary['detection_methods_used'].append('healthcare_specific')
                
            # ML-based detection
            if self.ml_detector and len(numeric_fields) >= 2:
                isolation_anomalies = self.ml_detector.isolation_forest_detection(
                    df, numeric_fields[:5]  # Limit fields for performance
                )
                all_anomalies.extend(isolation_anomalies)
                
                dbscan_anomalies = self.ml_detector.dbscan_anomaly_detection(
                    df, numeric_fields[:3]  # Even fewer fields for DBSCAN
                )
                all_anomalies.extend(dbscan_anomalies)
                
                detection_summary['detection_methods_used'].append('machine_learning')
                
        except Exception as e:
            logger.error(f"Error in anomaly detection: {str(e)}")
            
        # Summarize results
        detection_summary['total_anomalies'] = len(all_anomalies)
        detection_summary['anomaly_details'] = [anomaly.to_dict() for anomaly in all_anomalies]
        
        # Group by type and severity
        for anomaly in all_anomalies:
            anomaly_type = anomaly.anomaly_type.value
            severity = anomaly.severity.value
            
            if anomaly_type not in detection_summary['anomalies_by_type']:
                detection_summary['anomalies_by_type'][anomaly_type] = 0
            detection_summary['anomalies_by_type'][anomaly_type] += 1
            
            if severity not in detection_summary['anomalies_by_severity']:
                detection_summary['anomalies_by_severity'][severity] = 0
            detection_summary['anomalies_by_severity'][severity] += 1
            
        logger.info(f"Anomaly detection completed. Found {len(all_anomalies)} anomalies")
        
        return detection_summary
        
    def _is_healthcare_table(self, table_name: str, field_names: List[str]) -> bool:
        """Check if table contains healthcare data"""
        
        healthcare_indicators = [
            'member_id', 'patient_id', 'claim_id', 'provider_npi', 'diagnosis_code',
            'procedure_code', 'icd', 'cpt', 'hcpcs', 'claim_amount'
        ]
        
        # Check table name
        if any(indicator in table_name.lower() for indicator in ['claim', 'member', 'patient', 'provider']):
            return True
            
        # Check field names
        healthcare_field_count = sum(
            1 for field in field_names 
            if any(indicator in field.lower() for indicator in healthcare_indicators)
        )
        
        return healthcare_field_count >= 3  # If 3+ healthcare fields, likely healthcare table
        
    def get_anomaly_report(self, detection_results: Dict[str, Any]) -> str:
        """Generate formatted anomaly detection report"""
        
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("ANOMALY DETECTION REPORT")
        report_lines.append("=" * 60)
        report_lines.append(f"Table: {detection_results['table_name']}")
        report_lines.append(f"Timestamp: {detection_results['timestamp']}")
        report_lines.append(f"Total Records: {detection_results['total_records']:,}")
        report_lines.append(f"Detection Methods: {', '.join(detection_results['detection_methods_used'])}")
        report_lines.append("")
        
        # Summary
        report_lines.append("SUMMARY")
        report_lines.append("-" * 20)
        report_lines.append(f"Total Anomalies Found: {detection_results['total_anomalies']}")
        
        # By severity
        if detection_results['anomalies_by_severity']:
            report_lines.append("\nBy Severity:")
            for severity, count in detection_results['anomalies_by_severity'].items():
                report_lines.append(f"  {severity.title()}: {count}")
                
        # By type
        if detection_results['anomalies_by_type']:
            report_lines.append("\nBy Type:")
            for anomaly_type, count in detection_results['anomalies_by_type'].items():
                report_lines.append(f"  {anomaly_type.replace('_', ' ').title()}: {count}")
                
        # Detailed anomalies
        if detection_results['anomaly_details']:
            report_lines.append("\n" + "=" * 60)
            report_lines.append("DETAILED ANOMALIES")
            report_lines.append("=" * 60)
            
            for i, anomaly in enumerate(detection_results['anomaly_details'][:20], 1):  # Show top 20
                report_lines.append(f"\n{i}. {anomaly['description']}")
                report_lines.append(f"   Field: {anomaly['field_name']}")
                report_lines.append(f"   Severity: {anomaly['severity'].title()}")
                report_lines.append(f"   Score: {anomaly['score']:.2f}")
                report_lines.append(f"   Affected Records: {anomaly['affected_records']:,}")
                report_lines.append(f"   Method: {anomaly['detection_method']}")
                
                if anomaly['recommendations']:
                    report_lines.append("   Recommendations:")
                    for rec in anomaly['recommendations']:
                        report_lines.append(f"     • {rec}")
                        
        return "\n".join(report_lines)


# Usage example
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("AnomalyDetectionTest").getOrCreate()
    
    config = {}
    
    # Create anomaly detection engine
    anomaly_engine = AnomalyDetectionEngine(spark, config)
    
    # Sample healthcare data
    sample_data = [
        ("M123456789", "1234567890", 125.50, "2024-01-15", "Z23.1", "99213"),
        ("M987654321", "0987654321", 200.75, "2024-01-16", "I10", "99214"),
        ("M555666777", "1111111111", 0.00, "2024-01-17", "J44.1", "99215"),    # Zero amount anomaly
        ("M444555666", "2222222222", 50000.00, "2024-01-18", "C80.1", "99205"), # High amount anomaly
    ]
    
    columns = ["member_id", "provider_npi", "claim_amount", "service_date", "diagnosis_code", "procedure_code"]
    sample_df = spark.createDataFrame(sample_data, columns)
    
    # Run anomaly detection
    results = anomaly_engine.detect_all_anomalies(
        sample_df, 
        "healthcare_claims",
        timestamp_col="service_date"
    )
    
    # Generate report
    report = anomaly_engine.get_anomaly_report(results)
    print(report)
    
    spark.stop()