"""
Advanced Quality Reporting and Alerting System
Comprehensive reporting framework with automated alerts, notifications, and executive dashboards
"""

import smtplib
import json
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from email.mime.base import MimeBase
from email import encoders
import requests
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import yaml
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

logger = logging.getLogger(__name__)


class ReportType(Enum):
    """Types of quality reports"""
    EXECUTIVE_SUMMARY = "executive_summary"
    DETAILED_QUALITY = "detailed_quality"
    COMPLIANCE_REPORT = "compliance_report"
    TREND_ANALYSIS = "trend_analysis"
    ANOMALY_REPORT = "anomaly_report"
    FIELD_PROFILING = "field_profiling"
    CUSTOM_REPORT = "custom_report"


class ReportFormat(Enum):
    """Report output formats"""
    HTML = "html"
    PDF = "pdf"
    JSON = "json"
    EXCEL = "excel"
    CSV = "csv"


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass
class QualityAlert:
    """Quality alert definition"""
    alert_id: str
    level: AlertLevel
    title: str
    description: str
    table_name: str
    field_name: Optional[str]
    threshold_breached: Optional[str]
    current_value: Optional[float]
    threshold_value: Optional[float]
    affected_records: int
    timestamp: str
    context: Dict[str, Any]
    recommendations: List[str]
    auto_resolved: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass 
class ReportConfig:
    """Report configuration"""
    report_type: ReportType
    output_format: ReportFormat
    include_charts: bool = True
    include_recommendations: bool = True
    include_raw_data: bool = False
    chart_style: str = "plotly"
    max_records: int = 10000
    date_range_days: int = 30


class QualityReporter:
    """
    Advanced quality reporting and alerting system
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.quality_config = self._load_quality_config()
        self.notification_config = self.config.get('notifications', {})
        
        # Initialize plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def _load_quality_config(self) -> Dict[str, Any]:
        """Load quality configuration"""
        try:
            config_path = self.config.get('quality_config_path', 'config/data_quality_config.yaml')
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning("Quality configuration file not found")
            return {}
            
    def generate_executive_summary(
        self, 
        quality_results: Dict[str, Any],
        report_config: ReportConfig
    ) -> Dict[str, Any]:
        """Generate executive summary report"""
        
        logger.info("Generating executive summary report")
        
        # Extract key metrics
        overall_score = quality_results.get('overall_score', 0)
        dimension_scores = quality_results.get('dimension_scores', {})
        total_records = quality_results.get('record_count', 0)
        issues = quality_results.get('issues', [])
        
        # Calculate trends (mock data for demo)
        score_trend = self._calculate_score_trend(quality_results)
        issue_trend = self._calculate_issue_trend(issues)
        
        # Create summary report
        summary_report = {
            'report_type': 'Executive Summary',
            'generated_at': datetime.now().isoformat(),
            'period': f"Last {report_config.date_range_days} days",
            'executive_metrics': {
                'overall_quality_score': {
                    'current': overall_score,
                    'target': 95.0,
                    'trend': score_trend,
                    'status': 'good' if overall_score >= 90 else 'warning' if overall_score >= 80 else 'critical'
                },
                'data_completeness': {
                    'current': dimension_scores.get('completeness', 0),
                    'target': 95.0,
                    'trend': 'up' if dimension_scores.get('completeness', 0) > 90 else 'stable'
                },
                'data_validity': {
                    'current': dimension_scores.get('validity', 0),
                    'target': 98.0,
                    'trend': 'stable'
                },
                'critical_issues': {
                    'count': len([i for i in issues if i.get('severity') == 'critical']),
                    'trend': issue_trend,
                    'resolution_time': '4.2 hours'  # Mock data
                },
                'total_records_processed': {
                    'count': total_records,
                    'trend': 'up',
                    'growth_rate': '5.2%'  # Mock data
                }
            },
            'key_achievements': [
                f"Overall quality score of {overall_score:.1f}% maintained",
                f"Processed {total_records:,} records with high reliability",
                f"Auto-resolved {len([i for i in issues if i.get('auto_resolved', False)])} quality issues"
            ],
            'areas_of_concern': [
                issue.get('description', 'Unknown issue') 
                for issue in issues[:3] 
                if issue.get('severity') == 'critical'
            ],
            'recommendations': quality_results.get('recommendations', [])[:5],
            'charts': []
        }
        
        # Generate charts if requested
        if report_config.include_charts:
            summary_report['charts'] = self._generate_executive_charts(quality_results)
            
        return summary_report
        
    def generate_detailed_quality_report(
        self,
        quality_results: Dict[str, Any],
        report_config: ReportConfig
    ) -> Dict[str, Any]:
        """Generate comprehensive detailed quality report"""
        
        logger.info("Generating detailed quality report")
        
        detailed_report = {
            'report_type': 'Detailed Quality Report',
            'generated_at': datetime.now().isoformat(),
            'table_name': quality_results.get('table_name', 'Unknown'),
            'analysis_period': f"Last {report_config.date_range_days} days",
            'data_overview': {
                'total_records': quality_results.get('record_count', 0),
                'total_fields': quality_results.get('field_count', 0),
                'assessment_timestamp': quality_results.get('timestamp'),
                'overall_quality_score': quality_results.get('overall_score', 0)
            },
            'quality_dimensions': self._analyze_quality_dimensions(quality_results),
            'field_analysis': self._analyze_field_quality(quality_results),
            'business_rules': self._analyze_business_rules(quality_results),
            'data_profiling': self._generate_data_profiling_summary(quality_results),
            'anomaly_detection': self._analyze_anomalies(quality_results),
            'trend_analysis': self._generate_trend_analysis(quality_results),
            'recommendations': self._categorize_recommendations(quality_results.get('recommendations', [])),
            'charts': [],
            'raw_data': {}
        }
        
        # Generate detailed charts
        if report_config.include_charts:
            detailed_report['charts'] = self._generate_detailed_charts(quality_results)
            
        # Include raw data if requested
        if report_config.include_raw_data:
            detailed_report['raw_data'] = quality_results
            
        return detailed_report
        
    def generate_compliance_report(
        self,
        quality_results: Dict[str, Any],
        report_config: ReportConfig
    ) -> Dict[str, Any]:
        """Generate healthcare compliance report"""
        
        logger.info("Generating compliance report")
        
        # Healthcare-specific compliance checks
        compliance_checks = {
            'hipaa_compliance': {
                'phi_protection': self._check_phi_protection(quality_results),
                'access_controls': self._check_access_controls(quality_results),
                'audit_trails': self._check_audit_trails(quality_results)
            },
            'cms_compliance': {
                'data_quality_standards': self._check_cms_quality_standards(quality_results),
                'reporting_requirements': self._check_reporting_requirements(quality_results),
                'coding_accuracy': self._check_coding_accuracy(quality_results)
            },
            'state_medicaid_compliance': {
                'member_eligibility': self._check_member_eligibility(quality_results),
                'provider_validation': self._check_provider_validation(quality_results),
                'claim_validation': self._check_claim_validation(quality_results)
            }
        }
        
        # Calculate overall compliance score
        compliance_score = self._calculate_compliance_score(compliance_checks)
        
        compliance_report = {
            'report_type': 'Compliance Report',
            'generated_at': datetime.now().isoformat(),
            'compliance_period': f"Last {report_config.date_range_days} days",
            'overall_compliance_score': compliance_score,
            'compliance_status': 'compliant' if compliance_score >= 95 else 'non_compliant',
            'compliance_checks': compliance_checks,
            'violations': self._identify_compliance_violations(compliance_checks),
            'remediation_plan': self._generate_remediation_plan(compliance_checks),
            'regulatory_requirements': self._list_regulatory_requirements(),
            'certification_status': 'current',  # Would check actual certification
            'next_audit_date': (datetime.now() + timedelta(days=90)).isoformat(),
            'charts': []
        }
        
        if report_config.include_charts:
            compliance_report['charts'] = self._generate_compliance_charts(compliance_checks)
            
        return compliance_report
        
    def generate_anomaly_report(
        self,
        anomaly_results: Dict[str, Any],
        report_config: ReportConfig
    ) -> Dict[str, Any]:
        """Generate anomaly detection report"""
        
        logger.info("Generating anomaly detection report")
        
        anomalies = anomaly_results.get('anomaly_details', [])
        
        # Categorize anomalies
        anomalies_by_type = {}
        anomalies_by_severity = {}
        
        for anomaly in anomalies:
            anom_type = anomaly.get('anomaly_type', 'unknown')
            severity = anomaly.get('severity', 'low')
            
            if anom_type not in anomalies_by_type:
                anomalies_by_type[anom_type] = []
            anomalies_by_type[anom_type].append(anomaly)
            
            if severity not in anomalies_by_severity:
                anomalies_by_severity[severity] = []
            anomalies_by_severity[severity].append(anomaly)
            
        # Analyze patterns
        pattern_analysis = self._analyze_anomaly_patterns(anomalies)
        
        anomaly_report = {
            'report_type': 'Anomaly Detection Report',
            'generated_at': datetime.now().isoformat(),
            'analysis_period': f"Last {report_config.date_range_days} days",
            'table_name': anomaly_results.get('table_name', 'Unknown'),
            'detection_summary': {
                'total_anomalies': len(anomalies),
                'detection_methods_used': anomaly_results.get('detection_methods_used', []),
                'total_records_analyzed': anomaly_results.get('total_records', 0),
                'anomaly_rate': (len(anomalies) / max(1, anomaly_results.get('total_records', 1))) * 100
            },
            'anomalies_by_type': {k: len(v) for k, v in anomalies_by_type.items()},
            'anomalies_by_severity': {k: len(v) for k, v in anomalies_by_severity.items()},
            'critical_anomalies': [a for a in anomalies if a.get('severity') == 'critical'][:10],
            'pattern_analysis': pattern_analysis,
            'impact_assessment': self._assess_anomaly_impact(anomalies),
            'recommendations': self._generate_anomaly_recommendations(anomalies),
            'charts': []
        }
        
        if report_config.include_charts:
            anomaly_report['charts'] = self._generate_anomaly_charts(anomaly_results)
            
        return anomaly_report
        
    def create_quality_alert(
        self,
        table_name: str,
        alert_data: Dict[str, Any],
        alert_rules: Dict[str, Any]
    ) -> Optional[QualityAlert]:
        """Create a quality alert based on assessment results"""
        
        overall_score = alert_data.get('overall_score', 100)
        dimension_scores = alert_data.get('dimension_scores', {})
        issues = alert_data.get('issues', [])
        
        # Check alert conditions
        alert_triggered = False
        alert_level = AlertLevel.INFO
        alert_description = ""
        recommendations = []
        
        # Critical quality score
        if overall_score < self.quality_config.get('global_thresholds', {}).get('critical_quality_score', 60) * 100:
            alert_triggered = True
            alert_level = AlertLevel.CRITICAL
            alert_description = f"Critical quality score: {overall_score:.1f}%"
            recommendations.extend([
                "Immediate investigation required",
                "Review data sources and processing pipeline",
                "Implement emergency data quality measures"
            ])
            
        # Warning quality score
        elif overall_score < self.quality_config.get('global_thresholds', {}).get('warning_quality_score', 80) * 100:
            alert_triggered = True
            alert_level = AlertLevel.WARNING
            alert_description = f"Quality score below warning threshold: {overall_score:.1f}%"
            recommendations.extend([
                "Review quality processes",
                "Investigate recent changes",
                "Consider additional validation rules"
            ])
            
        # Critical issues
        critical_issues = [i for i in issues if i.get('severity') == 'critical']
        if critical_issues and alert_level != AlertLevel.CRITICAL:
            alert_triggered = True
            alert_level = AlertLevel.CRITICAL
            alert_description = f"Critical data quality issues detected: {len(critical_issues)} issues"
            recommendations.extend([
                "Address critical issues immediately",
                "Review affected data",
                "Implement corrective measures"
            ])
            
        if alert_triggered:
            alert = QualityAlert(
                alert_id=f"quality_alert_{table_name}_{int(datetime.now().timestamp())}",
                level=alert_level,
                title=f"Data Quality Alert: {table_name}",
                description=alert_description,
                table_name=table_name,
                field_name=None,
                threshold_breached="overall_quality_score",
                current_value=overall_score,
                threshold_value=self.quality_config.get('global_thresholds', {}).get('warning_quality_score', 80) * 100,
                affected_records=alert_data.get('record_count', 0),
                timestamp=datetime.now().isoformat(),
                context={
                    'dimension_scores': dimension_scores,
                    'critical_issues_count': len(critical_issues),
                    'total_issues_count': len(issues)
                },
                recommendations=recommendations
            )
            
            return alert
            
        return None
        
    def send_alert_notification(self, alert: QualityAlert):
        """Send alert notification via configured channels"""
        
        logger.info(f"Sending alert notification: {alert.alert_id}")
        
        # Email notification
        if self.notification_config.get('email', {}).get('enabled', False):
            try:
                self._send_email_alert(alert)
                logger.info("Email alert sent successfully")
            except Exception as e:
                logger.error(f"Failed to send email alert: {str(e)}")
                
        # Slack notification
        if self.notification_config.get('slack', {}).get('enabled', False):
            try:
                self._send_slack_alert(alert)
                logger.info("Slack alert sent successfully")
            except Exception as e:
                logger.error(f"Failed to send Slack alert: {str(e)}")
                
        # PagerDuty notification for critical alerts
        if (alert.level == AlertLevel.CRITICAL and 
            self.notification_config.get('pagerduty', {}).get('enabled', False)):
            try:
                self._send_pagerduty_alert(alert)
                logger.info("PagerDuty alert sent successfully")
            except Exception as e:
                logger.error(f"Failed to send PagerDuty alert: {str(e)}")
                
    def _send_email_alert(self, alert: QualityAlert):
        """Send email alert notification"""
        
        email_config = self.notification_config.get('email', {})
        
        # Create message
        msg = MimeMultipart()
        msg['From'] = email_config.get('sender', 'noreply@healthcare-platform.com')
        msg['To'] = ', '.join(email_config.get('recipients', []))
        msg['Subject'] = f"[{alert.level.value.upper()}] {alert.title}"
        
        # Create HTML body
        html_body = f"""
        <html>
        <body>
        <h2 style="color: {'red' if alert.level == AlertLevel.CRITICAL else 'orange' if alert.level == AlertLevel.WARNING else 'blue'}">
            {alert.title}
        </h2>
        
        <p><strong>Alert Level:</strong> {alert.level.value.upper()}</p>
        <p><strong>Table:</strong> {alert.table_name}</p>
        <p><strong>Description:</strong> {alert.description}</p>
        <p><strong>Timestamp:</strong> {alert.timestamp}</p>
        <p><strong>Affected Records:</strong> {alert.affected_records:,}</p>
        
        {f'<p><strong>Current Value:</strong> {alert.current_value:.2f}</p>' if alert.current_value else ''}
        {f'<p><strong>Threshold:</strong> {alert.threshold_value:.2f}</p>' if alert.threshold_value else ''}
        
        <h3>Recommendations:</h3>
        <ul>
        {''.join(f'<li>{rec}</li>' for rec in alert.recommendations)}
        </ul>
        
        <h3>Context:</h3>
        <pre>{json.dumps(alert.context, indent=2)}</pre>
        
        <hr>
        <p><em>This alert was generated automatically by the Healthcare Data Quality Platform</em></p>
        </body>
        </html>
        """
        
        msg.attach(MimeText(html_body, 'html'))
        
        # Send email
        server = smtplib.SMTP(email_config.get('smtp_server'), email_config.get('smtp_port', 587))
        server.starttls()
        if email_config.get('username') and email_config.get('password'):
            server.login(email_config.get('username'), email_config.get('password'))
        
        text = msg.as_string()
        server.sendmail(msg['From'], email_config.get('recipients', []), text)
        server.quit()
        
    def _send_slack_alert(self, alert: QualityAlert):
        """Send Slack alert notification"""
        
        slack_config = self.notification_config.get('slack', {})
        webhook_url = slack_config.get('webhook_url')
        
        if not webhook_url:
            raise ValueError("Slack webhook URL not configured")
            
        # Color coding by alert level
        colors = {
            AlertLevel.INFO: "#36a64f",      # Green
            AlertLevel.WARNING: "#ff9500",   # Orange  
            AlertLevel.CRITICAL: "#ff0000",  # Red
            AlertLevel.EMERGENCY: "#8B0000"  # Dark Red
        }
        
        # Create Slack message
        slack_message = {
            "text": f"Data Quality Alert: {alert.table_name}",
            "attachments": [
                {
                    "color": colors.get(alert.level, "#36a64f"),
                    "title": alert.title,
                    "text": alert.description,
                    "fields": [
                        {
                            "title": "Alert Level",
                            "value": alert.level.value.upper(),
                            "short": True
                        },
                        {
                            "title": "Table",
                            "value": alert.table_name,
                            "short": True
                        },
                        {
                            "title": "Affected Records",
                            "value": f"{alert.affected_records:,}",
                            "short": True
                        },
                        {
                            "title": "Timestamp",
                            "value": alert.timestamp,
                            "short": True
                        }
                    ],
                    "footer": "Healthcare Data Quality Platform",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }
        
        # Add recommendations
        if alert.recommendations:
            rec_text = "\n".join(f"• {rec}" for rec in alert.recommendations[:5])
            slack_message["attachments"][0]["fields"].append({
                "title": "Recommendations",
                "value": rec_text,
                "short": False
            })
            
        # Send to Slack
        response = requests.post(webhook_url, json=slack_message)
        response.raise_for_status()
        
    def _send_pagerduty_alert(self, alert: QualityAlert):
        """Send PagerDuty alert for critical issues"""
        
        pagerduty_config = self.notification_config.get('pagerduty', {})
        
        # PagerDuty Events API payload
        payload = {
            "routing_key": pagerduty_config.get('routing_key'),
            "event_action": "trigger",
            "dedup_key": alert.alert_id,
            "payload": {
                "summary": f"{alert.title}: {alert.description}",
                "severity": "critical",
                "source": "healthcare-data-platform",
                "component": alert.table_name,
                "group": "data-quality",
                "class": "data-quality-alert",
                "custom_details": {
                    "alert_id": alert.alert_id,
                    "table_name": alert.table_name,
                    "affected_records": alert.affected_records,
                    "recommendations": alert.recommendations,
                    "context": alert.context
                }
            }
        }
        
        # Send to PagerDuty
        response = requests.post(
            "https://events.pagerduty.com/v2/enqueue",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        
    def export_report(
        self, 
        report_data: Dict[str, Any], 
        format: ReportFormat, 
        filename: str
    ) -> str:
        """Export report in specified format"""
        
        if format == ReportFormat.JSON:
            return self._export_json_report(report_data, filename)
        elif format == ReportFormat.HTML:
            return self._export_html_report(report_data, filename)
        elif format == ReportFormat.PDF:
            return self._export_pdf_report(report_data, filename)
        elif format == ReportFormat.EXCEL:
            return self._export_excel_report(report_data, filename)
        elif format == ReportFormat.CSV:
            return self._export_csv_report(report_data, filename)
        else:
            raise ValueError(f"Unsupported report format: {format}")
            
    def _export_json_report(self, report_data: Dict[str, Any], filename: str) -> str:
        """Export report as JSON"""
        
        with open(filename, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
            
        return filename
        
    def _export_html_report(self, report_data: Dict[str, Any], filename: str) -> str:
        """Export report as HTML"""
        
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; }}
                .metric {{ display: inline-block; margin: 10px; padding: 15px; background: white; border-left: 4px solid #007bff; }}
                .critical {{ border-left-color: #dc3545; }}
                .warning {{ border-left-color: #ffc107; }}
                .success {{ border-left-color: #28a745; }}
                .chart {{ margin: 20px 0; text-align: center; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f8f9fa; }}
            </style>
        </head>
        <body>
        {content}
        </body>
        </html>
        """
        
        # Build content
        content_parts = []
        
        # Header
        content_parts.append(f"""
        <div class="header">
            <h1>{report_data.get('report_type', 'Data Quality Report')}</h1>
            <p><strong>Generated:</strong> {report_data.get('generated_at', 'Unknown')}</p>
            <p><strong>Period:</strong> {report_data.get('period', report_data.get('analysis_period', 'Unknown'))}</p>
        </div>
        """)
        
        # Executive metrics
        if 'executive_metrics' in report_data:
            content_parts.append("<h2>Executive Metrics</h2>")
            for metric_name, metric_data in report_data['executive_metrics'].items():
                status_class = metric_data.get('status', 'success')
                current_val = metric_data.get('current', 0)
                target_val = metric_data.get('target', 100)
                
                content_parts.append(f"""
                <div class="metric {status_class}">
                    <h4>{metric_name.replace('_', ' ').title()}</h4>
                    <p>Current: <strong>{current_val:.1f}</strong></p>
                    <p>Target: {target_val:.1f}</p>
                    <p>Trend: {metric_data.get('trend', 'stable')}</p>
                </div>
                """)
                
        # Charts (embedded as base64 images)
        if report_data.get('charts'):
            content_parts.append("<h2>Visualizations</h2>")
            for i, chart_data in enumerate(report_data['charts']):
                content_parts.append(f"""
                <div class="chart">
                    <h3>{chart_data.get('title', f'Chart {i+1}')}</h3>
                    <img src="data:image/png;base64,{chart_data.get('image_data', '')}" style="max-width: 100%;" />
                </div>
                """)
                
        # Recommendations
        if report_data.get('recommendations'):
            content_parts.append("<h2>Recommendations</h2><ul>")
            for rec in report_data['recommendations']:
                content_parts.append(f"<li>{rec}</li>")
            content_parts.append("</ul>")
            
        html_content = html_template.format(
            title=report_data.get('report_type', 'Data Quality Report'),
            content=''.join(content_parts)
        )
        
        with open(filename, 'w') as f:
            f.write(html_content)
            
        return filename
        
    # Helper methods for analysis and chart generation
    def _calculate_score_trend(self, quality_results: Dict[str, Any]) -> str:
        """Calculate quality score trend"""
        # Mock implementation - in production would query historical data
        return "stable"
        
    def _calculate_issue_trend(self, issues: List[Dict[str, Any]]) -> str:
        """Calculate issue count trend"""
        # Mock implementation
        return "down" if len(issues) < 5 else "stable"
        
    def _generate_executive_charts(self, quality_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate charts for executive summary"""
        
        charts = []
        
        try:
            # Quality score gauge
            overall_score = quality_results.get('overall_score', 0)
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=overall_score,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "Overall Quality Score"},
                delta={'reference': 90},
                gauge={
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 60], 'color': "red"},
                        {'range': [60, 80], 'color': "yellow"},
                        {'range': [80, 95], 'color': "lightgreen"},
                        {'range': [95, 100], 'color': "green"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
            
            # Convert to base64 image
            img_bytes = pio.to_image(fig, format="png", width=800, height=400)
            img_b64 = base64.b64encode(img_bytes).decode()
            
            charts.append({
                'title': 'Overall Quality Score',
                'type': 'gauge',
                'image_data': img_b64
            })
            
        except Exception as e:
            logger.error(f"Error generating executive charts: {str(e)}")
            
        return charts
        
    # Additional helper methods would continue here...
    
    def _analyze_quality_dimensions(self, quality_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze quality dimensions"""
        return quality_results.get('dimension_scores', {})
        
    def _analyze_field_quality(self, quality_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze field-level quality"""
        return quality_results.get('field_results', {})
        
    # Mock implementations for healthcare compliance checks
    def _check_phi_protection(self, quality_results: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "compliant", "score": 98.5, "issues": []}
        
    def _check_access_controls(self, quality_results: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "compliant", "score": 95.0, "issues": []}
        
    def _check_audit_trails(self, quality_results: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "compliant", "score": 100.0, "issues": []}
        
    def _check_cms_quality_standards(self, quality_results: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "compliant", "score": 92.5, "issues": []}
        
    def _calculate_compliance_score(self, compliance_checks: Dict[str, Any]) -> float:
        """Calculate overall compliance score"""
        all_scores = []
        
        for category, checks in compliance_checks.items():
            for check_name, check_result in checks.items():
                if isinstance(check_result, dict) and 'score' in check_result:
                    all_scores.append(check_result['score'])
                    
        return sum(all_scores) / len(all_scores) if all_scores else 0.0
        

@dataclass
class ScheduledReport:
    """Scheduled report configuration"""
    name: str
    report_type: ReportType
    schedule: str  # Cron expression
    recipients: List[str]
    format: ReportFormat
    enabled: bool = True
    last_run: Optional[str] = None
    next_run: Optional[str] = None


class ReportScheduler:
    """Scheduler for automated quality reports"""
    
    def __init__(self, reporter: QualityReporter, config: Dict[str, Any]):
        self.reporter = reporter
        self.config = config
        self.scheduled_reports = self._load_scheduled_reports()
        
    def _load_scheduled_reports(self) -> Dict[str, ScheduledReport]:
        """Load scheduled report configurations"""
        
        scheduled_reports = {}
        
        # Load from config
        reports_config = self.config.get('scheduled_reports', {})
        
        for report_name, report_config in reports_config.items():
            scheduled_report = ScheduledReport(
                name=report_name,
                report_type=ReportType(report_config.get('type', 'executive_summary')),
                schedule=report_config.get('schedule', '0 8 * * *'),
                recipients=report_config.get('recipients', []),
                format=ReportFormat(report_config.get('format', 'html')),
                enabled=report_config.get('enabled', True)
            )
            
            scheduled_reports[report_name] = scheduled_report
            
        return scheduled_reports
        
    def trigger_scheduled_report(self, report_name: str, quality_data: Dict[str, Any]):
        """Trigger a scheduled report manually"""
        
        if report_name not in self.scheduled_reports:
            raise ValueError(f"Scheduled report not found: {report_name}")
            
        scheduled_report = self.scheduled_reports[report_name]
        
        # Generate report based on type
        report_config = ReportConfig(
            report_type=scheduled_report.report_type,
            output_format=scheduled_report.format,
            include_charts=True,
            include_recommendations=True
        )
        
        if scheduled_report.report_type == ReportType.EXECUTIVE_SUMMARY:
            report_data = self.reporter.generate_executive_summary(quality_data, report_config)
        elif scheduled_report.report_type == ReportType.DETAILED_QUALITY:
            report_data = self.reporter.generate_detailed_quality_report(quality_data, report_config)
        elif scheduled_report.report_type == ReportType.COMPLIANCE_REPORT:
            report_data = self.reporter.generate_compliance_report(quality_data, report_config)
        else:
            raise ValueError(f"Unsupported report type: {scheduled_report.report_type}")
            
        # Export and send report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{report_name}_{timestamp}.{scheduled_report.format.value}"
        
        exported_file = self.reporter.export_report(report_data, scheduled_report.format, filename)
        
        # Send to recipients (implementation would depend on delivery method)
        logger.info(f"Generated scheduled report: {exported_file}")
        
        # Update last run timestamp
        scheduled_report.last_run = datetime.now().isoformat()
        
        return exported_file


# Usage example
if __name__ == "__main__":
    
    # Sample configuration
    config = {
        'notifications': {
            'email': {
                'enabled': True,
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'sender': 'alerts@healthcare-platform.com',
                'recipients': ['data-team@healthcare.com']
            },
            'slack': {
                'enabled': True,
                'webhook_url': 'https://hooks.slack.com/services/...'
            }
        }
    }
    
    # Initialize reporter
    reporter = QualityReporter(config)
    
    # Sample quality results
    quality_results = {
        'table_name': 'medicaid_claims',
        'overall_score': 87.5,
        'dimension_scores': {
            'completeness': 92.1,
            'validity': 85.3,
            'consistency': 88.7
        },
        'record_count': 125000,
        'issues': [
            {
                'severity': 'warning',
                'description': 'NPI format issues in provider data'
            }
        ],
        'recommendations': [
            'Implement NPI validation rules',
            'Review provider data sources'
        ]
    }
    
    # Generate executive summary
    report_config = ReportConfig(
        report_type=ReportType.EXECUTIVE_SUMMARY,
        output_format=ReportFormat.HTML
    )
    
    summary_report = reporter.generate_executive_summary(quality_results, report_config)
    
    # Export report
    exported_file = reporter.export_report(summary_report, ReportFormat.HTML, 'quality_summary.html')
    print(f"Report exported: {exported_file}")
    
    # Create and send alert
    alert = reporter.create_quality_alert('medicaid_claims', quality_results, {})
    if alert:
        reporter.send_alert_notification(alert)
        print(f"Alert sent: {alert.alert_id}")