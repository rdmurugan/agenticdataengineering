"""
Ingestion Agent - Auto Loader with schema drift detection for Medicaid/Medicare feeds
"""

from .auto_loader import AutoLoaderAgent
from .schema_drift_detector import SchemaDriftDetector

__all__ = ["AutoLoaderAgent", "SchemaDriftDetector"]