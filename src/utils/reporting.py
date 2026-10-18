"""
Pipeline operational reporting module.
Collects stage metrics, execution timestamps, and writes machine-readable run reports.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional
from config.settings import REPORTS_DIR

class PipelineReporter:
    """Tracks pipeline metrics, stages, and generates execution reports."""

    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        self.start_time = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.status = "INITIALIZED"
        self.stages: Dict[str, str] = {}
        self.metrics: Dict[str, Any] = {
            "input_records": 0,
            "bronze_records": 0,
            "silver_records": 0,
            "rejected_records": 0,
            "duplicate_records": 0,
            "gold_daily_records": 0,
            "gold_product_records": 0,
            "gold_regional_records": 0,
            "gold_category_records": 0,
            "gold_payment_records": 0,
            "mysql_facts_loaded": 0,
        }
        self.rejection_breakdown: Dict[str, int] = {}
        self.output_locations: Dict[str, str] = {}
        self.error_message: Optional[str] = None

    def record_stage(self, stage_name: str, stage_status: str):
        """Record the outcome status of a pipeline stage."""
        self.stages[stage_name] = stage_status

    def record_metric(self, key: str, value: Any):
        """Update a specific numeric or metadata metric."""
        self.metrics[key] = value

    def set_rejection_breakdown(self, breakdown: Dict[str, int]):
        """Record counts for specific rejection rules."""
        self.rejection_breakdown = breakdown

    def set_output_location(self, key: str, path: str):
        """Record destination paths for datasets."""
        self.output_locations[key] = path

    def finalize(self, status: str = "SUCCESS", error_message: Optional[str] = None) -> Dict[str, Any]:
        """Finalize timing, status, and write reports/latest_run.json."""
        self.end_time = datetime.now(timezone.utc)
        self.status = status
        self.error_message = error_message

        duration = (self.end_time - self.start_time).total_seconds()

        report_data = {
            "run_id": self.run_id,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": round(duration, 3),
            "stages": self.stages,
            "metrics": self.metrics,
            "rejection_breakdown": self.rejection_breakdown,
            "output_locations": self.output_locations,
            "error_message": self.error_message
        }

        # Write latest run report
        latest_report_path = REPORTS_DIR / "latest_run.json"
        with open(latest_report_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2)

        # Also write historical run report
        historical_path = REPORTS_DIR / f"{self.run_id}.json"
        with open(historical_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2)

        return report_data
