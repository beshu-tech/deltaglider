"""CloudWatch metrics adapter for production metrics collection."""

import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from ..ports.metrics import MetricsPort

logger = logging.getLogger(__name__)

# Constants for byte conversions
BYTES_PER_KB = 1024
BYTES_PER_MB = 1024 * 1024
BYTES_PER_GB = 1024 * 1024 * 1024


class CloudWatchMetricsAdapter(MetricsPort):
    """CloudWatch implementation of MetricsPort for AWS-native metrics."""

    def __init__(
        self,
        namespace: str = "DeltaGlider",
        region: str | None = None,
        endpoint_url: str | None = None,
    ):
        """Initialize CloudWatch metrics adapter.

        Args:
            namespace: CloudWatch namespace for metrics
            region: AWS region (uses default if None)
            endpoint_url: Override endpoint for testing
        """
        self.namespace = namespace
        try:
            self.client = boto3.client(
                "cloudwatch",
                region_name=region,
                endpoint_url=endpoint_url,
            )
            self.enabled = True
        except Exception as e:
            logger.warning(f"CloudWatch metrics disabled: {e}")
            self.enabled = False
            self.client = None

    def increment(self, name: str, value: int = 1, tags: dict[str, str] | None = None) -> None:
        """Increment a counter metric.

        Args:
            name: Metric name
            value: Increment value
            tags: Optional tags/dimensions
        """
        if not self.enabled:
            return

        try:
            dimensions = self._tags_to_dimensions(tags)
            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[
                    {
                        "MetricName": name,
                        "Value": value,
                        "Unit": "Count",
                        "Timestamp": datetime.utcnow(),
                        "Dimensions": dimensions,
                    }
                ],
            )
        except ClientError as e:
            logger.debug(f"Failed to send metric {name}: {e}")

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Set a gauge metric value.

        Args:
            name: Metric name
            value: Gauge value
            tags: Optional tags/dimensions
        """
        if not self.enabled:
            return

        try:
            dimensions = self._tags_to_dimensions(tags)

            # Determine unit based on metric name
            unit = self._infer_unit(name, value)

            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[
                    {
                        "MetricName": name,
                        "Value": value,
                        "Unit": unit,
                        "Timestamp": datetime.utcnow(),
                        "Dimensions": dimensions,
                    }
                ],
            )
        except ClientError as e:
            logger.debug(f"Failed to send gauge {name}: {e}")

    def timing(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Record a timing metric.

        Args:
            name: Metric name
            value: Time in milliseconds
            tags: Optional tags/dimensions
        """
        if not self.enabled:
            return

        try:
            dimensions = self._tags_to_dimensions(tags)
            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[
                    {
                        "MetricName": name,
                        "Value": value,
                        "Unit": "Milliseconds",
                        "Timestamp": datetime.utcnow(),
                        "Dimensions": dimensions,
                    }
                ],
            )
        except ClientError as e:
            logger.debug(f"Failed to send timing {name}: {e}")

    def _tags_to_dimensions(self, tags: dict[str, str] | None) -> list[dict[str, str]]:
        """Convert tags dict to CloudWatch dimensions format.

        Args:
            tags: Tags dictionary

        Returns:
            List of dimension dicts for CloudWatch
        """
        if not tags:
            return []

        return [
            {"Name": key, "Value": str(value)}
            for key, value in tags.items()
            if key and value  # Skip empty keys/values
        ][:10]  # CloudWatch limit is 10 dimensions

    def _infer_unit(self, name: str, value: float) -> str:
        """Infer CloudWatch unit from metric name.

        Args:
            name: Metric name
            value: Metric value

        Returns:
            CloudWatch unit string
        """
        name_lower = name.lower()

        # Size metrics
        if any(x in name_lower for x in ["size", "bytes"]):
            if value > BYTES_PER_GB:  # > 1GB
                return "Gigabytes"
            elif value > BYTES_PER_MB:  # > 1MB
                return "Megabytes"
            elif value > BYTES_PER_KB:  # > 1KB
                return "Kilobytes"
            return "Bytes"

        # Time metrics
        if any(x in name_lower for x in ["time", "duration", "latency"]):
            if value > 1000:  # > 1 second
                return "Seconds"
            return "Milliseconds"

        # Percentage metrics
        if any(x in name_lower for x in ["ratio", "percent", "rate"]):
            return "Percent"

        # Count metrics
        if any(x in name_lower for x in ["count", "total", "number"]):
            return "Count"

        # Default to None (no unit)
        return "None"


class LoggingMetricsAdapter(MetricsPort):
    """Simple logging-based metrics adapter for development/debugging."""

    def __init__(self, log_level: str = "INFO"):
        """Initialize logging metrics adapter.

        Args:
            log_level: Logging level for metrics
        """
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)

    def increment(self, name: str, value: int = 1, tags: dict[str, str] | None = None) -> None:
        """Log counter increment."""
        logger.log(self.log_level, f"METRIC:INCREMENT {name}={value} tags={tags or {}}")

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Log gauge value."""
        logger.log(self.log_level, f"METRIC:GAUGE {name}={value:.2f} tags={tags or {}}")

    def timing(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Log timing value."""
        logger.log(self.log_level, f"METRIC:TIMING {name}={value:.2f}ms tags={tags or {}}")
