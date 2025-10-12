"""EC2 Instance Metadata Service (IMDS) adapter.

Provides access to EC2 instance metadata using IMDSv2 with token-based authentication.
Falls back gracefully when not running on EC2.
"""

import os

import requests


class EC2MetadataAdapter:
    """Adapter for EC2 Instance Metadata Service (IMDSv2)."""

    IMDS_BASE_URL = "http://169.254.169.254/latest"
    TOKEN_URL = f"{IMDS_BASE_URL}/api/token"
    TOKEN_TTL_SECONDS = 21600  # 6 hours
    TOKEN_HEADER = "X-aws-ec2-metadata-token"
    TIMEOUT_SECONDS = 1  # Fast timeout for non-EC2 environments

    def __init__(self) -> None:
        """Initialize EC2 metadata adapter."""
        self._token: str | None = None
        self._is_ec2: bool | None = None
        self._region: str | None = None

    def is_running_on_ec2(self) -> bool:
        """Check if running on an EC2 instance.

        Returns:
            True if running on EC2, False otherwise

        Note:
            Result is cached after first check for performance.
        """
        if self._is_ec2 is not None:
            return self._is_ec2

        # Skip check if explicitly disabled
        if os.environ.get("DG_DISABLE_EC2_DETECTION", "").lower() in ("true", "1", "yes"):
            self._is_ec2 = False
            return False

        try:
            # Try to get IMDSv2 token
            self._token = self._get_token()
            self._is_ec2 = self._token is not None
        except Exception:
            self._is_ec2 = False

        return self._is_ec2

    def get_region(self) -> str | None:
        """Get the EC2 instance's AWS region.

        Returns:
            AWS region code (e.g., "us-east-1") or None if not on EC2

        Note:
            Result is cached after first successful fetch.
        """
        if not self.is_running_on_ec2():
            return None

        if self._region is not None:
            return self._region

        try:
            if self._token:
                response = requests.get(
                    f"{self.IMDS_BASE_URL}/meta-data/placement/region",
                    headers={self.TOKEN_HEADER: self._token},
                    timeout=self.TIMEOUT_SECONDS,
                )
                if response.status_code == 200:
                    self._region = response.text.strip()
                    return self._region
        except Exception:
            pass

        return None

    def get_availability_zone(self) -> str | None:
        """Get the EC2 instance's availability zone.

        Returns:
            Availability zone (e.g., "us-east-1a") or None if not on EC2
        """
        if not self.is_running_on_ec2():
            return None

        try:
            if self._token:
                response = requests.get(
                    f"{self.IMDS_BASE_URL}/meta-data/placement/availability-zone",
                    headers={self.TOKEN_HEADER: self._token},
                    timeout=self.TIMEOUT_SECONDS,
                )
                if response.status_code == 200:
                    return str(response.text.strip())
        except Exception:
            pass

        return None

    def _get_token(self) -> str | None:
        """Get IMDSv2 token for authenticated metadata requests.

        Returns:
            IMDSv2 token or None if unable to retrieve

        Note:
            Uses IMDSv2 for security. IMDSv1 is not supported.
        """
        try:
            response = requests.put(
                self.TOKEN_URL,
                headers={"X-aws-ec2-metadata-token-ttl-seconds": str(self.TOKEN_TTL_SECONDS)},
                timeout=self.TIMEOUT_SECONDS,
            )
            if response.status_code == 200:
                return response.text.strip()
        except Exception:
            pass

        return None
