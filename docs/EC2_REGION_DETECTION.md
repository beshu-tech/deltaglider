# EC2 Region Detection & Cost Optimization

DeltaGlider automatically detects when you're running on an EC2 instance and warns you about potential cross-region data transfer charges.

## Overview

When running `deltaglider migrate` on an EC2 instance, DeltaGlider:

1. **Detects EC2 Environment**: Uses IMDSv2 (Instance Metadata Service v2) to determine if running on EC2
2. **Retrieves Instance Region**: Gets the actual AWS region where your EC2 instance is running
3. **Compares Regions**: Checks if your EC2 region matches the S3 client region
4. **Warns About Costs**: Displays clear warnings when regions don't match

## Why This Matters

**AWS Cross-Region Data Transfer Costs**:
- **Same region**: No additional charges for data transfer
- **Cross-region**: $0.02 per GB transferred (can add up quickly for large migrations)
- **NAT Gateway**: Additional charges if going through NAT

**Example Cost Impact**:
- Migrating 1TB from `us-east-1` EC2 → `us-west-2` S3 = ~$20 in data transfer charges
- Same migration within same region = $0 in data transfer charges

## Output Examples

### Scenario 1: Regions Aligned (Optimal) ✅

```bash
$ deltaglider migrate s3://old-bucket/ s3://new-bucket/
EC2 Instance: us-east-1a
S3 Client Region: us-east-1
✓ Regions aligned - no cross-region charges
Migrating from s3://old-bucket/
           to s3://new-bucket/
...
```

**Result**: No warnings, optimal configuration, no extra charges.

---

### Scenario 2: Auto-Detected Mismatch (INFO) ℹ️

```bash
$ deltaglider migrate s3://old-bucket/ s3://new-bucket/
EC2 Instance: us-west-2a
S3 Client Region: us-east-1

ℹ️  INFO: EC2 region (us-west-2) differs from configured S3 region (us-east-1)
    Consider using --region us-west-2 to avoid cross-region charges.

Migrating from s3://old-bucket/
           to s3://new-bucket/
...
```

**Result**: Informational warning, suggests optimal region. User didn't explicitly set wrong region, so it's likely from their AWS config.

---

### Scenario 3: Explicit Region Override Mismatch (WARNING) ⚠️

```bash
$ deltaglider migrate --region us-east-1 s3://old-bucket/ s3://new-bucket/
EC2 Instance: us-west-2a
S3 Client Region: us-east-1

⚠️  WARNING: EC2 region=us-west-2 != S3 client region=us-east-1
    Expect cross-region/NAT data charges. Align regions (set client region=us-west-2)
    before proceeding. Or drop --region for automatic region resolution.

Migrating from s3://old-bucket/
           to s3://new-bucket/
...
```

**Result**: Strong warning because user explicitly set the wrong region with `--region` flag. They might not realize the cost implications.

---

### Scenario 4: Not on EC2

```bash
$ deltaglider migrate s3://old-bucket/ s3://new-bucket/
S3 Client Region: us-east-1
Migrating from s3://old-bucket/
           to s3://new-bucket/
...
```

**Result**: Simple region display, no EC2 warnings (not applicable).

## Configuration

### Disable EC2 Detection

If you want to disable EC2 detection (e.g., for testing or if it causes issues):

```bash
export DG_DISABLE_EC2_DETECTION=true
deltaglider migrate s3://old/ s3://new/
```

Or in your script:

```python
import os
os.environ["DG_DISABLE_EC2_DETECTION"] = "true"
```

### How It Works

DeltaGlider uses **IMDSv2** (Instance Metadata Service v2) for security:

1. **Token Request** (PUT with TTL):
   ```
   PUT http://169.254.169.254/latest/api/token
   X-aws-ec2-metadata-token-ttl-seconds: 21600
   ```

2. **Metadata Request** (GET with token):
   ```
   GET http://169.254.169.254/latest/meta-data/placement/region
   X-aws-ec2-metadata-token: <token>
   ```

3. **Fast Timeout**: 1 second timeout for non-EC2 environments (no delay if not on EC2)

### Security Notes

- **IMDSv2 Only**: DeltaGlider uses the more secure IMDSv2, not the legacy IMDSv1
- **No Credentials**: Only reads metadata, never accesses credentials
- **Graceful Fallback**: Silently skips detection if IMDS unavailable
- **No Network Impact**: Uses local-only IP (169.254.169.254), never leaves the instance

## Best Practices

### For Cost Optimization

1. **Same Region**: Always try to keep EC2 instance and S3 bucket in the same region
2. **Check First**: Run with `--dry-run` to verify the setup before actual migration
3. **Use Auto-Detection**: Don't specify `--region` unless you have a specific reason
4. **Monitor Costs**: Use AWS Cost Explorer to track cross-region data transfer

### For Terraform/IaC

```hcl
# Good: EC2 and S3 in same region
resource "aws_instance" "app" {
  region = "us-west-2"
}

resource "aws_s3_bucket" "data" {
  region = "us-west-2"  # Same region
}
```

### For Multi-Region Setups

If you MUST do cross-region transfers:

1. **Use VPC Endpoints**: Reduce NAT Gateway costs
2. **Schedule Off-Peak**: AWS charges less during off-peak hours in some regions
3. **Consider S3 Transfer Acceleration**: May be cheaper for very large transfers
4. **Batch Operations**: Minimize number of API calls

## Technical Details

### EC2MetadataAdapter

Location: `src/deltaglider/adapters/ec2_metadata.py`

Key methods:
- `is_running_on_ec2()`: Detects EC2 environment
- `get_region()`: Returns AWS region code (e.g., "us-east-1")
- `get_availability_zone()`: Returns AZ (e.g., "us-east-1a")

### Region Logging

Location: `src/deltaglider/app/cli/aws_compat.py`

Function: `log_aws_region(service, region_override=False)`

Logic:
- If not EC2: Show S3 region only
- If EC2 + regions match: Green checkmark ✅
- If EC2 + auto-detected mismatch: Blue INFO ℹ️
- If EC2 + `--region` mismatch: Yellow WARNING ⚠️

## Troubleshooting

### "Cannot connect to IMDS"

**Cause**: Network policy blocks access to 169.254.169.254

**Solution**:
```bash
# Test IMDS connectivity
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
curl -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/placement/region

# If it fails, disable detection
export DG_DISABLE_EC2_DETECTION=true
```

### "Wrong region detected"

**Cause**: Cached metadata or race condition

**Solution**: DeltaGlider caches metadata for performance. Restart the process to refresh.

### "Warning appears but I want cross-region"

**Cause**: You intentionally need cross-region transfer

**Solution**: This is just a warning, not an error. The migration will proceed. The warning helps you confirm you understand the cost implications.

## FAQ

**Q: Does this slow down my migrations?**
A: No. EC2 detection happens once before migration starts (< 100ms). It doesn't affect migration performance.

**Q: What if I'm not on EC2 but the detection is slow?**
A: The timeout is 1 second. If IMDS is unreachable, it fails fast. Disable with `DG_DISABLE_EC2_DETECTION=true`.

**Q: Does this work on Fargate/ECS/Lambda?**
A: Yes! All AWS compute services support IMDSv2. The detection works the same way.

**Q: Can I use this with LocalStack/MinIO?**
A: Yes. When using `--endpoint-url`, DeltaGlider skips EC2 detection (not applicable for non-AWS S3).

**Q: Will this detect VPC endpoints?**
A: No. VPC endpoints don't change the "region" from an EC2 perspective. The warning still applies if regions don't match.

## Related Documentation

- [AWS Data Transfer Pricing](https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer)
- [AWS IMDSv2 Documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html)
- [S3 Transfer Costs](https://aws.amazon.com/s3/pricing/)
