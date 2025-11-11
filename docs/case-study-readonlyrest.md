## How ReadonlyREST Cut 4TB of S3 Storage Down to 5GB (and Saved 99.9%)

### TL;DR

We were paying to store 4TB of mostly identical plugin builds.
DeltaGlider deduplicated everything down to 4.9GB — 99.9% smaller, $1.1k/year cheaper, and no workflow changes.

#### The Problem

ReadonlyREST supports ~150 Elasticsearch/Kibana versions × multiple product lines × all our own releases.
After years of publishing builds, our S3 archive hit `4TB` (201,840 files, $93/month).
Glacier helped, but restoring files took 48 hours — useless for CI/CD.

Every plugin ZIP was ~82MB, but `99.7% identical` to the next one. We were paying to store duplicates.

#### The Fix: DeltaGlider

DeltaGlider stores binary deltas between similar files instead of full copies.

# Before
```
aws s3 cp readonlyrest-1.66.1_es8.0.0.zip s3://releases/  # 82MB
```

# After
```
deltaglider cp readonlyrest-1.66.1_es8.0.0.zip s3://releases/  # 65KB
```

Drop-in replacement for `aws s3 cp`. No pipeline changes.
Data integrity checked with SHA256, stored as metadata in S3.


### The Result

| Metric        | Before   | After    | Δ            |
|-------------- |----------|----------|--------------|
| Storage       | 4.06TB   | 4.9GB    | -99.9%       |
| Cost          | $93/mo   | $0.11/mo | -$1,119/yr   |
| Files         | 201,840  | 201,840  | identical    |
| Upload speed  | 1x       | 3–4x     | faster       |

Each “different” ZIP? Just a 65KB delta.
Reconstruction time: <100ms.
Zero user impact.


## Under the Hood

Uses xdelta3 diffs.
	•	Keeps one reference per group
	•	Stores deltas for near-identical files
	•	Skips small or text-based ones (.sha, .json, etc.)

It’s smart enough to decide what’s worth diffing automatically.


## Payoff
	•	4TB → 5GB overnight
	•	Uploads 1,200× faster
	•	CI bandwidth cut 99%
	•	100% checksum verified integrity
	•	Zero vendor lock-in (open source)

## Takeaways

If You Ship Versioned Artifacts

This will probably save you four figures and hours of upload time per year.

```
pip install deltaglider
deltaglider cp my-release.zip s3://releases/
```

That’s it.