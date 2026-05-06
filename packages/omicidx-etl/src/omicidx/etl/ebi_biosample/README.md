# EBI BioSample Extractor

## Overview

This package extracts biosample metadata from the European Bioinformatics Institute (EBI) BioSamples database via their public REST API. The extractor fetches samples incrementally by date ranges and saves them as Parquet files with ZSTD compression.

## Data Source

- **Source**: EBI BioSamples Database
- **API Endpoint**: `https://www.ebi.ac.uk/biosamples/samples`
- **Documentation**: https://www.ebi.ac.uk/biosamples/docs/references/api/overview
- **Update Frequency**: Continuous (EBI updates ongoing)
- **Data Coverage**: Global biosample metadata from 2021-01-01 to present

## Architecture

### Extraction Strategy

**Date-Ranged Incremental Extraction**:
- Data is fetched in daily chunks (one file per day: 2021-01-01, 2021-01-02, etc.)
- Each day is processed independently and can run in parallel
- Completed days are tracked with `.done` semaphore files to prevent re-processing
- Uses the EBI API's `update` date filter to get samples modified on each specific date

**Why Daily Chunks?**
- Fine-grained incremental updates (re-run only specific days)
- Smaller file sizes make individual files easier to manage
- Failed extractions only affect single day, not entire month
- Parallel processing of independent date ranges speeds up extraction
- Better alignment with daily update workflows

### Concurrency Model

**Async + Semaphore-Based Rate Limiting**:
- Uses `anyio` for async/await concurrency
- Semaphore limits concurrent tasks to 20 to avoid overwhelming the API
- Each date range is processed as an independent async task
- Automatic retries with exponential backoff (up to 10 attempts)

### Data Flow

```
EBI API (paginated)
      ↓
SampleFetcher (async iterator)
      ↓
JSON transformation (characteristics flattening)
      ↓
In-memory buffer (Python list)
      ↓
PyArrow Table with enforced schema
      ↓
Parquet file (one per day, ZSTD compression level 9)
      ↓
Semaphore file (.done) created
```

## Output Format

### File Structure

```
{output_dir}/ebi_biosample/
├── biosamples-2021-01-01--2021-01-01--daily.parquet
├── biosamples-2021-01-01--2021-01-01--daily.parquet.done
├── biosamples-2021-01-02--2021-01-02--daily.parquet
├── biosamples-2021-01-02--2021-01-02--daily.parquet.done
└── ...
```

**Filename Convention**: `biosamples-{date}--{date}--daily.parquet`
- Dates in `YYYY-MM-DD` format (start and end date are the same for daily extraction)
- `--daily` suffix indicates daily granularity
- `.done` semaphore files track completion

### Data Format

**Parquet** (Columnar storage):
- Apache Parquet format with enforced PyArrow schema
- ZSTD compression (level 9) for optimal size
- Each file contains all biosample records for a single day
- Schema includes nested structures for characteristics, organizations, publications, etc.

**Schema Transformation**:

The extractor performs one key transformation on the raw API response:

**Before** (API response):
```json
{
  "accession": "SAMEA123456",
  "characteristics": {
    "organism": [{"text": "Homo sapiens"}],
    "age": [{"text": "65", "unit": "years"}]
  }
}
```

**After** (written to file):
```json
{
  "accession": "SAMEA123456",
  "characteristics": [
    {"text": "Homo sapiens", "characteristic": "organism"},
    {"text": "65", "unit": "years", "characteristic": "age"}
  ]
}
```

**Why?** The API returns characteristics as a nested object with keys being characteristic names. We flatten this to an array and add the `characteristic` field explicitly, making downstream processing easier. The Parquet schema enforces this structure as a list of structs.

### Example Record

```json
{
  "accession": "SAMEA7733558",
  "name": "sample_name",
  "update": "2024-11-08T12:00:00Z",
  "release": "2024-11-08T12:00:00Z",
  "characteristics": [
    {
      "text": "Homo sapiens",
      "ontologyTerms": ["http://purl.obolibrary.org/obo/NCBITaxon_9606"],
      "characteristic": "organism"
    },
    {
      "text": "blood",
      "characteristic": "tissue"
    }
  ],
  "externalReferences": [
    {
      "url": "https://www.ncbi.nlm.nih.gov/biosample/SAMN12345678"
    }
  ]
}
```

## Path Configuration

### Current (Legacy)

**Hardcoded Configuration**:
```python
from ..config import settings
output_dir = str(UPath(settings.PUBLISH_DIRECTORY) / "ebi_biosample")
```

**Environment Variable**: `PUBLISH_DIRECTORY` (default: `/data/omicidx`)

**Result**: Files written to `/data/omicidx/ebi_biosample/`

### Future (PathProvider Migration)

When migrated to use PathProvider (see [../../EXTRACT_MIGRATION_GUIDE.md](../../EXTRACT_MIGRATION_GUIDE.md)):

```python
from omicidx_etl.extract_config import get_path_provider

provider = get_path_provider()
output_dir = provider.ensure_path("ebi_biosample")
```

**Environment Variable**: `OMICIDX_EXTRACT_BASE_DIR`

**Result**: Files written to `{OMICIDX_EXTRACT_BASE_DIR}/ebi_biosample/`

## Code Components

### SampleFetcher Class

**Purpose**: Manages stateful pagination through EBI API results

**Key Attributes**:
- `cursor`: Pagination cursor (starts at `*`)
- `start_date` / `end_date`: Date range filter
- `full_url`: Next page URL (from API `_links.next`)
- `samples_buffer`: List to buffer samples in memory before Parquet write
- `any_samples`: Flag to track if any data was fetched

**Key Methods**:
- `date_filter_string()`: Constructs EBI API date filter syntax
- `perform_request()`: HTTP request with retry logic (10 attempts, exponential backoff)
- `fetch_next_set()`: Async generator that yields samples, follows pagination
- `process()`: Main loop that buffers samples in memory
- `completed()`: Called when pagination exhausted

### Helper Functions

#### `get_filename(start_date, end_date, tmp=True, output_directory)`
Generates output filename for a date range.

**Parameters**:
- `start_date`, `end_date`: Date range
- `tmp`: If True, appends `.tmp` (for in-progress files)
- `output_directory`: Base directory

**Returns**: Full path string

**Example**: `"/data/omicidx/ebi_biosample/biosamples-2021-01-01--2021-01-01--daily.parquet.tmp"`

#### `get_date_ranges(start_date_str, end_date_str)`
Generates daily date ranges between two dates.

**Parameters**:
- `start_date_str`: Start date `"YYYY-MM-DD"`
- `end_date_str`: End date `"YYYY-MM-DD"`

**Returns**: Iterator of `(date, date)` tuples for each day (start and end are the same)

**Example**:
```python
list(get_date_ranges("2021-01-01", "2021-01-03"))
# [(date(2021, 1, 1), date(2021, 1, 1)),
#  (date(2021, 1, 2), date(2021, 1, 2)),
#  (date(2021, 1, 3), date(2021, 1, 3))]
```

#### `process_by_dates(start_date, end_date, output_directory)`
Async function to process a single date range.

**Flow**:
1. Create `SampleFetcher` for date range
2. Fetch samples into in-memory buffer
3. **If samples were found**:
   - Convert buffer to PyArrow Table using `get_biosample_schema()`
   - Write to `.tmp` Parquet file with ZSTD compression
   - Rename `.tmp` → final filename
   - Create empty `.done` file
4. **If no samples found**:
   - Create `.done` file with content `NO_SAMPLES`
   - This marks the day as processed to prevent re-checks

#### `limited_process(semaphore, start_date, end_date, output_directory)`
Wrapper that applies semaphore rate limiting to `process_by_dates`.

#### `main(output_directory)`
Main orchestration function.

**Flow**:
1. Define date range: `2021-01-01` to yesterday (excludes today to avoid partial day data)
2. Create semaphore (limit: 20 concurrent tasks)
3. Generate all daily date ranges
4. Skip dates with existing `.done` files
5. Schedule async tasks for remaining dates
6. Wait for all tasks to complete

## Usage

### CLI Command

```bash
# Use default output directory
uv run oidx ebi_biosample extract

# Specify custom output directory
uv run oidx ebi_biosample extract --output-dir /custom/path
```

### Programmatic

```python
import anyio
from omicidx_etl.ebi_biosample.extract import main

# Run with default directory
anyio.run(main)

# Run with custom directory
anyio.run(main, output_directory="/custom/path")
```

### Direct Execution

```bash
# Run directly (uses default output)
python -m omicidx_etl.ebi_biosample.extract
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLISH_DIRECTORY` | `/data/omicidx` | Base directory for all extractions |

### Hardcoded Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `BASEURL` | `https://www.ebi.ac.uk/biosamples/samples` | EBI API endpoint |
| Start Date | `2021-01-01` | Beginning of extraction range |
| End Date | `datetime.now() - 1 day` | Extract up to yesterday (avoids partial day data) |
| Semaphore Limit | `20` | Max concurrent daily tasks |
| Page Size | `200` | Samples per API request |
| Retry Attempts | `10` | Max retries for failed requests |
| Retry Max Wait | `40s` | Max exponential backoff wait time |

## Assumptions & Design Decisions

### 1. Update Date Filtering
**Assumption**: The API's `update` field reliably tracks when samples were modified.

**Implication**: Re-running a day will capture updates to existing samples, not just new samples.

**Trade-off**: May fetch duplicates if samples are updated multiple times, but ensures completeness.

**Important**: Extraction stops at yesterday (not today) to avoid partial day data from samples still being updated.

### 2. Daily Granularity
**Assumption**: Daily chunks provide fine-grained incremental extraction.

**Rationale**:
- Better incremental updates (only re-run affected days)
- Smaller file sizes
- Failed extractions only affect single day
- More files (1000+), but modern filesystems handle well

### 3. Semaphore Files (.done)
**Assumption**: File existence is sufficient to track completion.

**Behavior**:
- **With samples**: `.done` file created alongside data file (empty marker)
- **No samples**: `.done` file created with content `NO_SAMPLES` (data file doesn't exist)
- This prevents re-checking days with legitimately zero samples

**Limitation**: `.done` files only track completion, not when completed or how many samples.

**Future Enhancement**: Store full metadata (timestamp, record count) in `.done` files as JSON.

### 4. Characteristics Flattening
**Assumption**: Array format is more useful than object format for downstream processing.

**Rationale**:
- Easier to explode in SQL/Pandas
- Columnar formats (Parquet) handle arrays better than nested objects
- Makes characteristic names queryable as values, not just keys

### 5. No Incremental Within Day
**Design**: Each day is all-or-nothing (no partial completion tracking).

**Risk**: If a day fails midway, restart from beginning.

**Mitigation**: `.tmp` files prevent partial data; retries handle transient failures; daily scope is small enough that restart cost is minimal.

## Error Handling

### Retry Logic
- **HTTP Errors**: 10 retries with exponential backoff (1-40s)
- **Network Errors**: Caught and retried
- **Timeout**: 40s per request

### Failure Modes

| Scenario | Behavior | Recovery |
|----------|----------|----------|
| API returns 429 (rate limit) | Retry with backoff | Automatic |
| Network timeout | Retry up to 10 times | Automatic |
| Day has no samples | Delete `.tmp` file, create `.done` with `NO_SAMPLES` marker | Intentional (valid case) |
| KeyError in response | Silently skip, continue to next page | Logged warning |
| Disk full during write | Unhandled, will crash | Manual: check disk space |

### Error Scenarios Not Handled
1. **Disk space exhaustion**: No pre-check or graceful degradation
2. **Corrupt .tmp files**: No validation before rename
3. **Partial day completion**: No checkpointing within a day (though impact is minimal given daily scope)
4. **API schema changes**: No schema validation

## Testing & Validation

### Verify Extraction

```bash
# Check output files exist
ls -lh /data/omicidx/ebi_biosample/

# Count .done files (should match total days processed)
ls /data/omicidx/ebi_biosample/*.done | wc -l

# Check which days had no samples
grep -l "NO_SAMPLES" /data/omicidx/ebi_biosample/*.done

# Count records in a file using DuckDB
duckdb -c "SELECT COUNT(*) FROM 'biosamples-2021-01-01--2021-01-01--daily.parquet'"

# Inspect schema
duckdb -c "DESCRIBE SELECT * FROM 'biosamples-2021-01-01--2021-01-01--daily.parquet'"

# View sample records
duckdb -c "SELECT accession, name, update FROM 'biosamples-2021-01-01--2021-01-01--daily.parquet' LIMIT 5"

# Verify characteristics structure (should be list of structs)
duckdb -c "SELECT characteristics FROM 'biosamples-2021-01-01--2021-01-01--daily.parquet' LIMIT 1"

# Check file compression ratio
ls -lh biosamples-2021-01-01--2021-01-01--daily.parquet
```

### Test Date Range Logic

```python
from omicidx_etl.ebi_biosample.extract import get_date_ranges
from datetime import date

# Test daily splitting
ranges = list(get_date_ranges("2021-01-01", "2021-01-03"))
assert len(ranges) == 3  # 3 days
assert ranges[0] == (date(2021, 1, 1), date(2021, 1, 1))  # Single day
assert ranges[2] == (date(2021, 1, 3), date(2021, 1, 3))
```

### Test with Small Date Range

```python
import anyio
from datetime import date
from omicidx_etl.ebi_biosample.extract import process_by_dates
from pathlib import Path

# Test single day
anyio.run(
    process_by_dates,
    start_date=date(2021, 1, 1),
    end_date=date(2021, 1, 1),
    output_directory="/tmp/test"
)

# Verify output
assert Path("/tmp/test/biosamples-2021-01-01--2021-01-01--daily.parquet").exists()
assert Path("/tmp/test/biosamples-2021-01-01--2021-01-01--daily.parquet.done").exists()

# Verify with DuckDB
import duckdb
result = duckdb.query("SELECT COUNT(*) FROM '/tmp/test/biosamples-2021-01-01--2021-01-01--daily.parquet'").fetchone()
assert result[0] > 0  # Should have some records
```

## Performance Characteristics

### Throughput
- **API Speed**: ~200 samples/request, ~1-2 requests/second (with retries)
- **Daily Volume**: Varies widely (0 to 1000s of samples per day)
- **Parallelism**: 20 concurrent days
- **Typical Full Run**: 3-6 hours for 3+ years of data (2021-2024, ~1000+ days)

### Resource Usage
- **Memory**: Moderate (~50-200MB per task for buffering samples, 20 tasks = <4GB)
- **Disk I/O**: Sequential writes (Parquet with ZSTD compression is CPU-bound)
- **Network**: ~20 concurrent HTTP connections

### Bottlenecks
1. **API Rate Limits**: 20 concurrent is conservative (could potentially go higher)
2. **Parquet Compression**: CPU-bound (ZSTD level 9), but provides 50-70% space savings
3. **Network Latency**: Each request has ~100-500ms latency
4. **Memory Buffering**: All samples for a day must fit in memory before writing

## Known Issues & Limitations

### 1. Hardcoded Start Date
**Issue**: Start date is hardcoded to `2021-01-01` in `main()`

**Impact**: Cannot easily extract pre-2021 data or different ranges

**Workaround**: Modify `main()` or call `process_by_dates()` directly

**Future**: Add CLI options for date range

### 2. No Progress Tracking
**Issue**: No visibility into overall progress during long runs

**Impact**: Can't estimate completion time

**Workaround**: Check `.done` files vs total days needed

**Future**: Add progress bar or logging of `X/Y days completed`

### 3. Characteristics Schema Not Validated
**Issue**: Assumes all characteristics have `text` field; no validation

**Impact**: Could fail silently if API schema changes

**Future**: Add JSON schema validation

### 4. No Deduplication
**Issue**: Re-running a day will duplicate records if samples were updated

**Impact**: Downstream processing must deduplicate by accession + update timestamp

**Workaround**: Handle in warehouse layer (staging model)

### 5. Day Boundary Artifacts
**Issue**: Samples updated exactly at midnight might appear in two days

**Impact**: Potential edge-case duplicates

**Probability**: Very low (API update timestamps are precise)

## Maintenance Notes

### Updating Date Range

To change the extraction date range:

```python
# In main() function, modify:
start = "2021-01-01"  # Change start date here
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
end = yesterday  # Default: yesterday to avoid partial days
# Or set fixed end date:
# end = "2024-12-31"
```

**Note**: The default excludes "today" to ensure complete day data only.

### Adjusting Concurrency

To change parallel task limit:

```python
# In main() function, modify:
semaphore = anyio.Semaphore(20)  # Increase/decrease number
```

**Note**: With daily granularity, you may have 1000+ tasks queued. The semaphore ensures only 20 run concurrently.

### Changing Page Size

To fetch more/fewer samples per request:

```python
# In SampleFetcher.__init__, modify:
self.size = 200  # API supports up to 1000
```

**Trade-off**: Larger pages = fewer requests but higher latency and failure risk.

## Migration Path to PathProvider

When this module is migrated to use PathProvider (per [EXTRACT_MIGRATION_GUIDE.md](../../EXTRACT_MIGRATION_GUIDE.md)):

### Changes Required

1. **Import PathProvider**:
```python
from omicidx_etl.extract_config import get_path_provider
```

2. **Remove module-level path**:
```python
# DELETE THIS:
output_dir = str(UPath(settings.PUBLISH_DIRECTORY) / "ebi_biosample")
```

3. **Update functions**:
```python
def get_filename(start_date, end_date, tmp=True, output_dir=None):
    if output_dir is None:
        provider = get_path_provider()
        output_dir = str(provider.get_path("ebi_biosample"))
    # ... rest of function
```

4. **Update CLI**:
```python
@ebi_biosample.command()
@click.option('--output-dir', type=click.Path(), default=None,
              help='Output directory (default: from config)')
def extract(output_dir: str):
    if output_dir is None:
        provider = get_path_provider()
        output_dir = str(provider.ensure_path("ebi_biosample"))

    anyio.run(main, output_dir)
```

5. **Register files (optional)**:
```python
# After successful extraction in process_by_dates()
provider.register_file("ebi_biosample", final_filename, {
    "start_date": str(start_date),
    "end_date": str(end_date),
    "record_count": fetcher.record_count  # Would need to track this
})
```

## Related Documentation

- [../../EXTRACT_MIGRATION_GUIDE.md](../../EXTRACT_MIGRATION_GUIDE.md) - PathProvider migration guide
- [../../PATH_ARCHITECTURE.md](../../PATH_ARCHITECTURE.md) - Overall path management architecture
- [../../CLAUDE.md](../../CLAUDE.md) - Repository-level guidance for AI agents
- EBI BioSamples API Docs: https://www.ebi.ac.uk/biosamples/docs/references/api/overview

## Code Principles for AI Agents

When modifying this code, follow these principles:

1. **Preserve async patterns**: Use `async`/`await` and `anyio` for concurrency
2. **Maintain retry logic**: Don't remove tenacity decorators without replacement
3. **Keep semaphore limits**: Prevents API abuse and rate limiting
4. **Preserve file naming convention**: Downstream systems depend on date-based filenames
5. **Don't skip semaphore files**: Critical for incremental extraction logic
6. **Maintain characteristics transformation**: Downstream warehouse expects array format
7. **Test with small date ranges**: Always test changes with 1-2 months before full run
8. **Log progress**: Add logging, don't remove existing log statements

## Changelog

### Current Version
- Initial implementation
- Daily chunking strategy (changed from monthly)
- Async extraction with semaphore limiting
- Characteristics flattening transformation
- Extracts up to yesterday (excludes today to avoid partial data)
- Enhanced `.done` file handling: writes `NO_SAMPLES` marker for empty days
- **Parquet output format** with enforced PyArrow schema and ZSTD compression
- In-memory buffering with batch write for better performance

### Future Enhancements
- [ ] Migrate to PathProvider
- [ ] Add CLI date range options
- [ ] Add progress tracking/reporting
- [ ] Store full metadata in semaphore files (JSON with timestamp, record count, etc.)
- [ ] Add schema validation against API responses
- [ ] Add record count tracking to fetcher
- [ ] Support partial day checkpointing (lower priority given small scope)
- [ ] Consider streaming write for very large days (if memory becomes an issue)
