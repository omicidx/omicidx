import aioboto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime
import pyarrow.parquet as pq
import polars as pl
import asyncio
import httpx
from tqdm.asyncio import tqdm

SESSION = aioboto3.Session()

MAX_CONCURRENT_FETCHES = 50
ROWS_PER_PARQUET = 5000
obj_count = 0
 
async def fetch_object(httpx_client: httpx.AsyncClient, bucket, key):
    try:
        resp = await httpx_client.get(
            f"https://{bucket}.s3.amazonaws.com/{key}",
            timeout=60.0
        )
        resp.raise_for_status()
        headers = resp.headers
        body = resp.content
        return {
            "bucket": bucket,
            "key": key,
            "etag": headers.get("ETag", ""),
            "last_modified": headers.get("LastModified", ""),
            "content_length": len(body),
            "text": body.decode("utf-8", errors="replace")
        }
    except Exception as e:
        print(f"ERROR fetching s3://{bucket}/{key}: {e}")
        raise
    
async def fetch_worker(queue, writer_queue, semaphore, httpx_client, pbar):
    byte_count = 0
    while True:
        try:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            async with semaphore:
                try:
                    row = await fetch_object(httpx_client, item["bucket"], item["key"])
                    row['fetched_at'] = datetime.now()
                    byte_count += row['content_length']
                    await writer_queue.put(row)
                    pbar.update(1)
                except Exception as e:
                    print(f"Worker failed to fetch: {e}")
                    pbar.update(1)
            queue.task_done()
        except Exception as e:
            print(f"Worker exception: {e}")
            queue.task_done()
        
async def write_worker(queue, rows):
    byte_count = 0
    while True:
        item = await queue.get()
        if item is None:
            # Flush any remaining rows before exiting
            if rows:
                print(f"Flushing {len(rows)} rows to parquet")
                table = pl.DataFrame(rows)
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                filename = f"pmc_text_{timestamp}.parquet"
                pq.write_table(table.to_arrow(), filename)
                print(f"Wrote {filename}")
                rows.clear()
            queue.task_done()
            break

        rows.append(item)
        byte_count += item['content_length']
        if byte_count >= 1_000_000_000: # 1 GB
            print(f"Flushing {len(rows)} rows to parquet (1GB threshold)")
            table = pl.DataFrame(rows)
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            filename = f"pmc_text_{timestamp}.parquet"
            pq.write_table(table.to_arrow(), filename)
            print(f"Wrote {filename}")
            rows.clear()
            byte_count = 0
        queue.task_done()




async def fetch_metadata_csv() -> pl.DataFrame:
    cache_file = 'ncbi_pmc_oa.filelist.parquet'
    try:
        df = pl.read_parquet(cache_file)
        return df
    except FileNotFoundError:
        df = pl.read_csv('https://pmc-oa-opendata.s3.amazonaws.com/oa_comm/txt/metadata/csv/oa_comm.filelist.csv')
        df_noncomm = pl.read_csv('https://pmc-oa-opendata.s3.amazonaws.com/oa_noncomm/txt/metadata/csv/oa_noncomm.filelist.csv')
        #df_author_manuscript = pl.read_csv('https://pmc-oa-opendata.s3.amazonaws.com/author_manuscript/txt/metadata/csv/author_manuscript.filelist.csv')
        #df = pl.concat([df, df_noncomm, df_author_manuscript])
        df = pl.concat([df, df_noncomm])
        df = df.unique()
        df.write_parquet(cache_file)
    return df


async def key_generator(metadata_df: pl.DataFrame) -> dict[str, str]:
    bucket = "pmc-oa-opendata"
    keys = metadata_df.select("Key").to_series().to_list()
    existing_keys = set(pl.scan_parquet('pmc*.parquet').collect().select("key").to_series().to_list())
    keys = [{"bucket": bucket, "key": k} for k in keys if k not in existing_keys]
    return keys

async def run():
    reader_queue = asyncio.Queue()
    writer_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    rows = []
    metadata_df = await fetch_metadata_csv()
    keys = await key_generator(metadata_df)
    total_keys = len(keys)
  
    # Create httpx client with custom timeout settings
    httpx_client = httpx.AsyncClient(timeout=60.0)
    try:
        # Create progress bar
        pbar = tqdm(total=total_keys, desc="Fetching PMC texts", unit="file")
        
        # Create fetch workers
        fetch_workers = [
            asyncio.create_task(fetch_worker(reader_queue, writer_queue, semaphore, httpx_client, pbar))
            for _ in range(30)
        ]
        
        # Create write workers
        write_workers = [
            asyncio.create_task(write_worker(writer_queue, rows))
        ]

        print(f"Starting to fetch from metadata")
        for k in keys:
            await reader_queue.put(k)

        print("All items queued, waiting for readers...")
        # Wait for all fetches to complete
        await reader_queue.join()
        print("All reads complete, signaling workers to stop...")

        # Signal fetch workers to stop
        for _ in fetch_workers:
            await reader_queue.put(None)
        
        try:
            await asyncio.wait_for(asyncio.gather(*fetch_workers), timeout=30.0)
        except asyncio.TimeoutError:
            print("Timeout waiting for fetch workers")
        
        print("All fetch workers stopped, waiting for writers...")
        # Wait for all writes to complete
        await writer_queue.join()
        
        # Signal write workers to stop
        for _ in write_workers:
            await writer_queue.put(None)
        
        try:
            await asyncio.wait_for(asyncio.gather(*write_workers), timeout=30.0)
        except asyncio.TimeoutError:
            print("Timeout waiting for write workers")
        
        pbar.close()
        print(f"Complete. Fetched {len(rows)} items")
    finally:
        await httpx_client.aclose()

    return rows



if __name__ == "__main__":
    asyncio.run(run())