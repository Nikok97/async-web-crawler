import aiohttp
import argparse
import asyncio
import collections
import json
import logging
import re

from crawler import setup_loggers, worker, enqueue_url, normalize_url
from dataclasses import dataclass
from db import db_initialization
from export_utilities import export_to_csv, export_to_json
from typing import Dict
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

@dataclass
class CrawlerContext:
    base_domain: str                
    rules: dict                       
    db: dict                           
    semaphores: Dict[str, asyncio.Semaphore] 
    logger: logging.Logger
    error_logger: logging.Logger
    skipped_logger: logging.Logger
    seed_url: str                 
    batch_size: int                
    output_format: str
    user_agent: str

logger, error_logger, skipped_logger = setup_loggers()

#Def main()
async def main(ctx: CrawlerContext, resume=False):
    """Main crawling loop that sets up the URL queue, starts worker tasks,
    manages asynchronous fetching, processes pages, and handles exporting results.
    Supports resuming unfinished crawls or starting fresh, with optional delays
    and concurrency limits per domain. Logs progress, skipped pages, and errors.
    """
    logging.info("main started")

    # Setup queue (local)
    url_queue = asyncio.Queue()

    # Load URLs into queue according to resume flag
    if resume:

        ctx.logger.info("Resume mode: checking for unfinished URLs...")
        ctx.db["cur"].execute('SELECT name FROM Urls WHERE date IS NULL')
        unfinished_urls = ctx.db["cur"].fetchall()
        ctx.logger.info(f"Found {len(unfinished_urls)} unfinished URLs.")

        if unfinished_urls:
            for (url,) in unfinished_urls:
                await enqueue_url(url_queue, url, 0)
                print("Queue size after enqueue:", url_queue.qsize())
        else:
            # If none unfinished, enqueue ctx.seed_url
            ctx.db["cur"].execute('INSERT OR IGNORE INTO Urls (name) VALUES (?)', (ctx.seed_url,))
            ctx.db["cur"].execute('UPDATE Urls SET date=NULL WHERE name=?', (ctx.seed_url,))
            ctx.db["conn"].commit()
            await enqueue_url(url_queue, ctx.seed_url, 0)
            #print("it has reached this else")
            print("Queue size after enqueue:", url_queue.qsize())
    else:
        # Fresh start
        ctx.db["cur"].execute('INSERT OR IGNORE INTO Urls (name) VALUES (?)', (ctx.seed_url,))
        ctx.db["conn"].commit()
        await enqueue_url(url_queue, ctx.seed_url, 0)
        #print("it has reached this fresh start")

    workers_count = ctx.batch_size or 1

    # Create the aiohttp session here
    async with aiohttp.ClientSession() as session:

        # Start workers with session argument
        worker_tasks = [asyncio.create_task(worker(session, url_queue, ctx)) for _ in range(workers_count)]

        # Wait until local queue is empty
        await url_queue.join()

        # Cancel workers
        for w in worker_tasks:
            w.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

# Export results
    if ctx.output_format == 'json':
        export_to_json(ctx)
    elif ctx.output_format == 'csv':
        export_to_csv(ctx)
    else:
        ctx.logger.info("Output stored in SQLite database")

    ctx.db["conn"].close()

#####################################
#Command line interface section
#####################################

def cli_main():
    """Parse configuration and CLI arguments, initialize database and context,
    set up loggers and semaphores, and run the main crawling coroutine.
    Handles overrides from CLI such as domain, depth, output format, resume flag,
    and optional Playwright usage.
    """
    logging.info("cli_main started")

    #Config setup
    with open('config.json') as f:
        config = json.load(f)

    db_path = config.get("database_path", "mini.sqlite")
    db = db_initialization(db_path)
    seed_url = config['seed_url']
    delay_min, delay_max = config['delay_range']
    batch_size = config['batch_size']
    file_type_filters = tuple(config['file_type_filters'])
    crawl_depth_limit = config.get('crawl_depth_limit', None)
    output_format = config.get('output_format', 'sqlite')
    use_playwright = config.get('use_playwright', False)
    max_concurrent_per_domain = config.get('max_concurrent_per_domain', 2)
    include_paths = config.get('include_paths', [])
    exclude_patterns = config.get('exclude_patterns', [])
    compiled_exclude_regexes = [re.compile(p) for p in exclude_patterns]
    min_content_length = config.get('min_content_length', 0)
    max_content_length = config.get('max_content_length', 10_000_000)

    #Argparser setup
    parser = argparse.ArgumentParser(description='Async Web Crawler with Playwright')
    parser.add_argument('--domain', type=str, default=None, help='Domain to crawl (overrides config)')
    parser.add_argument('--depth', type=int, default=None, help='Crawl depth limit')
    parser.add_argument('--output', type=str, choices=['sqlite', 'json', 'csv'], default=None, help='Output format')
    parser.add_argument('--resume', action='store_true', help='Resume crawling from unfinished URLs')
    parser.add_argument('--playwright', action='store_true', help='Use Playwright for dynamic content fetching')
    # Subarguments for on demand export
    parser.add_argument('--export', choices=['json', 'csv'], help='Export existing database to JSON or CSV (no crawling)')
    parser.add_argument('--export-file', type=str, help='Optional filename for export output')

    args = parser.parse_args()

    # Handle on-demand export mode
    if args.export:
        db = db_initialization(db_path)
        if args.export == 'json':
            export_to_json(db, args.export_file or 'exported_data.json')
        else:
            export_to_csv(db, args.export_file or 'exported_data.csv')
        #print(f"Exported crawl results to {args.export_file or f'exported_data.{args.export}'}")
        return

    # Initialize semaphores after reading config
    domain_semaphores = collections.defaultdict(lambda: asyncio.Semaphore(max_concurrent_per_domain))
    semaphores = domain_semaphores

    # Override ctx.seed_url with CLI domain if provided
    if args.domain:
        seed_url = args.domain

    # Normalize ctx.seed_url and get base_domain after override
    seed_url = normalize_url(seed_url)
    base_domain = urlparse(seed_url).netloc
    crawl_depth_limit = args.depth or crawl_depth_limit
    output_format = args.output or output_format
    #use_playwright = args.playwright or use_playwright

    # Setup robots.txt
    parsed_seed = urlparse(seed_url)
    robots_url = f"{parsed_seed.scheme}://{parsed_seed.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception as e:
        logger.error(f"Could not read robots.txt: {e}")

    # Context rules
    rules = {
        "rp": rp,
        "base_domain": base_domain,
        "include_paths": include_paths,
        "exclude_regexes": compiled_exclude_regexes,
        "min_content_length": min_content_length,
        "max_content_length": max_content_length,
        "crawl_depth_limit": crawl_depth_limit,
        "delay_min": delay_min,
        "delay_max": delay_max,
        "seed_url": seed_url,
        
        "user_agent": config.get("user_agent", "Mozilla/5.0"),
        "retries": config.get("retries", 3),
        "max_redirects": config.get("max_redirects", 5),
        "timeout_seconds": config.get("timeout_seconds", 10),
        "respect_robots_txt": config.get("respect_robots_txt", True),
    }

    # Dataclass creation
    ctx = CrawlerContext(
        base_domain=base_domain,
        rules=rules,
        db=db,
        semaphores=semaphores,
        logger=logger,
        error_logger=error_logger,
        skipped_logger=skipped_logger,
        seed_url=seed_url,
        batch_size=batch_size,
        output_format=output_format,
        user_agent=rules.get("user_agent")
    )

    asyncio.run(main(ctx, resume=True))

if __name__ == "__main__":
    cli_main()