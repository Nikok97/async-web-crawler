import asyncio
import logging
import random

from urllib.parse import urlparse

from db import (
    insert_url_and_get_id,
    insert_link_relationship,
    insert_category,
    insert_keywords,
    insert_product,
    get_url_id,
    save_to_db
)

from parse import (
    normalize_url,
    is_allowed_domain,
    url_allowed,
    parse_links,
    extract_keywords,
    compute_hash,
    get_product_data,
    extract_category,
    should_skip_url,
    process_page
)

from fetch_utility import (
    fetch_static,
    fetch_dynamic,
    fetch_url,
    looks_like_content,
    now
)

def setup_loggers():
    """Sets up a general logger, a skipped logger and an error logger, with corresponding handlers and levels."""
    # General logger
    logging.basicConfig(
        filename='crawler.log',
        filemode='a',
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )

    # Error logger
    error_logger = logging.getLogger('error_logger')
    error_handler = logging.FileHandler('crawler_errors.log')
    error_handler.setLevel(logging.ERROR)
    error_logger.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    error_handler.setFormatter(error_formatter)
    error_logger.addHandler(error_handler)
    error_logger.propagate = False

    # Skipped pages logger
    skipped_logger = logging.getLogger('skipped_logger')
    skipped_handler = logging.FileHandler('skipped_pages.log')
    skipped_handler.setLevel(logging.INFO)
    skipped_formatter = logging.Formatter('%(asctime)s SKIPPED: %(message)s')
    skipped_handler.setFormatter(skipped_formatter)
    skipped_logger.addHandler(skipped_handler)
    skipped_logger.propagate = False

    # Return for use
    logger = logging.getLogger() 
    return logger, error_logger, skipped_logger


######################################################

#Def enqueue_url
async def enqueue_url(queue, url, depth):
    """
    Add a URL with its crawl depth to the async queue for processing.

    """
    print(f"Enqueueing URL: {url} at depth {depth}")
    await queue.put((url, depth))

#Def dequeue_url
async def dequeue_url(queue):
    """
    Retrieve a URL and its depth from the queue, with a timeout to avoid blocking indefinitely.

    """
    try:
        url, depth = await asyncio.wait_for(queue.get(), timeout=3)
        print(f"Dequeued URL from local queue: {url} at depth {depth}")
        return url, depth
    except asyncio.TimeoutError:
        #print("Asyncio Error")
        return None, None
    
# Worker coroutine
async def worker(session, url_queue, ctx):

    """Continuously fetch and process URLs from the queue."""
    
    while True:

        currenturl, currentdepth = await dequeue_url(url_queue)

        if currenturl is None:
            ctx.logger.info("Queue is empty, waiting for new URLs...")
            await asyncio.sleep(1)
            continue

        skip_reason = None
        html = None

        try:
            # Skip based on rules and DB date checking
            skip, reason = await should_skip_url(currenturl, ctx)
            if skip:
                ctx.logger.info(f"Skipped {currenturl}: {reason}")
                #print(f"Url skipped, {reason}")
                continue
    
            # Fetch page
            else:
                domain = urlparse(currenturl).netloc
                semaphore = ctx.semaphores[domain]

                async with semaphore:
                    html = await fetch_url(ctx, session, currenturl)
                    print("HTML fetched length:", len(html) if html else "None")

                # Optional delay
                if ctx.rules["delay_min"] is not None and ctx.rules["delay_max"] is not None:
                    await asyncio.sleep(random.uniform(ctx.rules["delay_min"], ctx.rules["delay_max"]))

                # Validate content using fetch utility
                if not looks_like_content(html):
                    skip_reason = "Fetch failed, empty, or too short content"
                    ctx.logger.info(f"Skipped {currenturl}, {skip_reason})")

            if skip_reason:
                ctx.skipped_logger.info(f"Skipped {currenturl}: {skip_reason}")
                ctx.logger.info(f"{currenturl} | Reason: {skip_reason}")
                await asyncio.sleep(0) 
                continue

            # Page is valid; process content:
            # Parse links: get links and metadata for valid URLs
            to_enqueue, link_pairs, soup, keywords, category, product_data = process_page(ctx, currenturl, html, currentdepth)
                        
            # DB writes in a single lock
            inserted_ids = await save_to_db(ctx, currenturl, to_enqueue, link_pairs, product_data, category, keywords)

            # Enqueue outside lock (non-blocking db lock) 
            for normalized_link, next_depth in to_enqueue:
                
                print(f"Inserted URL {normalized_link} with id={inserted_ids.get(normalized_link)}")

                await enqueue_url(url_queue, normalized_link, next_depth)

     
        finally:
            # Mark queue item as done
            url_queue.task_done()
