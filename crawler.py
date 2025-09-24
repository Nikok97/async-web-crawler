import asyncio
import aiohttp
import sqlite3
import ssl
import logging
import collections
import json
import argparse
import hashlib
import re
import random

from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode, urldefrag
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.robotparser import RobotFileParser
from collections import Counter
from datetime import datetime, timezone

#Set up keywords
STOPWORDS = set([
    'the','and','for','are','this','that','with',
    'from','was','were',
    'will','would','shall','should','can','could','have','has','had',
    'you','your','yours','his','her','hers','its','our','ours','their',
    'theirs','a','an','in','on','of','to','is','it','as','by','at'
])

def extract_keywords(html, top_n=20):
    soup = BeautifulSoup(html, 'html.parser')
    text = " ".join(soup.stripped_strings).lower()
    words = re.findall(r'\b[a-z]{3,}\b', text)
    filtered = [w for w in words if w not in STOPWORDS]
    return Counter(filtered).most_common(top_n)

def now():
    return datetime.now(timezone.utc).isoformat()

# Max concurrent requests per domain
max_concurrent_per_domain = 2

# Semaphore per domain to limit concurrency
domain_semaphores = collections.defaultdict(lambda: asyncio.Semaphore(max_concurrent_per_domain))


#Def URL normalization
def normalize_url(url):
    parsed = urlparse(url)

    # Lowercase scheme and netloc
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()

    # Remove default ports
    if (scheme == 'http' and netloc.endswith(':80')):
        netloc = netloc[:-3]
    elif (scheme == 'https' and netloc.endswith(':443')):
        netloc = netloc[:-4]

    # Ensure path is set, add trailing slash if empty
    path = parsed.path or '/'
    
    # Sort query parameters
    query = urlencode(sorted(parse_qsl(parsed.query)))

    # Remove fragment
    fragment = ''

    normalized = urlunparse((scheme, netloc, path, parsed.params, query, fragment))
    return normalized

#Allowed domain and URL
def is_allowed_domain(url, base_domain):
    parsed = urlparse(url)
    return parsed.netloc == base_domain

def url_allowed(url):
    parsed = urlparse(url)
    path = parsed.path

    # Check include paths
    if include_paths and not any(path.startswith(p) for p in include_paths):
        return False

    # Check exclude regexes
    if any(regex.search(url) for regex in compiled_exclude_regexes):
        return False

    return True

# Optional Playwright support for JS-rendered pages
#try:
#    from playwright.async_api import async_playwright
#    PLAYWRIGHT_AVAILABLE = True
#except ImportError:
#    PLAYWRIGHT_AVAILABLE = False
PLAYWRIGHT_AVAILABLE = False

# Ignore SSL certificate errors globally for aiohttp
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Configure logging
logging.basicConfig(
    filename='crawler.log',
    filemode='a',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
error_logger = logging.getLogger('error_logger')
error_handler = logging.FileHandler('crawler_errors.log')
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)

# Logger for skipped pages
skipped_logger = logging.getLogger('skipped_logger')
skipped_handler = logging.FileHandler('skipped_pages.log')
skipped_handler.setLevel(logging.INFO)
skipped_formatter = logging.Formatter('%(asctime)s SKIPPED: %(message)s')
skipped_handler.setFormatter(skipped_formatter)
skipped_logger.addHandler(skipped_handler)


# Globals updated from config/CLI
seed_url = None
delay_min = None
delay_max = None
batch_size = None
file_type_filters = None
crawl_depth_limit = None
output_format = None
#use_playwright = False

# DB connection globals
conn = None
cur = None

# Rate limiting tracking
last_request_time = collections.defaultdict(lambda: 0)
min_delay_between_requests = None

# Robots parser global
rp = None

# Base domain global
base_domain = None

# Async queue for URLs (local)
url_queue = None

# Counters
commit_counter = 0
count = 0

#Def parse_links
def parse_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        href, _ = urldefrag(href)  # remove URL fragments (optional)
        # Make absolute URLs
        absolute_url = urljoin(base_url, href)
        # Optional: filter URLs by domain or scheme
        parsed = urlparse(absolute_url)
        if parsed.scheme in ('http', 'https'):
            links.add(absolute_url)
    return links

#Def fetch_static
async def fetch_static(session, url, retries=3, backoff=1, max_redirects=5):
    for attempt in range(retries):
        try:
            visited = set()
            current_url = url
            redirects = 0

            while redirects <= max_redirects:
                if current_url in visited:
                    logging.error(f"Redirect loop detected at {current_url}")
                    return None
                visited.add(current_url)

                async with session.get(current_url, ssl=ssl_context, headers={'User-Agent': 'Mozilla/5.0'}) as response:
                    if response.status in (301, 302, 303, 307, 308):
                        location = response.headers.get('Location')
                        if not location:
                            break
                        current_url = urljoin(current_url, location)
                        redirects += 1
                    elif response.status == 200:
                        return await response.text()
                    else:
                        logging.error(f"HTTP error {response.status} at {current_url}")
                        return None

            logging.error(f"Too many redirects for {url}")
            return None

        except aiohttp.ClientError as e:
            logging.error(f"Attempt {attempt+1} failed for {url}: {e}")
            await asyncio.sleep(backoff * (2 ** attempt))
        except Exception as e:
            logging.error(f"Attempt {attempt+1} unknown error for {url}: {e}")
            await asyncio.sleep(backoff * (2 ** attempt))
    logging.error(f"Failed to fetch {url} after {retries} attempts.")
    return None

#Optional Playwright fetch
#async def fetch_dynamic(playwright, url, timeout=15000):
    # Use Playwright to load JS content fully
#    try:
#        browser = await playwright.chromium.launch(headless=True)
#        page = await browser.new_page()
#        await page.goto(url, timeout=timeout)
#        content = await page.content()
#        await browser.close()
#        return content
#    except Exception as e:
#        logging.error(f"Playwright failed to fetch {url}: {e}")
#        return None

#Def fetch existing session
async def fetch_url(session, url):
    # Try static fetch first
    html = await fetch_static(session, url)
    if html and looks_like_content(html):
        return html

    # If Playwright enabled, fallback to dynamic fetch
    #if not use_playwright or not PLAYWRIGHT_AVAILABLE:
    #return html 

#    async with async_playwright() as playwright:
#        dyn_html = await fetch_dynamic(playwright, url)
#        return dyn_html
    
def looks_like_content(html):
 # Check if there is any meaningful visible text
    if not html:
        logging.info("Empty HTML content")
        return False
    soup = BeautifulSoup(html, "html.parser")
    texts = soup.stripped_strings
    combined = ' '.join(texts)
    if len(combined) <= 100:
        logging.info("Fetched content is too short to be meaningful")
        return False
    return True

#Def enqueue_url
async def enqueue_url(queue, url, depth):
    print(f"Enqueueing URL: {url} at depth {depth}")
    await queue.put((url, depth))

#Def dequeue_url
async def dequeue_url(queue):
    try:
        url, depth = await asyncio.wait_for(queue.get(), timeout=1)
        print(f"Dequeued URL from local queue: {url} at depth {depth}")
        return url, depth
    except asyncio.TimeoutError:
        return None, None
    
#Def of compute_hash function    
def compute_hash(content: str) -> str:
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

async def save_product_data(current_url, soup, from_id):
    """
    Saves book product data to the Products table.
    Only processes actual product pages; skips category pages.
    """

    # --- 1. URL pattern check ---
    path = urlparse(current_url).path
    if not re.match(r'^/catalogue/[^/]+/index\.html$', path):
        return  # Not a product page

    # --- 2. Product-specific elements ---
    title_tag = soup.find("h1")
    price_tag = soup.find("p", class_="price_color")
    stock_tag = soup.find("p", class_="instock availability")

    if not title_tag or not price_tag or not stock_tag:
        return  # Missing essential product info

    title = title_tag.get_text(strip=True)

    # Price
    price_text = re.sub(r'[^\d.,]', '', price_tag.get_text(strip=True)).replace(',', '')
    try:
        price = float(price_text)
    except:
        price = None

    # Stock
    match = re.search(r'(\d+)', stock_tag.get_text())
    stock = int(match.group(1)) if match else 1

    # Rating (convert text to number)
    rating_tag = soup.find("p", class_="star-rating")
    rating_map = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5}
    rating = None
    if rating_tag:
        classes = rating_tag.get("class", [])
        text_rating = next((c for c in classes if c.lower() != "star-rating"), None)
        if text_rating:
            rating = rating_map.get(text_rating.lower())

    # Image URL
    img_div = soup.find("div", class_="item active")
    image_url = None
    if img_div and img_div.find("img"):
        src = img_div.find("img").get("src")
        if src:
            image_url = urljoin(current_url, src)

    # --- 3. Save to database ---
    try:
        async with db_lock:
            if from_id is None:
                cur.execute('SELECT id FROM Urls WHERE name = ?', (current_url,))
                row = cur.fetchone()
                from_id = row[0] if row else None

            if from_id is None:
                logging.warning(f"No URL ID found for product page {current_url}; skipping save.")
                return

            cur.execute('''
                INSERT OR REPLACE INTO Products (url_id, title, price, stock, rating, image_url)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (from_id, title, price, stock, rating, image_url))
            conn.commit()
            print(f"💾 Product saved: {title} | price={price} | stock={stock} | rating={rating}")

    except Exception as e:
        error_logger.error(f"Failed to save product for {current_url}: {e}")



# Worker coroutine
async def worker(session):
    while True:
        currenturl, currentdepth = await dequeue_url(url_queue)
        if currenturl is None:
            await asyncio.sleep(1)
            continue

        skip_reason = None  # Track why the page is skipped
        html = None

        try:
            # --- Apply include paths filter ---
            if include_paths and not any(path in urlparse(currenturl).path for path in include_paths):
                skip_reason = "Does not match include paths"

            # --- Apply exclude patterns filter ---
            elif any(regex.search(currenturl) for regex in compiled_exclude_regexes):
                skip_reason = "Matches exclude pattern"

            # --- Check robots.txt ---
            elif not rp.can_fetch("*", currenturl):
                skip_reason = "Blocked by robots.txt"

            # --- Fetch page ---
            else:
                domain = urlparse(currenturl).netloc
                semaphore = domain_semaphores[domain]
                async with semaphore:
                    html = await fetch_url(session, currenturl)

                # Optional delay between requests
                if delay_min is not None and delay_max is not None:
                    await asyncio.sleep(random.uniform(delay_min, delay_max))

                # --- Validate content ---
                if not html:
                    skip_reason = "Fetch failed or empty content"
                else:
                    content_length = len(html)
                    if content_length < min_content_length or content_length > max_content_length:
                        skip_reason = f"Content length out of range ({content_length})"
                    else:
                        # Check if content unchanged
                        new_hash = compute_hash(html)
                        async with db_lock:
                            cur.execute('SELECT content_hash FROM Urls WHERE name=?', (currenturl,))
                            row = cur.fetchone()
                            old_hash = row[0] if row else None

                        if new_hash == old_hash:
                            skip_reason = "Content unchanged"

            # --- Handle skipped pages ---
            if skip_reason:
                skipped_logger.info(f"{currenturl} | Reason: {skip_reason}")
                async with db_lock:
                    cur.execute(
                        'UPDATE Urls SET date = ? WHERE name = ?',
                        (f'SKIPPED - {skip_reason}', currenturl)
                    )
                    conn.commit()
                continue  # URL processed, move to next

            # --- Page is valid; process content ---
            logging.info(f"Processing page: {currenturl}")

            # Update content hash and date
            new_hash = compute_hash(html)
            async with db_lock:
                cur.execute(
                    'UPDATE Urls SET content_hash = ?, date = ? WHERE name = ?',
                    (new_hash, now(), currenturl)
                )
                conn.commit()

            # --- Parse links ---
            links = parse_links(html, currenturl)
            async with db_lock:
                # Get current URL ID for Links table
                cur.execute('SELECT id FROM Urls WHERE name=?', (currenturl,))
                from_row = cur.fetchone()
                from_id = from_row[0] if from_row else None

                for link in links:
                    normalized_link = normalize_url(link)
                    if not is_allowed_domain(normalized_link, base_domain):
                        continue

                    next_depth = currentdepth + 1
                    if crawl_depth_limit is None or next_depth <= crawl_depth_limit:
                        cur.execute('SELECT date FROM Urls WHERE name=?', (normalized_link,))
                        row = cur.fetchone()
                        if row is None or row[0] is None:
                            cur.execute('INSERT OR IGNORE INTO Urls (name) VALUES (?)', (normalized_link,))
                            await enqueue_url(url_queue, normalized_link, next_depth)

                    # Insert relationship into Links table
                    if from_id is not None:
                        cur.execute('SELECT id FROM Urls WHERE name=?', (normalized_link,))
                        to_row = cur.fetchone()
                        if to_row:
                            to_id = to_row[0]
                            cur.execute(
                                'INSERT OR IGNORE INTO Links (from_id, to_id) VALUES (?, ?)',
                                (from_id, to_id)
                            )

            # --- Parse HTML for product data and keywords ---
            soup = BeautifulSoup(html, 'html.parser')
            await save_product_data(currenturl, soup, from_id)

            # --- Extract category ---
            breadcrumbs = soup.find('ul', class_='breadcrumb')
            category = None
            if breadcrumbs:
                li_tags = breadcrumbs.find_all('li')
                if len(li_tags) >= 3:
                    category_tag = li_tags[-2].find('a')
                    if category_tag:
                        category = category_tag.get_text(strip=True)

            # --- Extract keywords ---
            keywords = extract_keywords(html)

            # --- Save category and keywords ---
            if from_id is not None:
                async with db_lock:
                    if category:
                        cur.execute(
                            'INSERT OR IGNORE INTO Category (url_id, name) VALUES (?, ?)',
                            (from_id, category)
                        )
                        logging.info(f"Category '{category}' stored for URL id {from_id}")

                    for keyword, count in keywords:
                        cur.execute(
                            'INSERT OR REPLACE INTO PageKeywords (url_id, keyword, count) VALUES (?, ?, ?)',
                            (from_id, keyword, count)
                        )

                    # Update crawl date
                    cur.execute('UPDATE Urls SET date = ? WHERE name = ?', (now(), currenturl))
                    conn.commit()

        finally:
            # Mark the queue item as done, whether skipped or processed
            url_queue.task_done()



#Def main()
async def main(resume=False):
    logging.info("main started")
    global base_domain, rp, conn, cur, min_delay_between_requests, url_queue

    # Setup DB connection
    conn = sqlite3.connect('mini.sqlite', check_same_thread=False, timeout=30)
    cur = conn.cursor()

    # make a global lock
    global db_lock
    db_lock = asyncio.Lock()

    # Create tables if not exist
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS Urls (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        date TEXT,
        content_hash TEXT,
        changer INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS Links (
        from_id INTEGER,
        to_id INTEGER,
        PRIMARY KEY (from_id, to_id)
    );
    
    CREATE TABLE IF NOT EXISTS Category (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url_id INTEGER,
        name TEXT
    );
                    
    CREATE TABLE IF NOT EXISTS PageKeywords (
        url_id INTEGER,
        keyword TEXT,
        count INTEGER,
        PRIMARY KEY (url_id, keyword)
    );
                      
    CREATE TABLE IF NOT EXISTS Products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url_id INTEGER,
    title TEXT,
    price REAL,
    stock INTEGER,
    rating TEXT,
    image_url TEXT
    );
                      
''')                        

    # Setup robots.txt parser
    parsed_seed = urlparse(seed_url)
    robots_url = f"{parsed_seed.scheme}://{parsed_seed.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception as e:
        logging.error(f"Could not read robots.txt: {e}")

    #Delay + robots.txt delay
    min_delay_between_requests = delay_min or 0
    crawl_delay = rp.crawl_delay('*')
    if crawl_delay is not None:
        min_delay_between_requests = crawl_delay


    # Setup queue (local)
    url_queue = asyncio.Queue()

    # Load URLs into queue according to resume flag
    if resume:
        cur.execute('SELECT name FROM Urls WHERE date IS NULL')
        unfinished_urls = cur.fetchall()
        if unfinished_urls:
            for (url,) in unfinished_urls:
                await enqueue_url(url_queue, url, 0)
        else:
            # If none unfinished, enqueue seed_url
            cur.execute('INSERT OR IGNORE INTO Urls (name) VALUES (?)', (seed_url,))
            conn.commit()
            await enqueue_url(url_queue, seed_url, 0)
    else:
        # Fresh start
        cur.execute('INSERT OR IGNORE INTO Urls (name) VALUES (?)', (seed_url,))
        conn.commit()
        await enqueue_url(url_queue, seed_url, 0)

   
    global workers_count
    workers_count = batch_size or 1

    # Create the aiohttp session here
    async with aiohttp.ClientSession() as session:

        # Start workers with session argument
        #workers = [asyncio.create_task(worker(session)) for _ in range(workers_count)]
        worker_tasks = [asyncio.create_task(worker(session)) for _ in range(workers_count)]

        # Wait until local queue is empty
        await url_queue.join()

        # Cancel workers
        for w in worker_tasks:
            w.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Export results
    if output_format == 'json':
        export_to_json(conn)
    elif output_format == 'csv':
        export_to_csv(conn)
    else:
        logging.info("Output stored in SQLite database")

    conn.close()

#Def export JSON
def export_to_json(db_conn, filename='export.json'):
    cur = db_conn.cursor()
    cur.execute('''
    SELECT Urls.id, Urls.name, Urls.date, Category.name
    FROM Urls
    LEFT JOIN Category ON Urls.id = Category.url_id
    ''')

    results = {}
    for url_id, url_name, url_date, category in cur.fetchall():
        if url_name not in results:
            results[url_name] = {
                'date': url_date,
                'categories': []
            }
        if category and category not in results[url_name]['categories']:
            results[url_name]['categories'].append(category)

    data = [{'url': k, 'date': v['date'], 'categories': v['categories']} for k, v in results.items()]

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    logging.info(f"Exported crawl results to {filename}")

#Def export CSV
def export_to_csv(db_conn, filename='export.csv'):
    import csv
    
    cur = db_conn.cursor()
    
    cur.execute("""
    SELECT Urls.name, Urls.date, GROUP_CONCAT(Category.name, '; ')
    FROM Urls
    LEFT JOIN Category ON Urls.id = Category.url_id
    GROUP BY Urls.id
    """
)

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['URL', 'Date', 'Categories'])
        for row in cur.fetchall():
            writer.writerow(row)

    logging.info(f"Exported crawl results to {filename}")


#####################################
#Command line interface section
#####################################

def cli_main():
    logging.info("cli_main started")
    global seed_url, delay_min, delay_max, batch_size, file_type_filters, crawl_depth_limit, output_format, use_playwright, include_paths, compiled_exclude_regexes, min_content_length, max_content_length, base_domain, domain_semaphores

    with open('config.json') as f:
        config = json.load(f)

    seed_url = config['seed_url']
    delay_min, delay_max = config['delay_range']
    batch_size = config['batch_size']
    file_type_filters = tuple(config['file_type_filters'])
    crawl_depth_limit = config.get('crawl_depth_limit', None)
    output_format = config.get('output_format', 'sqlite')
    #use_redis = config.get('use_redis', False)
    #use_playwright = config.get('use_playwright', False)
    #redis_url = config.get('redis_url', 'redis://localhost')
    max_concurrent_per_domain = config.get('max_concurrent_per_domain', 2)

    include_paths = config.get('include_paths', [])
    exclude_patterns = config.get('exclude_patterns', [])
    compiled_exclude_regexes = [re.compile(p) for p in exclude_patterns]
    min_content_length = config.get('min_content_length', 0)
    max_content_length = config.get('max_content_length', 10_000_000)

    parser = argparse.ArgumentParser(description='Async Web Crawler')
    parser.add_argument('--domain', type=str, default=None, help='Domain to crawl (overrides config)')
    parser.add_argument('--depth', type=int, default=None, help='Crawl depth limit')
    parser.add_argument('--output', type=str, choices=['sqlite', 'json', 'csv'], default=None, help='Output format')
    parser.add_argument('--resume', action='store_true', help='Resume crawling from unfinished URLs')
    #parser.add_argument('--redis', action='store_true', help='Use Redis queue for distributed crawling')
    #parser.add_argument('--playwright', action='store_true', help='Use Playwright for dynamic content fetching')

    args = parser.parse_args()

    # Initialize semaphores after reading config
    domain_semaphores = collections.defaultdict(lambda: asyncio.Semaphore(max_concurrent_per_domain))

    # Override seed_url with CLI domain if provided
    if args.domain:
        seed_url = args.domain

    # Normalize seed_url and get base_domain after override
    seed_url = normalize_url(seed_url)
    base_domain = urlparse(seed_url).netloc

    if args.depth is not None:
        crawl_depth_limit = args.depth
    if args.output:
        output_format = args.output
    #if args.playwright:
    #    use_playwright = True

    asyncio.run(main(resume=True))

if __name__ == "__main__":
    cli_main()

#####################################
#####################################