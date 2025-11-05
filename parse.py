import re
import hashlib
import logging

from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode, urldefrag
from bs4 import BeautifulSoup
from collections import Counter

#Def URL normalization
def normalize_url(url):
    """Normalize a URL by removing fragments, trailing slashes, and standardizing format.
    Ensures consistent URL comparison and storage.
    """
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

#Def Allowed domain
def is_allowed_domain(url, base_domain):
    """Check if a URL belongs to the allowed domain for crawling.
    Prevents the crawler from navigating to external sites.
    """
    parsed = urlparse(url)
    return parsed.netloc == base_domain

#Def Allowed URL
def url_allowed(url, include_paths, compiled_exclude_regexes):
    """Determine if a URL is allowed according to include/exclude rules,
    content length limits, and crawl depth.
    """
    parsed = urlparse(url)
    path = parsed.path

    # Check include paths
    if include_paths and not any(path.startswith(p) for p in include_paths):
        return False

    # Check exclude regexes
    if any(regex.search(url) for regex in compiled_exclude_regexes):
        return False

    return True

#Def should skip URL
async def should_skip_url(currenturl, ctx):
    """Determine if a URL should be skipped based on previous visits, database status,
    robots.txt, and configured rules.
    """
    # 1. Check URL patterns
    if not url_allowed(currenturl, ctx.rules["include_paths"], ctx.rules["exclude_regexes"]):
        return True, "URL not allowed by include/exclude rules"
    
    # 2. Check robots.txt (if enabled)
    if ctx.rules.get("respect_robots_txt", True):
        rp = ctx.rules.get("rp")
        user_agent = ctx.rules.get("user_agent", "*")
        if rp and not rp.can_fetch(user_agent, currenturl):
            return True, "Blocked by robots.txt"

    # 3. Check DB for already fetched
    async with ctx.db["lock"]:
        ctx.db["cur"].execute('SELECT date FROM Urls WHERE name=?', (currenturl,))
        row = ctx.db["cur"].fetchone()
        if row and row[0] is not None:
            return True, "Already fetched"

    # Not skipped
    return False, None

#Def parse_links
def parse_links(html, base_url):
    """Extract all links from the given HTML content and convert them into absolute URLs.
    Returns URLs to potentially enqueue for further crawling.
    """
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

#Def EXTRACT_KEYWORDS and HASH
def extract_keywords(html, top_n=20):
    """Analyze HTML content to extract meaningful keywords for indexing or storage.
    """
    #Set up keywords
    STOPWORDS = set([
        'the','and','for','are','this','that','with',
        'from','was','were',
        'will','would','shall','should','can','could','have','has','had',
        'you','your','yours','his','her','hers','its','our','ours','their',
        'theirs','a','an','in','on','of','to','is','it','as','by','at'
])
    soup = BeautifulSoup(html, 'html.parser')
    text = " ".join(soup.stripped_strings).lower()
    words = re.findall(r'\b[a-z]{3,}\b', text)
    filtered = [w for w in words if w not in STOPWORDS]
    return Counter(filtered).most_common(top_n)

#Def Compute_hash
def compute_hash(html):
    """Compute a hash of the page content to detect duplicates or track changes.
    """
    return hashlib.sha256(html.encode('utf-8')).hexdigest()


#Def save_product_data
def get_product_data(current_url, soup):
    """
    Saves book product data to a products data object.
    Only processes actual product pages; skips category pages.
    """

    logging.info(f"save_product_data called for: {current_url}")
    parsed = urlparse(current_url)
    logging.info(f"  parsed.path = {repr(parsed.path)}")

    # --- 1. URL pattern check ---
    path = parsed.path.strip()
    logging.info(f"save_product_data reached: {current_url} | path={path}")
    if not re.search(r'/catalogue/[^/]+(/index\.html)?$', path):
        #logging.info(f"Skipping {current_url} (did not match product pattern)")
        return  # Not a product page

    # --- 2. Product-specific elements ---
    title_tag = soup.find("h1")
    price_tag = soup.find("p", class_="price_color")
    stock_tag = soup.find("p", class_="instock availability")

    if not title_tag or not price_tag or not stock_tag:
        logging.info(f"Missing product elements on {current_url}: title={bool(title_tag)} price={bool(price_tag)} stock={bool(stock_tag)}")
        return  # Missing essential product info

    title = title_tag.get_text(strip=True)

    # Price
    price_text = re.sub(r'[^\d.,]', '', price_tag.get_text(strip=True)).replace(',', '')
    try:
        price = float(price_text)
    except Exception:
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

    product_data = {
        "title": title,
        "price": price,
        "stock": stock,
        "rating": rating,
        "image_url": image_url
    }

    return product_data


def extract_category(soup):
    """Identify the category of a product or page from its HTML structure.
    """
    # --- Extract category ---
    if soup:
        breadcrumbs = soup.find('ul', class_='breadcrumb')
    else: 
        breadcrumbs = None

    category = None

    if breadcrumbs:

        li_tags = breadcrumbs.find_all('li')
        if len(li_tags) >= 3:
            category_tag = li_tags[-2].find('a')
            if category_tag:
                category = category_tag.get_text(strip=True)

    return category

def process_page(ctx, currenturl, html, currentdepth):
    """
    Process the HTML of a page: parse links, extract metadata, normalize links,
    and decide which links should be enqueued.

    Returns:
        to_enqueue: list of (normalized_link, next_depth) tuples
        link_pairs: list of normalized links (for DB relationships)
        soup: BeautifulSoup object (or None)
        keywords: list
        category: string or None
        product_data: dict or None
    """
        
    ctx.logger.info(f"Processing page: {currenturl}")

    # Parse links
    links = parse_links(html, currenturl)
    print(f"Found {len(links)} links on {currenturl}:")

    # Parse metadata outside lock
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        keywords = extract_keywords(html)
        category = extract_category(soup) if soup else None
        product_data = get_product_data(currenturl, soup) if soup else None
    
    else:
        soup = None
        keywords = []

    # Prepare normalized links and depth decisions
    to_enqueue = []
    link_pairs = [] 

    #Parse all the links from the page
    for link in links:
        normalized_link = normalize_url(link)
        #ctx.logger.info(f"Normalized URL: {normalized_link}")
        if not is_allowed_domain(normalized_link, ctx.rules["base_domain"]):
            print('Domain not allowed')
            continue

        next_depth = currentdepth + 1

        if ctx.rules["crawl_depth_limit"] is None or next_depth <= ctx.rules["crawl_depth_limit"]:

                to_enqueue.append((normalized_link, next_depth))
                # Insert relationship for all links (even if depth prevented enqueue)
                link_pairs.append(normalized_link)

    return to_enqueue, link_pairs, soup, keywords, category, product_data
