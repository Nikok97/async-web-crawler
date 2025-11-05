import logging
import aiohttp
import asyncio
import ssl

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timezone

#Def Now
def now():
    """Return the actual date, hour and timezone."""
    return datetime.now(timezone.utc).isoformat()

#Def fetch_static
async def fetch_static(ctx, session, url, backoff=1):

    """Fetch a static HTML page using aiohttp with retries, backoff and redirect handling."""

    timeout_seconds = ctx.rules.get("timeout_seconds", 10)
    retries = ctx.rules.get("retries", 3)
    max_redirects = int(ctx.rules.get("max_redirects", 5))

    # Ignore SSL certificate errors globally for aiohttp
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    for attempt in range(retries):
        try:
            visited = set()
            current_url = url
            redirects = 0

            while redirects <= max_redirects:
                if current_url in visited:
                    logging.info(f"Redirect loop detected at {current_url}")
                    return None
                visited.add(current_url)

                timeout = aiohttp.ClientTimeout(total=timeout_seconds)

                async with session.get(current_url, ssl=ssl_context, headers={"User-Agent": ctx.rules["user_agent"]}, timeout=timeout) as response:

                    if response.status in (301, 302, 303, 307, 308):
                        location = response.headers.get('Location')
                        if not location:
                            break
                        current_url = urljoin(current_url, location)
                        redirects += 1
                    elif response.status == 200:
                        return await response.text()
                    else:
                        ctx.logger.info(f"HTTP error {response.status} at {current_url}")
                        return None

            ctx.logger.info(f"Too many redirects for {url}")

            return None

        except aiohttp.ClientError as e:
            ctx.logger.info(f"Attempt {attempt+1} failed for {url}: {e}")
            await asyncio.sleep(backoff * (2 ** attempt))
        except Exception as e:
            ctx.logger.info(f"Attempt {attempt+1} unknown error for {url}: {e}")
            await asyncio.sleep(backoff * (2 ** attempt))
    ctx.logger.info(f"Failed to fetch {url} after {retries} attempts.")

    return None

#Optional Playwright fetch: *******TE FALTA REVISTAR ESTO******
async def fetch_dynamic(playwright, url, timeout=15000):

    """Fetch HTML content of a page rendered with JavaScript using Playwright."""
    # Use Playwright to load JS content fully
    try:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, timeout=timeout)
        content = await page.content()
        await browser.close()
        return content
    except Exception as e:
        logging.error(f"Playwright failed to fetch {url}: {e}")
        return None

    
def looks_like_content(html):

    """Check if fetched HTML content contains meaningful visible text."""
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

#Def fetch existing session
async def fetch_url(ctx, session, url):
    """Fetch a URL using static fetch first, and optionally dynamic fetch via Playwright if enabled."""
    # Try static fetch first
    #print(f"Fetching {url} with UA: {ctx.user_agent}")
    html = await fetch_static(ctx, session, url)
    #print(f"fetch_static returned: {len(html) if html else 'None'}")
    if html and looks_like_content(html):
        return html

    # If Playwright enabled, fallback to dynamic fetch
    if not use_playwright or not PLAYWRIGHT_AVAILABLE:
        return html 

    async with async_playwright() as playwright:
        dyn_html = await fetch_dynamic(playwright, url)
        return dyn_html

