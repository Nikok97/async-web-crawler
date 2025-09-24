# Async Web Crawler

An **asynchronous web crawler** built with Python and aiohttp.
The crawler stores results in SQLite by default and can also export to JSON or CSV. It fetches pages asynchronously, stores URLs, categories, and keywords, and optionally scrapes product data. In addition, the crawler respects robots.txt, avoids duplicate requests, and can resume unfinished crawls.

Note: as a personal project, some sections are **hardcoded for https://books.toscrape.com**. Adapting to other sites would require minor changes.

---

## Features

- **Asynchronous crawling** with `aiohttp` and `asyncio`
- **Configurable crawl depth, rate limits, and file type filters** via `config.json` and CLI flags  
- **Robots.txt** compliance (via RobotFileParser).
- **SQLite database** with tables for URLs, links, categories, keywords, and products
- **Resume crawling** from unfinished URLs
- **Keyword extraction** with **stopword filtering**
- **Product page scraper** (example: books catalogue)
- **Export results** to SQLite, JSON, or CSV
- **Handles redirects, errors, and SSL issues**

---

## Requirements

- Python 3.9+
- Install dependencies:

    ```pip install aiohttp beautifulsoup4 apscheduler```

---

## Configuration

- Crawler settings are stored in a config.json file. Example:

```json
{
  "seed_url": "https://books.toscrape.com",
  "delay_range": [1, 3],
  "batch_size": 2,
  "file_type_filters": [".jpg", ".png", ".gif", ".pdf"],
  "crawl_depth_limit": 10,
  "output_format": "sqlite",
  "max_concurrent_per_domain": 2,
  "include_paths": [],  
  "exclude_patterns": ["\\.pdf$", "\\.jpg$", "/private/.*"],
  "min_content_length": 100,
  "max_content_length": 100000
}
```

---

## Usage

- CLI configurable. 
- Example:

```python crawler.py --domain https://books.toscrape.com/ --depth 2```

### CLI Options:

- `--domain` : Override seed URL from config  
- `--depth` : Set crawl depth limit  
- `--resume` : Resume crawling from unfinished URLs  

**Note**: The crawler stores results in mini.sqlite by default. To export to JSON or CSV, run export.py.

---    

## Database schema

- Urls: crawled URLs, timestamps, content hash

- Links: relationships between pages

- Category: extracted categories

- PageKeywords: keywords and counts

- Products: scraped product data (title, price, stock, rating, image)

---

## Export

- sqlite (default): Results stored in `mini.sqlite`
- JSON / CSV: Run `export.py` to export products from `mini.sqlite` to JSON (`export.json`) or CSV (`export.csv`).

---

## Logs

- crawler.log: general activity

- crawler_errors.log: errors during crawling

- skipped_pages.log: skipped URLs with reasons

---

## Quick Start

1. Install dependencies: `pip install aiohttp beautifulsoup4 apscheduler`
2. Configure `config.json` (domain, depth, batch size)
3. Run the crawler (results stored in mini.sqlite by default): `python crawler.py`
4. Export results to JSON (or CSV) (optional):
   run `python export.py`

--

## License

This project is licensed under the MIT License – see the LICENSE file for details.

---

## Contributions & Issues

Feel free to open issues or pull requests!
For major changes, please open an issue first to discuss.

---

## Acknowledgements

- Uses aiohttp library and BeatifulSoup.

- Inspired by best practices in asynchronous crawling and web scraping.

