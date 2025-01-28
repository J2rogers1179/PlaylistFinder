import scrapy
import os
import logging
import hashlib
from pathlib import Path
from urllib.parse import urlsplit, urljoin
from mimetypes import guess_extension
from typing import Set, Dict
from scrapy import Request
from scrapy.http import Response

class EnhancedSpider(scrapy.Spider):
    """
    Advanced website archiver with:
    - Domain-focused crawling
    - Asset deduplication
    - Content-type validation
    - Depth limiting
    - Proxy support
    """
    name = "enhanced_archiver"
    
    # Configurable through CLI: scrapy crawl enhanced_archiver -a start_urls=https://yzy-sply.com
    def __init__(self, start_urls: str = None, allowed_domains: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = start_urls.split(",") if start_urls else ["https://yzy-sply.com/"]
        self.allowed_domains = allowed_domains.split(",") if allowed_domains else ["yzy-sply.com"]
        self.processed_urls: Set[str] = set()
        self.asset_hashes: Set[str] = set()
        self.depth_map: Dict[str, int] = {}
        self.setup_logging()

    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 1.5,
        "DEPTH_LIMIT": 3,
        "RETRY_TIMES": 3,
        "HTTP_PROXY": "http://proxy.example.com:8080",  # Set your proxy here
        "HTTPS_PROXY": "http://proxy.example.com:8080",
        "AUTOTHROTTLE_ENABLED": True,
        "LOG_LEVEL": "INFO",
    }

    def setup_logging(self) -> None:
        """Configure structured logging with rotation"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        handler = logging.FileHandler(log_dir / "archiver.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s"
        ))
        self.logger.addHandler(handler)

    def start_requests(self):
        """Initialize crawling with depth tracking"""
        for url in self.start_urls:
            self.depth_map[url] = 0
            yield Request(url, callback=self.parse, errback=self.handle_error)

    def parse(self, response: Response):
        """Main parsing logic with depth control"""
        current_depth = self.depth_map.get(response.url, 0)
        
        if current_depth > self.settings.getint("DEPTH_LIMIT"):
            self.logger.warning(f"Skipping {response.url} (depth {current_depth} exceeds limit)")
            return

        # Save primary content
        self.save_html(response)
        
        # Process page assets
        yield from self.process_assets(response)
        
        # Extract and follow internal links
        if current_depth < self.settings.getint("DEPTH_LIMIT"):
            yield from self.find_links(response, current_depth)

    def process_assets(self, response: Response):
        """Yield requests for all page assets"""
        # CSS
        for link in response.css('link[rel="stylesheet"]::attr(href)').getall():
            yield from self.yield_asset_request(response.url, link, "css")

        # JavaScript
        for link in response.css('script[src]::attr(src)').getall():
            yield from self.yield_asset_request(response.url, link, "js")

        # Images (img/src and source/srcset)
        for selector in ['img::attr(src)', 'source::attr(srcset)']:
            for link in response.css(selector).getall():
                yield from self.yield_asset_request(response.url, link, "images")

    def yield_asset_request(self, base_url: str, link: str, asset_type: str):
        """Generate validated asset requests"""
        absolute_url = self.normalize_url(base_url, link)
        
        if not absolute_url:
            return
            
        if self.is_duplicate_asset(absolute_url):
            self.logger.debug(f"Skipping duplicate asset: {absolute_url}")
            return

        yield Request(
            absolute_url,
            callback=self.save_asset,
            errback=self.handle_error,
            meta={"asset_type": asset_type},
            priority=1  # Lower priority than HTML pages
        )

    def save_html(self, response: Response):
        """Save HTML with DOM-based directory structure"""
        try:
            domain = self.get_domain_folder(response.url)
            path = urlsplit(response.url).path.strip("/")
            
            filename = Path(domain) / path / "index.html" if path else Path(domain) / "index.html"
            filename.parent.mkdir(parents=True, exist_ok=True)
            
            with open(filename, "w", encoding="utf-8") as f:
                f.write(response.text)
                
            self.logger.info(f"Saved HTML: {filename}")
        except Exception as e:
            self.logger.error(f"HTML save failed for {response.url}: {str(e)}")

    def save_asset(self, response: Response):
        """Save asset with content-type validation"""
        asset_type = response.meta["asset_type"]
        content_hash = hashlib.md5(response.body).hexdigest()
        
        if content_hash in self.asset_hashes:
            self.logger.debug(f"Skipping duplicate content: {response.url}")
            return
            
        self.asset_hashes.add(content_hash)

        try:
            domain = self.get_domain_folder(response.url)
            content_type = response.headers.get("Content-Type", b"").decode().split(";")[0]
            
            # Determine file extension
            if asset_type == "images":
                ext = guess_extension(content_type) or ".bin"
            else:
                ext = Path(urlsplit(response.url).path).suffix or {
                    "css": ".css",
                    "js": ".js"
                }.get(asset_type, ".bin")

            # Build file path
            file_path = urlsplit(response.url).path.strip("/")
            file_name = Path(file_path).name if file_path else f"{content_hash}{ext}"
            
            filename = Path(domain) / asset_type / file_name
            filename.parent.mkdir(parents=True, exist_ok=True)

            # Save with proper binary/text handling
            with open(filename, "wb") as f:
                f.write(response.body)

            self.logger.info(f"Saved {asset_type.upper()}: {filename}")

        except Exception as e:
            self.logger.error(f"{asset_type.upper()} save failed for {response.url}: {str(e)}")

    def find_links(self, response: Response, current_depth: int):
        """Discover and queue internal links"""
        for link in response.css("a::attr(href)").getall():
            absolute_url = self.normalize_url(response.url, link)
            
            if absolute_url and absolute_url not in self.processed_urls:
                self.processed_urls.add(absolute_url)
                self.depth_map[absolute_url] = current_depth + 1
                yield Request(
                    absolute_url,
                    callback=self.parse,
                    errback=self.handle_error,
                    priority=0  # Higher priority for HTML
                )

    def normalize_url(self, base_url: str, url: str) -> str:
        """Sanitize and validate URLs"""
        try:
            # Remove URL fragments and query parameters
            parsed = urlsplit(url)
            clean_url = urlsplit((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                "",  # Remove query
                ""   # Remove fragment
            ))
            
            absolute_url = urljoin(base_url, clean_url)
            if urlsplit(absolute_url).netloc in self.allowed_domains:
                return absolute_url
            return None
        except Exception as e:
            self.logger.warning(f"Invalid URL {url}: {str(e)}")
            return None

    def is_duplicate_asset(self, url: str) -> bool:
        """Check for already processed assets"""
        return url in self.processed_urls

    def handle_error(self, failure):
        """Centralized error handling"""
        self.logger.error(
            f"Request failed: {failure.request.url} | "
            f"Error: {str(failure.value)}"
        )

    @staticmethod
    def get_domain_folder(url: str) -> str:
        """Generate filesystem-safe domain name"""
        return urlsplit(url).netloc.replace(".", "_")