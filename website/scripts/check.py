#!/usr/bin/env python3
"""Checks the built site in website/dist. Stdlib only; run after build.sh.

Usage: python3 website/scripts/check.py
"""

import json
import os
import pathlib
import re
import sys
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from urllib.parse import unquote, urlsplit

HERE = pathlib.Path(__file__).resolve().parent
WEBSITE = HERE.parent
REPO = WEBSITE.parent
DIST = WEBSITE / "dist"
DOCS = DIST / "docs"
BRAND = REPO / "thadam-assets" / "brand"
ORIGIN = os.environ.get("SITE_ORIGIN", "https://thadam.dineshkarthik.me").rstrip("/")

REQUIRED = [
    "index.html",
    "404.html",
    "robots.txt",
    "sitemap.xml",
    "docs/index.html",
    "assets/site.css",
    "assets/site.js",
    "assets/brand/tokens/thadam.css",
    "assets/brand/svg/thadam-logo-light.svg",
    "assets/brand/svg/thadam-logo-dark.svg",
    "assets/brand/svg/thadam-icon-light.svg",
    "assets/brand/png/favicon-32.png",
    "assets/brand/png/favicon-180.png",
    "assets/brand/png/favicon-192.png",
    "assets/brand/png/favicon-512.png",
    "assets/brand/png/og-card.png",
    "site.webmanifest",
]
NOINDEX_PAGES = {"docs/print.html", "docs/404.html", "docs/toc.html"}
HOMEPAGE_BUDGET_BYTES = 30 * 1024
HEX_RE = re.compile(r"#(?:[0-9A-Fa-f]{3}){1,2}\b")
FRAGMENT_PAGES = {"docs/toc.html"}
FAVICON_GUARDS = {
    "favicon-*.svg": "svg/thadam-icon-light.svg",
    "favicon-*.png": "png/favicon-192.png",
}


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.in_title = False
        self.viewport = False
        self.canonical = None
        self.og_url = None
        self.refs = []
        self.in_pre = False
        self.missing_alt = 0
        self.og_image = None
        self.json_ld = []
        self.in_json_ld = False
        self._ld_buffer = ""

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "img" and "alt" not in a:
            self.missing_alt += 1
        if tag == "script" and (a.get("type") or "") == "application/ld+json":
            self.in_json_ld = True
            self._ld_buffer = ""
        if tag == "title":
            self.in_title = True
        elif tag == "meta":
            if (a.get("name") or "").lower() == "viewport":
                self.viewport = True
            if a.get("property") == "og:url":
                self.og_url = a.get("content")
            if a.get("property") == "og:image":
                self.og_image = a.get("content")
        elif tag == "link":
            if "canonical" in (a.get("rel") or "").split():
                self.canonical = a.get("href")
            if a.get("href"):
                self.refs.append(("href", a["href"]))
        elif tag in ("a", "area"):
            if a.get("href"):
                self.refs.append(("href", a["href"]))
        elif tag in ("img", "script", "source", "iframe", "video", "audio"):
            if a.get("src"):
                self.refs.append(("src", a["src"]))
            if a.get("srcset"):
                self.refs.append(("srcset", a["srcset"]))

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False
        if tag == "script" and self.in_json_ld:
            self.json_ld.append(self._ld_buffer)
            self.in_json_ld = False

    def handle_data(self, data):
        if self.in_title:
            self.title += data
        if self.in_json_ld:
            self._ld_buffer += data


def docs_expected(rel):
    if rel in ("index.html", "introduction.html"):
        return "/docs/"
    if rel.endswith("/index.html"):
        return "/docs/" + rel[: -len("index.html")]
    return "/docs/" + rel[: -len(".html")]


def resolve(page_dir_rel, target):
    path = urlsplit(target).path
    if not path:
        return None
    path = unquote(path)
    if path.startswith("/"):
        base = DIST
        rel = path.lstrip("/")
    else:
        base = DIST / page_dir_rel
        rel = path
    candidate = (base / rel).resolve()
    if rel == "" or rel.endswith("/"):
        candidate = candidate / "index.html"
    return candidate


def main():
    errors = []
    if not DIST.is_dir():
        print(f"missing {DIST}; run website/scripts/build.sh first", file=sys.stderr)
        return 1

    for rel in REQUIRED:
        if not (DIST / rel).is_file():
            errors.append(f"missing required file: {rel}")

    for pattern, brand_rel in FAVICON_GUARDS.items():
        copies = sorted(DOCS.glob(pattern))
        if not copies:
            errors.append(f"docs has no hashed {pattern} favicon")
        for copy in copies:
            if copy.read_bytes() != (BRAND / brand_rel).read_bytes():
                errors.append(f"{copy.relative_to(DIST)} differs from thadam-assets/brand/{brand_rel}")

    pages = sorted(DIST.rglob("*.html"))
    expected_docs = set()
    for page in pages:
        rel = page.relative_to(DIST).as_posix()
        text = page.read_text(encoding="utf-8")
        parser = PageParser()
        parser.feed(text)
        page_dir_rel = page.parent.relative_to(DIST)

        if rel not in FRAGMENT_PAGES:
            if not parser.title.strip():
                errors.append(f"{rel}: empty <title>")
            if not parser.viewport:
                errors.append(f"{rel}: missing viewport meta")
        if parser.missing_alt:
            errors.append(f"{rel}: {parser.missing_alt} <img> without alt")
        for block in parser.json_ld:
            try:
                json.loads(block)
            except json.JSONDecodeError as exc:
                errors.append(f"{rel}: JSON-LD does not parse: {exc}")
        if rel == "index.html":
            if not parser.json_ld:
                errors.append("index.html: missing JSON-LD")
            if parser.og_image is None:
                errors.append("index.html: og:image missing")
        if rel.startswith("docs/") and rel not in NOINDEX_PAGES and "og:title" not in text:
            errors.append(f"{rel}: missing og:title")
        if HEX_RE.search(text) and rel == "index.html":
            errors.append(f"{rel}: hex colour outside the tokens")
        if "THADAM_CANONICAL" in text or "__SITE_ORIGIN__" in text:
            errors.append(f"{rel}: unreplaced build placeholder")
        if rel.startswith("docs/") and rel not in NOINDEX_PAGES:
            expected_url = ORIGIN + docs_expected(rel[len("docs/"):])
            if parser.canonical != expected_url:
                errors.append(f"{rel}: canonical is {parser.canonical!r}, expected {expected_url!r}")
            if parser.canonical and parser.canonical.endswith(".html"):
                errors.append(f"{rel}: canonical must be extensionless")
            if parser.canonical:
                expected_docs.add(docs_expected(rel[len("docs/"):]))
        elif rel == "index.html":
            if parser.canonical != ORIGIN + "/":
                errors.append(f"{rel}: canonical is {parser.canonical!r}, expected {ORIGIN + '/'!r}")
            if parser.og_url != ORIGIN + "/":
                errors.append(f"{rel}: og:url is {parser.og_url!r}, expected {ORIGIN + '/'!r}")

        for kind, value in parser.refs:
            candidates = [value] if kind != "srcset" else [part.strip().split()[0] for part in value.split(",") if part.strip()]
            for target in candidates:
                if target.startswith(("#", "mailto:", "tel:", "data:")):
                    continue
                parts = urlsplit(target)
                if parts.scheme in ("http", "https"):
                    if "localhost" in parts.netloc or parts.netloc.startswith("127.0.0.1"):
                        errors.append(f"{rel}: {kind} points at localhost: {target}")
                    continue
                if parts.scheme or parts.netloc:
                    errors.append(f"{rel}: unexpected {kind} scheme: {target}")
                    continue
                if target == "/docs":
                    errors.append(f"{rel}: /docs link must use the trailing slash")
                    continue
                resolved = resolve(page_dir_rel, target)
                if resolved is None:
                    continue
                if not resolved.is_relative_to(DIST.resolve()):
                    errors.append(f"{rel}: {kind} escapes dist/: {target}")
                    continue
                if not (resolved.is_file() or (resolved.is_dir() and (resolved / "index.html").is_file())):
                    errors.append(f"{rel}: broken {kind} {target}")

    if not (DIST / "docs" / "index.html").is_file():
        errors.append("docs/index.html missing")

    sitemap = ET.parse(DIST / "sitemap.xml").getroot()
    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    locs = {loc.text.strip() for loc in sitemap.iter(f"{ns}loc")}
    expected_locs = {ORIGIN + "/"} | {ORIGIN + path for path in expected_docs}
    if locs != expected_locs:
        errors.append(f"sitemap.xml mismatch: missing {sorted(expected_locs - locs)}, extra {sorted(locs - expected_locs)}")
    for loc in locs:
        if not loc.startswith(ORIGIN):
            errors.append(f"sitemap.xml entry outside {ORIGIN}: {loc}")

    manifest_path = DIST / "site.webmanifest"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            for icon in manifest.get("icons", []):
                if not (DIST / icon["src"].lstrip("/")).is_file():
                    errors.append(f"site.webmanifest icon missing: {icon['src']}")
        except (json.JSONDecodeError, KeyError) as exc:
            errors.append(f"site.webmanifest invalid: {exc}")

    homepage_bytes = (DIST / "index.html").stat().st_size + (DIST / "assets" / "site.css").stat().st_size
    if homepage_bytes > HOMEPAGE_BUDGET_BYTES:
        errors.append(f"homepage HTML plus site.css is {homepage_bytes} bytes; budget is {HOMEPAGE_BUDGET_BYTES}")

    for css in [DIST / "assets" / "site.css", *DOCS.glob("theme/custom-*.css")]:
        if css.is_file() and HEX_RE.search(css.read_text(encoding="utf-8")):
            errors.append(f"{css.relative_to(DIST)}: hex colour outside the tokens")

    robots = (DIST / "robots.txt").read_text(encoding="utf-8")
    if f"Sitemap: {ORIGIN}/sitemap.xml" not in robots:
        errors.append("robots.txt lacks the Sitemap line for SITE_ORIGIN")

    for path in DIST.rglob("*"):
        if path.is_file() and path.suffix in (".html", ".css", ".xml", ".txt", ".js", ".svg"):
            if "thadam-brand-kit" in path.read_text(encoding="utf-8", errors="ignore"):
                errors.append(f"{path.relative_to(DIST)}: references thadam-brand-kit")

    if errors:
        for error in errors:
            print(f"FAIL {error}", file=sys.stderr)
        print(f"{len(errors)} problem(s) in {DIST}", file=sys.stderr)
        return 1

    print(f"OK: {len(pages)} HTML pages, {len(locs)} sitemap URLs, canonical origin {ORIGIN}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
