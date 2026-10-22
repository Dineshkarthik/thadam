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
VERSIONS_JSON = WEBSITE / "docs" / "versions.json"
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
    "assets/brand/illustrations/thadam-data-plane-light.webp",
    "assets/brand/illustrations/thadam-platform-light.webp",
    "favicon.ico",
    "site.webmanifest",
]
NOINDEX_INNER_PAGES = {"print.html", "404.html", "toc.html"}
FRAGMENT_INNER_PAGES = {"toc.html"}
VERSION_ID_RE = re.compile(r"^[0-9A-Za-z._-]+$")
RESERVED_DOCS_NAMES = {
    "cli",
    "concepts",
    "configuration",
    "design",
    "getting-started",
    "operations",
    "sdk",
    "sinks",
    "sources",
    "transforms",
    "theme",
    "fonts",
    "css",
    "index.html",
    "introduction.html",
    "toc.html",
    "404.html",
    "print.html",
}
HOMEPAGE_BUDGET_BYTES = 30 * 1024
REQUIRED_CSS_SELECTORS = [".flow-grid", ".flow-node", ".flow-link", ".features", ".hero-art img", ".theme-toggle", "img.on-dark", ".pill-progress"]
FORBIDDEN_CLAIMS = [
    "zero data loss",
    "zero-loss",
    "zero loss",
    "no data loss",
    "exactly-once",
    "exactly once",
    "lossless",
    "production-ready",
    "production ready",
    "never lost",
    "never lose",
    "loses nothing",
    "is lost",
    "guaranteed",
]
NEGATION_CUES = ("not ", "cannot", "can't", "refuse", "rather than", "without ", "no claim")
STATUS_WORDS = ("Available", "In progress", "Planned")
STATUS_LINE_RE = re.compile(r"\bStatus:\s*([^.\n]+)")
PR_TOKEN_RE = re.compile(r"\bPR[0-9]+\b")
HEX_RE = re.compile(r"#(?:[0-9A-Fa-f]{3}){1,2}\b")
FAVICON_GUARDS = {
    "favicon-*.svg": "svg/thadam-icon-light.svg",
    "favicon-*.png": "png/favicon-192.png",
}


class VisibleText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self.skip:
            self.skip -= 1

    def handle_data(self, data):
        if not self.skip:
            self.parts.append(data)

    def text(self):
        return re.sub(r"\s+", " ", " ".join(self.parts))


def lint_claims(label, text, errors):
    lowered = text.lower()
    for phrase in FORBIDDEN_CLAIMS:
        for match in re.finditer(re.escape(phrase), lowered):
            end = re.search(r"[.?!]", lowered[match.end():])
            if end and end.group(0) == "?":
                continue
            before = lowered[max(0, match.start() - 80):match.start()]
            if not any(cue in before for cue in NEGATION_CUES):
                snippet = text[max(0, match.start() - 40):match.end() + 20]
                errors.append(f"{label}: forbidden claim wording '{phrase}' in: ...{snippet}...")


def lint_status(html_text, errors):
    section = re.search(r'<section class="section" id="status".*?</section>', html_text, re.S)
    if not section:
        errors.append("index.html: status section not found")
        return
    block = section.group(0)
    for row in re.findall(r"<tr>(.*?)</tr>", block, re.S):
        cells = re.findall(r"<td>(.*?)</td>", row, re.S)
        if not cells:
            continue
        status = re.sub(r"<[^>]+>", "", cells[-1]).strip()
        if not status.startswith(STATUS_WORDS):
            errors.append(f"index.html: status cell must start with Available, In progress, or Planned: '{status}'")
    for label in re.findall(r'class="pill[^"]*">([^<]+)</span>', html_text):
        if label not in STATUS_WORDS:
            errors.append(f"index.html: pill label must be Available, In progress, or Planned: '{label}'")


def lint_docs_status(label, text, errors):
    for match in STATUS_LINE_RE.finditer(text):
        value = match.group(1).strip()
        if not any(
            value == word or value.startswith((word + " ", word + "(", word + ",", word + ";"))
            for word in STATUS_WORDS
        ):
            errors.append(
                f"{label}: Status: line must start with Available, In progress, or Planned: 'Status: {value}'"
            )


def lint_pr_tokens(label, text, errors):
    for match in PR_TOKEN_RE.finditer(text):
        snippet = text[max(0, match.start() - 30):match.end() + 30]
        errors.append(f"{label}: internal PR token '{match.group(0)}' in visible text: ...{snippet}...")


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.in_title = False
        self.viewport = False
        self.canonical = None
        self.og_url = None
        self.refs = []
        self.ids = set()
        self.in_pre = False
        self.missing_alt = 0
        self.og_image = None
        self.json_ld = []
        self.in_json_ld = False
        self._ld_buffer = ""
        self.has_version_select = False
        self.in_version_select = False
        self.version_options = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if a.get("id"):
            self.ids.add(a["id"])
        if tag == "a" and a.get("name"):
            self.ids.add(a["name"])
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
        elif tag == "select":
            if "data-version-select" in a:
                self.has_version_select = True
                self.in_version_select = True
        elif tag == "option" and self.in_version_select:
            val = a.get("value")
            if val:
                self.version_options.append(val)
                self.refs.append(("option", val))
            else:
                self.version_options.append("")

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False
        if tag == "script" and self.in_json_ld:
            self.json_ld.append(self._ld_buffer)
            self.in_json_ld = False
        if tag == "select" and self.in_version_select:
            self.in_version_select = False

    def handle_data(self, data):
        if self.in_title:
            self.title += data
        if self.in_json_ld:
            self._ld_buffer += data


def load_versions(errors):
    if not VERSIONS_JSON.is_file():
        errors.append("missing website/docs/versions.json")
        return []
    try:
        data = json.loads(VERSIONS_JSON.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        errors.append(f"versions.json invalid JSON: {exc}")
        return []
    versions = data.get("versions") if isinstance(data, dict) else None
    if not isinstance(versions, list) or not versions:
        errors.append("versions.json: 'versions' must be a non-empty list")
        return []
    seen_ids = set()
    latest_count = 0
    valid_ids = []
    for idx, item in enumerate(versions):
        if not isinstance(item, dict):
            errors.append(f"versions.json: entry {idx} is not an object")
            continue
        v_id = item.get("id")
        label = item.get("label")
        ref = item.get("ref")
        if not isinstance(v_id, str) or not v_id.strip():
            errors.append(f"versions.json: entry {idx} missing non-empty string 'id'")
            continue
        if not isinstance(label, str) or not label.strip():
            errors.append(f"versions.json: entry '{v_id}' missing non-empty string 'label'")
        if not isinstance(ref, str) or not ref.strip():
            errors.append(f"versions.json: entry '{v_id}' missing non-empty string 'ref'")
        if not VERSION_ID_RE.match(v_id):
            errors.append(f"versions.json: id '{v_id}' does not match {VERSION_ID_RE.pattern}")
        if v_id in RESERVED_DOCS_NAMES:
            errors.append(f"versions.json: id '{v_id}' collides with reserved docs name")
        if v_id in seen_ids:
            errors.append(f"versions.json: duplicate id '{v_id}'")
        else:
            seen_ids.add(v_id)
            valid_ids.append(v_id)
        if item.get("latest") is True:
            latest_count += 1
    if latest_count != 1:
        errors.append(f"versions.json: expected exactly one 'latest: true' entry, found {latest_count}")
    return valid_ids


def docs_book_rel(rel, version_ids):
    if not rel.startswith("docs/"):
        return None, None
    after_docs = rel[len("docs/"):]
    first, sep, rest = after_docs.partition("/")
    if sep and first in version_ids:
        return first, rest
    return None, after_docs


def docs_expected(rel):
    if rel in ("index.html", "introduction.html"):
        return "/docs/"
    if rel.endswith("/index.html"):
        return "/docs/" + rel[: -len("index.html")]
    return "/docs/" + rel[: -len(".html")]


def docs_version_expected(version_id, rel):
    if rel in ("index.html", "introduction.html"):
        return f"/docs/{version_id}/"
    if rel.endswith("/index.html"):
        return f"/docs/{version_id}/" + rel[: -len("index.html")]
    return f"/docs/{version_id}/" + rel[: -len(".html")]


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

    version_ids = load_versions(errors)
    version_id_set = set(version_ids)

    for rel in REQUIRED:
        if not (DIST / rel).is_file():
            errors.append(f"missing required file: {rel}")

    for v_id in version_ids:
        if not (DOCS / v_id / "index.html").is_file():
            errors.append(f"missing required versioned docs file: docs/{v_id}/index.html")

    for docs_root in [DOCS, *(DOCS / v_id for v_id in version_ids)]:
        rel_root = docs_root.relative_to(DIST).as_posix()
        for pattern, brand_rel in FAVICON_GUARDS.items():
            copies = sorted(docs_root.glob(pattern))
            if not copies:
                errors.append(f"{rel_root} has no hashed {pattern} favicon")
            for copy in copies:
                if copy.read_bytes() != (BRAND / brand_rel).read_bytes():
                    errors.append(f"{copy.relative_to(DIST)} differs from thadam-assets/brand/{brand_rel}")

    pages = sorted(DIST.rglob("*.html"))
    parsed_pages = []
    page_ids = {}
    for page in pages:
        text = page.read_text(encoding="utf-8")
        parser = PageParser()
        parser.feed(text)
        parsed_pages.append((page, text, parser))
        page_ids[page.resolve()] = parser.ids

    expected_docs = set()
    for page, text, parser in parsed_pages:
        rel = page.relative_to(DIST).as_posix()
        page_dir_rel = page.parent.relative_to(DIST)
        version_id, inner_rel = docs_book_rel(rel, version_id_set)
        is_fragment_page = inner_rel in FRAGMENT_INNER_PAGES
        is_docs_page = inner_rel is not None and inner_rel not in NOINDEX_INNER_PAGES

        if not is_fragment_page:
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
        if rel in ("index.html", "404.html") or is_docs_page:
            visible = VisibleText()
            visible.feed(text)
            visible_text = visible.text()
            label = "homepage" if rel == "index.html" else rel
            if rel == "index.html" or is_docs_page:
                lint_claims(label, visible_text, errors)
            lint_pr_tokens(label, visible_text, errors)
            if is_docs_page:
                lint_docs_status(rel, visible_text, errors)
        if rel == "index.html":
            if text.count("<h1") != 1:
                errors.append(f"index.html: expected exactly one <h1>, found {text.count('<h1')}")
            lint_status(text, errors)
            if not parser.json_ld:
                errors.append("index.html: missing JSON-LD")
            if parser.og_image is None:
                errors.append("index.html: og:image missing")
        if is_docs_page:
            if "og:title" not in text:
                errors.append(f"{rel}: missing og:title")
            if not parser.has_version_select or "data-version-select" not in text:
                errors.append(f"{rel}: missing data-version-select")
            elif not parser.version_options or any(not opt for opt in parser.version_options):
                errors.append(f"{rel}: data-version-select missing valid <option value=...> entries")
        if HEX_RE.search(text) and rel == "index.html":
            errors.append(f"{rel}: hex colour outside the tokens")
        if any(
            token in text
            for token in (
                "THADAM_CANONICAL",
                "THADAM_VERSION_SELECT",
                "THADAM_VERSION_BANNER",
                "__SITE_ORIGIN__",
                "__ASSET_VERSION__",
            )
        ):
            errors.append(f"{rel}: unreplaced build placeholder")
        if is_docs_page:
            if version_id is None:
                expected_url = ORIGIN + docs_expected(inner_rel)
                if parser.canonical != expected_url:
                    errors.append(f"{rel}: canonical is {parser.canonical!r}, expected {expected_url!r}")
                if parser.og_url != expected_url:
                    errors.append(f"{rel}: og:url is {parser.og_url!r}, expected {expected_url!r}")
                if parser.canonical and parser.canonical.endswith(".html"):
                    errors.append(f"{rel}: canonical must be extensionless")
                if parser.og_url and parser.og_url.endswith(".html"):
                    errors.append(f"{rel}: og:url must be extensionless")
                if parser.canonical:
                    expected_docs.add(docs_expected(inner_rel))
            else:
                expected_og = ORIGIN + docs_version_expected(version_id, inner_rel)
                expected_canon = (ORIGIN + docs_expected(inner_rel)) if (DOCS / inner_rel).is_file() else expected_og
                if parser.canonical != expected_canon:
                    errors.append(f"{rel}: canonical is {parser.canonical!r}, expected {expected_canon!r}")
                if parser.og_url != expected_og:
                    errors.append(f"{rel}: og:url is {parser.og_url!r}, expected {expected_og!r}")
                if parser.canonical and parser.canonical.endswith(".html"):
                    errors.append(f"{rel}: canonical must be extensionless")
                if parser.og_url and parser.og_url.endswith(".html"):
                    errors.append(f"{rel}: og:url must be extensionless")
        elif rel == "index.html":
            if parser.canonical != ORIGIN + "/":
                errors.append(f"{rel}: canonical is {parser.canonical!r}, expected {ORIGIN + '/'!r}")
            if parser.og_url != ORIGIN + "/":
                errors.append(f"{rel}: og:url is {parser.og_url!r}, expected {ORIGIN + '/'!r}")

        for kind, value in parser.refs:
            candidates = [value] if kind != "srcset" else [part.strip().split()[0] for part in value.split(",") if part.strip()]
            for target in candidates:
                if target.startswith(("mailto:", "tel:", "data:")):
                    continue
                if target.startswith("#"):
                    frag = unquote(target[1:])
                    if frag and frag not in parser.ids:
                        errors.append(f"{rel}: broken fragment {target}")
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
                target_file = resolved if resolved.is_file() else ((resolved / "index.html") if (resolved.is_dir() and (resolved / "index.html").is_file()) else None)
                if target_file is None:
                    errors.append(f"{rel}: broken {kind} {target}")
                    continue
                if parts.fragment and target_file in page_ids:
                    frag = unquote(parts.fragment)
                    if frag and frag not in page_ids[target_file]:
                        errors.append(f"{rel}: broken fragment #{frag} in {target}")

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

    site_css = (DIST / "assets" / "site.css").read_text(encoding="utf-8") if (DIST / "assets" / "site.css").is_file() else ""
    for selector in REQUIRED_CSS_SELECTORS:
        if selector not in site_css:
            errors.append(f"assets/site.css lacks required rule {selector}")

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

    css_files = [DIST / "assets" / "site.css", *DOCS.glob("theme/custom-*.css")]
    for v_id in version_ids:
        css_files.extend((DOCS / v_id).glob("theme/custom-*.css"))
    for css in css_files:
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
