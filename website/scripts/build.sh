#!/usr/bin/env bash
# Builds the whole site into website/dist: homepage, mdBook docs at /docs/, brand assets, sitemap.
# Usage: website/scripts/build.sh   (SITE_ORIGIN overrides the default origin)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
website="$(cd "$here/.." && pwd)"
repo="$(cd "$website/.." && pwd)"
brand="$repo/thadam-assets/brand"
dist="$website/dist"
origin="${SITE_ORIGIN:-https://thadam.dineshkarthik.me}"
origin="${origin%/}"

case "$origin" in
  https://*|http://*) ;;
  *) echo "SITE_ORIGIN must start with https:// or http://, got: $origin" >&2; exit 1 ;;
esac

command -v mdbook >/dev/null 2>&1 || { echo "mdbook not found. Install mdBook 0.5.x: cargo install mdbook --locked" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "python3 is required" >&2; exit 1; }

# The favicon copies are the only brand files inside website/; fail fast if they drift.
cmp -s "$website/docs/theme/favicon.svg" "$brand/svg/thadam-icon-light.svg" \
  || { echo "website/docs/theme/favicon.svg differs from thadam-assets/brand/svg/thadam-icon-light.svg; copy it again" >&2; exit 1; }
cmp -s "$website/docs/theme/favicon.png" "$brand/png/favicon-192.png" \
  || { echo "website/docs/theme/favicon.png differs from thadam-assets/brand/png/favicon-192.png; copy it again" >&2; exit 1; }

rm -rf "$dist"
mkdir -p "$dist/assets/brand"
cp -R "$website/public/." "$dist/"

mdbook build "$website/docs" --dest-dir "$dist/docs"

brand_files=(
  svg/thadam-logo-light.svg
  svg/thadam-logo-dark.svg
  svg/thadam-icon-light.svg
  svg/thadam-mark-light.svg
  svg/thadam-mark-dark.svg
  png/favicon-32.png
  png/favicon-180.png
  png/favicon-192.png
  png/favicon-512.png
  png/og-card.png
  png/thadam-illusion-light-hero.png
  png/thadam-illusion-dark-hero.png
  tokens/thadam.css
)
for f in "${brand_files[@]}"; do
  mkdir -p "$(dirname "$dist/assets/brand/$f")"
  cp "$brand/$f" "$dist/assets/brand/$f"
done

python3 - "$dist" "$origin" <<'PY'
import hashlib
import html
import pathlib
import re
import sys
from xml.sax.saxutils import escape

dist = pathlib.Path(sys.argv[1])
origin = sys.argv[2]
docs = dist / "docs"
placeholder = "<!--THADAM_CANONICAL-->"


def docs_path(rel):
    if rel == "index.html" or rel == "introduction.html":
        return "/docs/"
    if rel.endswith("/index.html"):
        return "/docs/" + rel[: -len("index.html")]
    return "/docs/" + rel[: -len(".html")]


sitemap = ["/"]
for page in sorted(docs.rglob("*.html")):
    rel = page.relative_to(docs).as_posix()
    text = page.read_text(encoding="utf-8")
    if placeholder not in text:
        raise SystemExit(f"{page}: canonical placeholder missing")
    if rel in ("print.html", "404.html", "toc.html"):
        text = text.replace(placeholder, "")
    else:
        url = origin + docs_path(rel)
        title_match = re.search(r"<title>(.*?)</title>", text, re.S)
        title = html.escape(title_match.group(1).strip() if title_match else "Thadam documentation", quote=True)
        image = origin + "/assets/brand/png/og-card.png"
        tags = (
            f'<link rel="canonical" href="{url}">\n'
            f'<meta property="og:url" content="{url}">\n'
            f'<meta property="og:site_name" content="Thadam">\n'
            f'<meta property="og:type" content="article">\n'
            f'<meta property="og:title" content="{title}">\n'
            f'<meta property="og:image" content="{image}">\n'
            f'<meta name="twitter:card" content="summary_large_image">\n'
            f'<meta name="twitter:title" content="{title}">\n'
            f'<meta name="twitter:image" content="{image}">'
        )
        text = text.replace(placeholder, tags)
        if docs_path(rel) not in sitemap and rel != "introduction.html":
            sitemap.append(docs_path(rel))
    page.write_text(text, encoding="utf-8")

for name in ("index.html", "404.html", "site.webmanifest"):
    page = dist / name
    if page.exists():
        page.write_text(page.read_text(encoding="utf-8").replace("__SITE_ORIGIN__", origin), encoding="utf-8")

entries = "\n".join(f"  <url><loc>{escape(origin + path)}</loc></url>" for path in sitemap)
(dist / "sitemap.xml").write_text(
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    f"{entries}\n"
    "</urlset>\n",
    encoding="utf-8",
)

digest = hashlib.sha256((dist / "assets" / "site.css").read_bytes() + (dist / "assets" / "site.js").read_bytes()).hexdigest()[:10]
for page in dist.rglob("*.html"):
    text = page.read_text(encoding="utf-8")
    if "__ASSET_VERSION__" in text:
        page.write_text(text.replace("__ASSET_VERSION__", digest), encoding="utf-8")

robots = dist / "robots.txt"
robots.write_text(robots.read_text(encoding="utf-8").rstrip("\n") + f"\n\nSitemap: {origin}/sitemap.xml\n", encoding="utf-8")
PY

python3 "$here/check.py"

echo "Built $dist"
(cd "$dist" && find . -type f | sort)
