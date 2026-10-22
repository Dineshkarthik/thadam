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

versions_manifest="$website/docs/versions.json"
[ -f "$versions_manifest" ] || { echo "versions manifest missing: $versions_manifest" >&2; exit 1; }

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

versions_tsv="$tmpdir/versions.tsv"
python3 - "$versions_manifest" > "$versions_tsv" <<'PY'
import json
import pathlib
import re
import sys

manifest = pathlib.Path(sys.argv[1])
data = json.loads(manifest.read_text(encoding="utf-8"))
versions = data.get("versions")
if not isinstance(versions, list) or not versions:
    raise SystemExit("versions.json must contain a non-empty 'versions' list")
reserved = {
    "cli", "concepts", "configuration", "design", "getting-started",
    "operations", "sdk", "sinks", "sources", "transforms",
    "theme", "fonts", "css", "FontAwesome", "book",
}
seen = set()
latest_count = 0
for entry in versions:
    ver_id = entry.get("id", "")
    ver_ref = entry.get("ref", "")
    ver_label = entry.get("label", "")
    if not isinstance(ver_id, str) or not re.fullmatch(r"[0-9A-Za-z._-]+", ver_id) or ver_id in (".", ".."):
        raise SystemExit(f"versions.json: invalid version id {ver_id!r}")
    if ver_id in reserved:
        raise SystemExit(f"versions.json: version id {ver_id!r} collides with reserved docs path")
    if ver_id in seen:
        raise SystemExit(f"versions.json: duplicate version id {ver_id!r}")
    seen.add(ver_id)
    if not isinstance(ver_label, str) or not ver_label.strip():
        raise SystemExit(f"versions.json: invalid label for {ver_id!r}")
    if not isinstance(ver_ref, str) or not ver_ref.strip():
        raise SystemExit(f"versions.json: invalid ref for {ver_id!r}")
    if entry.get("latest") is True:
        latest_count += 1
    print(f"{ver_id}\t{ver_ref}")
if latest_count != 1:
    raise SystemExit(f"versions.json: expected exactly 1 entry with latest=true, got {latest_count}")
PY

while IFS=$'\t' read -r ver_id ver_ref; do
  [ -n "$ver_id" ] || continue
  ver_dir="$tmpdir/$ver_id"
  mkdir -p "$ver_dir"
  if [ "$ver_ref" = "WORKTREE" ]; then
    mkdir -p "$ver_dir/src"
    cp -R "$website/docs/src/." "$ver_dir/src/"
  else
    git -C "$repo" archive "$ver_ref" website/docs/src | tar -x -C "$ver_dir" --strip-components=2
  fi
  cp -R "$website/docs/theme" "$ver_dir/theme"
  python3 - "$website/docs/book.toml" "$ver_dir/book.toml" "$ver_id" "$ver_ref" <<'PY'
import pathlib
import re
import sys

src_toml = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
dst_toml = pathlib.Path(sys.argv[2])
ver_id = sys.argv[3]
ver_ref = sys.argv[4]

text = re.sub(r'(?m)^site-url\s*=\s*".*"$', f'site-url = "/docs/{ver_id}/"', src_toml)
if ver_ref != "WORKTREE":
    text = re.sub(r'(?m)^edit-url-template\s*=.*\n?', "", text)
dst_toml.write_text(text, encoding="utf-8")
PY
  mdbook build "$ver_dir" --dest-dir "$dist/docs/$ver_id"
done < "$versions_tsv"

brand_files=(
  svg/thadam-logo-light.svg
  svg/thadam-logo-dark.svg
  svg/thadam-wordmark-light.svg
  svg/thadam-wordmark-dark.svg
  svg/thadam-icon-light.svg
  svg/thadam-mark-light.svg
  svg/thadam-mark-dark.svg
  png/favicon-32.png
  png/favicon-180.png
  png/favicon-192.png
  png/favicon-512.png
  png/og-card.png
  illustrations/thadam-data-plane-light.webp
  illustrations/thadam-platform-light.webp
  tokens/thadam.css
)
for f in "${brand_files[@]}"; do
  mkdir -p "$(dirname "$dist/assets/brand/$f")"
  cp "$brand/$f" "$dist/assets/brand/$f"
done
cp "$brand/png/favicon.ico" "$dist/favicon.ico"

python3 - "$dist" "$origin" "$versions_manifest" <<'PY'
import hashlib
import html
import json
import pathlib
import re
import sys
from xml.sax.saxutils import escape

dist = pathlib.Path(sys.argv[1])
origin = sys.argv[2]
versions_manifest = pathlib.Path(sys.argv[3])
versions = json.loads(versions_manifest.read_text(encoding="utf-8"))["versions"]
version_ids = {entry["id"] for entry in versions}
docs = dist / "docs"
canonical_placeholder = "<!--THADAM_CANONICAL-->"
select_placeholder = "<!--THADAM_VERSION_SELECT-->"
banner_placeholder = "<!--THADAM_VERSION_BANNER-->"


def docs_path(rel):
    if rel == "index.html" or rel == "introduction.html":
        return "/docs/"
    if rel.endswith("/index.html"):
        return "/docs/" + rel[: -len("index.html")]
    return "/docs/" + rel[: -len(".html")]


def docs_version_path(version_id, rel):
    if rel == "index.html" or rel == "introduction.html":
        return f"/docs/{version_id}/"
    if rel.endswith("/index.html"):
        return f"/docs/{version_id}/" + rel[: -len("index.html")]
    return f"/docs/{version_id}/" + rel[: -len(".html")]


def build_version_select(rel, version_id):
    options = []
    for entry in versions:
        v_id = entry["id"]
        selected = " selected" if ((version_id is None and entry.get("latest")) or (version_id == v_id)) else ""
        if version_id is not None and entry.get("latest"):
            target_url = "/docs/" if (rel in ("index.html", "introduction.html") or not (docs / rel).is_file()) else f"/docs/{rel}"
        elif version_id is None and entry.get("latest"):
            target_url = "/docs/" if rel in ("index.html", "introduction.html") else f"/docs/{rel}"
        else:
            target_url = f"/docs/{v_id}/{rel}" if ((docs / v_id / rel).is_file() and rel not in ("index.html", "introduction.html")) else f"/docs/{v_id}/"
        options.append(f'<option value="{html.escape(target_url, quote=True)}"{selected}>{html.escape(entry["label"])}</option>')
    return '<select class="thadam-version-select" data-version-select aria-label="Documentation version">' + "".join(options) + "</select>"


def build_version_banner(rel, version_id, is_latest):
    if version_id is not None and not is_latest:
        latest_href = f"/docs/{rel}" if ((docs / rel).is_file() and rel not in ("index.html", "introduction.html")) else "/docs/"
        return (
            '<div class="thadam-version-banner" role="region" aria-label="Version notice">'
            f"You are viewing documentation for Thadam <strong>{html.escape(version_id)}</strong>. "
            f'<a href="{html.escape(latest_href, quote=True)}">View the latest version</a>.'
            "</div>"
        )
    return ""


sitemap = ["/"]


def process_book(book_root, version_id, is_latest):
    for page in sorted(book_root.rglob("*.html")):
        rel_path = page.relative_to(book_root)
        if version_id is None and rel_path.parts[0] in version_ids:
            continue
        rel = rel_path.as_posix()
        text = page.read_text(encoding="utf-8")
        if canonical_placeholder not in text:
            raise SystemExit(f"{page}: canonical placeholder missing")
        if rel in ("print.html", "404.html", "toc.html"):
            text = text.replace(canonical_placeholder, "")
        else:
            self_path = docs_path(rel) if version_id is None else docs_version_path(version_id, rel)
            canonical_path = docs_path(rel) if (version_id is None or (docs / rel).is_file()) else self_path
            canonical_url = origin + canonical_path
            self_url = origin + self_path
            title_match = re.search(r"<title>(.*?)</title>", text, re.S)
            title = html.escape(title_match.group(1).strip() if title_match else "Thadam documentation", quote=True)
            image = origin + "/assets/brand/png/og-card.png"
            tags = (
                f'<link rel="canonical" href="{canonical_url}">\n'
                f'<meta property="og:url" content="{self_url}">\n'
                f'<meta property="og:site_name" content="Thadam">\n'
                f'<meta property="og:type" content="article">\n'
                f'<meta property="og:title" content="{title}">\n'
                f'<meta property="og:image" content="{image}">\n'
                f'<meta name="twitter:card" content="summary_large_image">\n'
                f'<meta name="twitter:title" content="{title}">\n'
                f'<meta name="twitter:image" content="{image}">'
            )
            text = text.replace(canonical_placeholder, tags)
            if version_id is None and self_path not in sitemap and rel != "introduction.html":
                sitemap.append(self_path)
        if rel != "toc.html":
            if select_placeholder not in text:
                raise SystemExit(f"{page}: version select placeholder missing")
            if banner_placeholder not in text:
                raise SystemExit(f"{page}: version banner placeholder missing")
        text = text.replace(select_placeholder, build_version_select(rel, version_id))
        text = text.replace(banner_placeholder, build_version_banner(rel, version_id, is_latest))
        page.write_text(text, encoding="utf-8")


process_book(docs, None, True)
for entry in versions:
    process_book(docs / entry["id"], entry["id"], bool(entry.get("latest")))

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
