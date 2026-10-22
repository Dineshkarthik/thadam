# Thadam website

This folder builds the whole public site into one artifact, `website/dist/`.

## Folder map

| Path | Served at | What it is |
|---|---|---|
| `public/` | `/` | Homepage (`index.html`), `404.html`, `robots.txt`, `assets/site.css` |
| `docs/` | `/docs/` | mdBook user documentation: `book.toml`, `src/` (pages and `SUMMARY.md`), `theme/` (overrides and the two favicon copies) |
| `scripts/` | n/a | `build.sh`, `preview.sh`, `check.py` |
| `dist/` | n/a | Generated output, gitignored. This is the deploy artifact. |

The engineering records in `docs/design/` (ADRs, readiness, spikes) are not site content and are not published.

## Commands

Run these from the repository root (they work from any directory):

```bash
website/scripts/build.sh          # builds website/dist and runs check.py
website/scripts/preview.sh        # serves website/dist at http://127.0.0.1:8000/
python3 website/scripts/check.py  # checks an existing website/dist
mdbook build website/docs         # user docs only, into website/docs/book/
```

`build.sh` needs `mdbook` 0.5.x (`cargo install mdbook --locked`) and `python3`. Set `SITE_ORIGIN` to override the default origin, `https://thadam.dineshkarthik.me`. The origin feeds canonical and `og:url` tags, the sitemap, and the `Sitemap:` line in `robots.txt`.

## How `dist/` is composed

1. `dist/` is wiped and `public/` is copied to its root.
2. `mdbook build` writes the docs to `dist/docs/`.
3. The brand files the site uses (logos, wordmarks, marks, icon, favicon PNGs, `og-card.png`, the two `illustrations/*.webp` homepage images, `tokens/thadam.css`) are copied from `thadam-assets/brand/` to `dist/assets/brand/`, and `png/favicon.ico` is copied to `dist/favicon.ico`. `thadam-assets/reference/` holds concept mockups that are never shipped.
4. The two favicon copies in `docs/theme/` are compared byte-for-byte with `thadam-assets/brand/`, and the build fails on drift.
5. A small Python step adds canonical and `og:url` tags to docs pages, writes `sitemap.xml`, and appends the sitemap line to `robots.txt`.
6. `check.py` verifies the result: required files, titles and viewport meta, every local link, `#fragment` anchor, and image resolving, canonicals, no `localhost` links, no internal `PR[0-9]+` tokens in visible text, and the sitemap matching the pages.

## Adding a docs page

1. Create the Markdown file under `docs/src/`, for example `docs/src/operations/new-page.md`.
2. Add it to `docs/src/SUMMARY.md` in the right section. Pages not listed there are not built.
3. Link to other docs pages with relative `.md` paths. Link to repository documents (ARCHITECTURE, ADRs, readiness) with absolute GitHub URLs, because they are not part of the site.
4. Run `website/scripts/build.sh`.

## Updating brand assets

Edit files in `thadam-assets/brand/` (SVG sources in `svg/`, tokens in `tokens/thadam.css`), not in the website. Then run `website/scripts/build.sh`. If you change the favicon, copy the new `svg/thadam-icon-light.svg` to `docs/theme/favicon.svg` and the new `png/favicon-192.png` to `docs/theme/favicon.png`; the build fails until they match.

## Metadata, performance, and checks

- Social card: `thadam-assets/brand/png/og-card.png` (1200x630) is used by `og:image` and `twitter:card` on the homepage and every docs page. The maintainer regenerates it from the brand source files, which are not in the repository, and commits the result.
- Web app manifest: `public/site.webmanifest` lists the 192 and 512 px PNGs from `thadam-assets/brand/png/`.
- The mdBook print page is disabled (`[output.html.print] enable = false` in `docs/book.toml`) to keep the output small.
- Budget: the homepage HTML plus `site.css` must stay under 30 KB. `check.py` fails above that. `site.css` defines its light and dark colours once with `light-dark()`; `data-theme` (or the system setting when JavaScript is off) picks the `color-scheme`.
- `check.py` also verifies JSON-LD parses, OG images and manifest icons exist, every `<img>` has `alt`, and the homepage and theme CSS use no hex colours outside the brand tokens.
- Claim wording: `check.py` scans the built homepage and every docs page for forbidden claim phrases ("zero data loss", "loses nothing", "exactly-once", "is lost", "guaranteed", and similar). A phrase passes only when it is negated in the sentence before it ("does not claim exactly-once") or appears in a question. Allowed wording comes from the claims register in `docs/design/adr/0011-delivery-guarantees-and-claims-register.md`.
- Status labels and internal tokens: every homepage status cell, homepage pill, and docs `Status:` line must read Available, In progress, or Planned, and built public pages may not contain `PR[0-9]+` tokens in visible text. The homepage must have exactly one `h1`.
- Contrast was measured for the text pairs: muted text is about 8:1 on warm white and about 10:1 on graphite. Orange on warm white is about 2.8:1, so it is used only for decorative icons and rules, never for informational graphics or text. The primary button is the one orange fill: graphite text on Signal orange, about 5.8:1.

## Deployment

Deployment is configured outside this repository section: the maintainer deploys `website/dist` to Cloudflare Pages through GitHub Actions. This folder contains no deploy step, Pages configuration, or secrets. CI builds, checks, and uploads `website/dist` as a workflow artifact only.
