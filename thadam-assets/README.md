# Thadam Brand Assets

**Name:** Thadam
**Tagline:** The open data plane for robotics.

`brand/` is the only canonical copy of the Thadam brand assets. Nothing else in the repository should hold a copy of them, except the two favicon files that the website theme needs (`website/docs/theme/`), which `website/scripts/build.sh` checks byte-for-byte.

## Files
- `brand/brand-guidelines.md`: merged usage rules, palette, and naming note.
- `brand/svg/`: editable SVG sources. `thadam-logo-light.svg` / `thadam-logo-dark.svg` are the horizontal logos; `thadam-mark-*.svg` the standalone geometric `T` mark; `thadam-icon-*.svg` the app icon.
- `brand/png/`: PNG exports and favicon sizes.
- `brand/tokens/thadam.css`, `brand/tokens/colors.json`: colour tokens.
- `brand/reference/`: design board and favicon preview. Maintainer-only, never published.
- `docs/project-identity.md`: project identity and README copy.

## Naming / legal note
Thadam is the selected project identity, not a claim of trademark or domain availability. Check GitHub, crates.io, package registries, domains, and relevant trademark databases before public launch.
