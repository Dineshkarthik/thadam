# Thadam Brand Assets

**Name:** Thadam
**Tagline:** The open data plane for robotics.

`brand/` is the only canonical copy of the Thadam brand assets. Nothing else in the repository should hold a copy of them, except the two favicon files that the website theme needs (`website/docs/theme/`), which `website/scripts/build.sh` checks byte-for-byte.

## Files
- `brand/brand-guidelines.md`: merged usage rules, palette, and naming note.
- `brand/svg/`: editable SVG sources. `thadam-logo-light.svg` / `thadam-logo-dark.svg` are the horizontal logos (`500×82`); `thadam-wordmark-light.svg` / `thadam-wordmark-dark.svg` are the wordmarks without the tagline (`500×58`); `thadam-mark-*.svg` the standalone geometric `T` mark (`64×64`); `thadam-icon-*.svg` the app icon (`64×64`).
- `brand/png/`: PNG exports and favicon sizes.
- `brand/tokens/thadam.css`, `brand/tokens/colors.json`: colour tokens.
- `docs/project-identity.md`: project identity and README copy.

## Naming / legal note
Thadam is the project name, not a claim of trademark or domain availability. [ADR-0016](../docs/design/adr/0016-naming-and-publishing.md) (Accepted) records the GitHub, crates.io, and PyPI checks; no trademark search has been done.

## License and trademark use
The files in `thadam-assets/` are covered by the Apache-2.0 [`LICENSE`](../LICENSE) at the root of this repository.

Apache-2.0 does not grant trademark rights. The name "Thadam" and the Thadam logos are governed by this trademark-use policy:
- Use the name and logos only to refer to Thadam.
- Do not use them in a way that implies endorsement by, or affiliation with, the Thadam project or its maintainer.
