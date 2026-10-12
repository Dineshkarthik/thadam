# Thadam brand guidelines

**Tagline:** The open data plane for robotics.
**Descriptor:** Open-source robotics data infrastructure.

This is the single merged guideline for Thadam. The files in this folder are the only canonical copy of the brand assets; the website and README consume them by build-time copy or by path.

## Brand direction
Developer-native, precise, understated, and infrastructure-oriented. The mark is a geometric `T` with a signal-orange crossbar detail. Use this system consistently; do not replace it with gears, robots, generic network nodes, diamonds, or literal fasteners.

## Palette
| Token | Hex | CSS variable | Use |
|---|---|---|---|
| Graphite (ink) | `#151719` | `--thadam-graphite` | Primary logo, text, dark surfaces |
| Warm white (paper) | `#F5F4F0` | `--thadam-warm-white` | Light surfaces and reverse logo |
| Signal orange | `#F26A3D` | `--thadam-signal-orange` | Small accents, actions, crossbar mark detail |
| Steel grey | `#8B9298` | `--thadam-steel-grey` | Secondary text, borders, muted UI |

Canonical values live in `tokens/thadam.css` and `tokens/colors.json`. Do not copy these hex values into other files; reference the tokens.

## Logo usage
- `svg/thadam-logo-light.svg` on warm-white and light backgrounds; `svg/thadam-logo-dark.svg` on graphite and dark backgrounds.
- `svg/thadam-mark-*.svg` is the standalone mark; `svg/thadam-icon-*.svg` is the app icon and favicon source.
- Use the SVGs unmodified: no redraw, recolour, stretch, rotation, shadow, or gradient.
- Preserve aspect ratio and clear space of at least the width of the mark's vertical stem.
- At tiny sizes, use the standalone icon and check legibility at 16, 24, and 32 px.
- Provide accessible text (`alt="Thadam"`) wherever the mark is used as an image.

## Typography
Use Inter where available; otherwise a modern system sans-serif stack. Wordmark SVGs use live text, not outlined lettering; convert the wordmark text to paths in a vector editor before final print use if pixel-identical rendering is required.

## Accessibility
Check contrast for UI text independently. Orange is an accent, not a colour for small text on warm white. Light and dark variants must both be readable.

## Files
- `svg/`: editable SVG sources (logos, wordmarks, marks, icons; light, dark, accent, and mono variants).
- `png/`: PNG exports and favicon sizes (`favicon-16/32/48/180/192/512.png`).
- `tokens/thadam.css`, `tokens/colors.json`: colour tokens.

## Naming and legal note
Thadam is the selected project identity, not a claim of trademark or domain availability. Name clearance (GitHub, crates.io, package registries, domains, trademark databases) is pending and must be done before public launch. The SVG geometry, font rendering, and small-size legibility should also be reviewed before launch.

## Status
Starter asset system for the Developer-Native visual direction. Review the geometry at small sizes before production use.
