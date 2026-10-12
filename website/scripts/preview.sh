#!/usr/bin/env bash
# Serves website/dist on 127.0.0.1 for local review. Builds first if dist is missing.
# Usage: website/scripts/preview.sh [port]   (default port 8000)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
website="$(cd "$here/.." && pwd)"
port="${1:-8000}"

if [ ! -f "$website/dist/index.html" ]; then
  "$here/build.sh"
fi

echo "Serving $website/dist at http://127.0.0.1:$port/ (Ctrl-C to stop)"
cd "$website/dist"
exec python3 -m http.server "$port" --bind 127.0.0.1
