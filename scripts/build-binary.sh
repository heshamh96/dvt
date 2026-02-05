#!/bin/bash
# Build DVT as a standalone macOS binary using Nuitka

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

VERSION=$(python3 -c "exec(open('core/dvt/__version__.py').read()); print(version)")

echo "Building DVT v${VERSION} for macOS..."
echo ""

# Check for Nuitka
if ! python3 -c "import nuitka" 2>/dev/null; then
    echo "Installing Nuitka and dependencies..."
    uv pip install nuitka ordered-set zstandard
fi

# Create dist directory
mkdir -p dist

# Build single binary
echo "Compiling with Nuitka (this may take a while on first run)..."
echo ""

python3 -m nuitka \
    --onefile \
    --standalone \
    --output-filename=dvt \
    --output-dir=dist \
    --include-package=dvt \
    --include-package-data=dvt \
    --follow-imports \
    --assume-yes-for-downloads \
    core/dvt/cli/main.py

echo ""
echo "============================================"
echo "Build complete!"
echo "Binary: dist/dvt"
echo "Version: ${VERSION}"
echo ""
echo "Test with:"
echo "  ./dist/dvt --version"
echo "  ./dist/dvt debug"
echo "============================================"
