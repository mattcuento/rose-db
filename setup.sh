#!/bin/bash
# Setup script for rose-db

echo "🌹 Rose-DB Setup Script"
echo ""

# Fetch latest
echo "📥 Fetching latest from remote..."
git fetch origin

# Pull main (or your branch)
echo "🔄 Pulling latest changes..."
git pull origin main

# CRITICAL: Update submodules (contains B+ tree index)
echo "📦 Updating submodules (this pulls the B+ tree index)..."
git submodule update --init --recursive

# Verify index files exist
echo ""
echo "✅ Verifying B+ tree index files..."
if [ -d "storage-engine/src/index" ]; then
    echo "   ✓ storage-engine/src/index/ exists"
    ls -la storage-engine/src/index/ | grep -E "\.rs$" | awk '{print "   ✓", $9}'
else
    echo "   ✗ storage-engine/src/index/ NOT FOUND"
    echo "   Run: git submodule update --init --recursive"
fi

# Verify query-engine
echo ""
echo "✅ Verifying query engine..."
if [ -d "query-engine/src" ]; then
    echo "   ✓ query-engine/src/ exists"
    ls -la query-engine/src/*.rs 2>/dev/null | awk '{print "   ✓", $9}'
else
    echo "   ✗ query-engine/ NOT FOUND"
fi

echo ""
echo "🔨 Building project..."
cargo build

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "  - Run tests: cargo test"
echo "  - Run demo: cargo run --package query_engine --example dataframe_demo"
echo "  - Check B+ tree: ls storage-engine/src/index/"
