#!/bin/bash
set -e

# Create build directory
mkdir -p build
cd build

# Configure and build
cmake ..
make -j$(nproc)

echo "Build complete! Libraries are in the build/ directory:"
echo "  - libotel_process_ctx.so (shared library)"
echo "  - libotel_process_ctx.a (static library)"
echo "  - (Check folder for more variants)"
echo "And the example program is in the build/ directory:"
echo "  - example_ctx"
