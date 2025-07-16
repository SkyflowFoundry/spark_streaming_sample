#!/bin/sh
# =============================================================================
# Build All Script
# =============================================================================
# 
# PURPOSE:
#   This script builds all Maven modules in the project using Docker.
#   It compiles, tests, and packages all modules (utils, data-model, data-gen, emr-task)
#   without requiring Java or Maven to be installed locally.
#
# USAGE:
#   ./build-all.sh
#
# WHAT IT DOES:
#   - Uses Docker to run Maven build process
#   - Mounts current directory into Maven container
#   - Executes 'mvn clean install' to build all modules
#   - Creates JAR files needed by Docker containers
#
# OUTPUT:
#   - Compiled JAR files in each module's target/ directory
#   - Fat JARs with dependencies for data-gen and emr-task modules
#
# DEPENDENCIES:
#   - Docker must be installed and running
#   - Internet connection for downloading Maven dependencies
#
# FOR MORE INFORMATION:
#   See the main README.md file for complete setup and usage instructions.
# =============================================================================

set -e

echo "Building all Maven modules using Docker..."
docker run --rm -v "$PWD":/usr/src/mymaven -w /usr/src/mymaven maven:3.9.6-eclipse-temurin-17 mvn clean install 