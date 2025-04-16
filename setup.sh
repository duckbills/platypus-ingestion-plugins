#!/bin/bash
set -e

# Build and install nrtsearch to local maven repo
echo "Building nrtsearch..."
(cd ../nrtsearch && ./gradlew clean publishToMavenLocal)

# Build the ingestion plugins
echo "Building ingestion plugins..."
./gradlew clean build --stacktrace