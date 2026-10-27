#!/usr/bin/env bash
# Builds build/scaleetl.jar with plain javac (JDK 21+, no dependencies).
set -euo pipefail
cd "$(dirname "$0")"
rm -rf build && mkdir -p build/classes
javac --release 21 -Xlint:all -d build/classes $(find src/main/java -name '*.java')
jar --create --file build/scaleetl.jar --main-class com.scaleetl.Main -C build/classes .
echo "built build/scaleetl.jar"
