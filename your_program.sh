#!/bin/sh
#
# Use this script to run your program LOCALLY.
#

set -e # Exit early if any commands fail
(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  mvn -B package -Ddir=/tmp/redis_java
)

exec java -jar /tmp/redis_java/redis.jar "$@"
