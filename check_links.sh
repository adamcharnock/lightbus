#!/usr/bin/env bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
LINKCHECK="$DIR/../bin/linkcheck"

if [ ! -e "$LINKCHECK" ]; then
    echo "Downloading linkcheck"
    curl -L -o "$LINKCHECK" https://github.com/filiph/linkcheck/releases/download/v2.0.11/linkcheck-mac-x64
    chmod +x "$LINKCHECK"
fi

echo "Starting mkdocs server"
pipenv run mkdocs serve &


until nc -z 127.0.0.1 8000; do
    echo "Waiting for server to start"
    sleep 1
done

echo "Checking links"

set +e
$LINKCHECK http://127.0.0.1:8000/
EXIT_CODE=$?
set -e


echo "Stopping server"
kill %%

exit $EXIT_CODE
