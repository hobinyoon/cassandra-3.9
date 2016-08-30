#!/bin/bash

set -e
set -u

DN_THIS=`dirname $BASH_SOURCE`

pushd $DN_THIS/../bin >/dev/null

./cassandra -f | \grep --color=always -E '(^|^WARN)'

popd >/dev/null
