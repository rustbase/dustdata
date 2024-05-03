#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

rm -rf test_data
cargo test $@
