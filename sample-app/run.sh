#!/bin/bash
set -o errexit
set -o pipefail

set -a
source .env

FILE=$1

python3 $FILE