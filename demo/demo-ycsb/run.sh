#/bin/bash

# Print commands while executing
set -x

CLIENT_THREADS=1
KEYS=1000
PARTITIONS=4
ZIPF_CONST=0.0
INPUT_MSG_PER_SEC=1000
DURATION_SEC=30
SAVE_DIR=results
WARMUP_SEC=5
RUN_WITH_VALIDATION=True

python client.py \
	$CLIENT_THREADS \
	$KEYS \
	$PARTITIONS \
	$ZIPF_CONST \
	$INPUT_MSG_PER_SEC \
	$DURATION_SEC \
	$SAVE_DIR \
	$WARMUP_SEC \
	$RUN_WITH_VALIDATION
