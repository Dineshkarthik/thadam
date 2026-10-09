#!/bin/sh
# Waits for the SeaweedFS S3 gateway and creates the demo bucket.
#
# SeaweedFS without an IAM/filer auth config (this demo) accepts unsigned
# requests, so a plain PUT is enough; 200 = created, 409 = already there.
# Verified against chrislusf/seaweedfs:3.81 on 2026-10-09.
set -eu

ENDPOINT="${S3_ENDPOINT:-http://seaweedfs:9000}"
BUCKET="${S3_BUCKET:-thadam-demo}"

attempts=0
until curl -sf --max-time 5 "$ENDPOINT/" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 60 ]; then
        echo "seaweedfs-init: S3 gateway at $ENDPOINT did not become ready" >&2
        exit 1
    fi
    sleep 1
done

code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X PUT "$ENDPOINT/$BUCKET")
case "$code" in
    200) echo "seaweedfs-init: created bucket $BUCKET" ;;
    409) echo "seaweedfs-init: bucket $BUCKET already exists" ;;
    *)
        echo "seaweedfs-init: PUT /$BUCKET returned HTTP $code" >&2
        exit 1
        ;;
esac
