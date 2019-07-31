#!/usr/bin/env bash
# need to run "gcloud auth configure-docker" once
# to test at any point: docker run --rm -it 1fb7c2e5dc16 sh
docker build -t warcraider . && docker tag warcraider gcr.io/dta-ga-bigquery/warcraider && docker push gcr.io/dta-ga-bigquery/warcraider
