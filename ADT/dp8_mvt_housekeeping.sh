#!/bin/bash

cd /ADT/scripts

/ADT/scripts/housekeeping_mvt.py -H $RDS_POSTGRES_DATA_INGEST_HOST_NAME -d mvt -u $MVT_SCHEMA_SSM_USERNAME -p $MVT_SCHEMA_SSM_PASSWORD -r $1

