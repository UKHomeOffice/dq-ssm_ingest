#!/bin/bash

cd /ADT/scripts

/ADT/scripts/export_ssm.py -d mvt -u $MVT_SCHEMA_SSM_USERNAME -p $MVT_SCHEMA_SSM_PASSWORD -f /ADT/output -a /ADT/archive/ssm -l /ADT/log

