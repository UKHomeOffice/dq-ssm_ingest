#!/bin/bash

cd /ADT/scripts

/ADT/scripts/import_acl.py -d mvt -u $MVT_SCHEMA_SSM_USERNAME -p $MVT_SCHEMA_SSM_PASSWORD -f /ADT/data/acl -a /ADT/archive/acl -l /ADT/log

