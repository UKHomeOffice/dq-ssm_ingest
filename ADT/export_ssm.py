#!/usr/bin/python

# test schd Output Script
# Version 1.3

# we only need the datetime class & the static function strptime from datetime module
from datetime import datetime
import re
import sys
import os
import csv
import logging
# best postgresql module so far, install it "yum install python-psycopg2"
import psycopg2
# argparse is std lib in 2.7 - install it for 2.6 "yum install python-argparse"
import argparse
import time

## local modules
import schd

def main():
        YYYYMMDDSTR = time.strftime("%Y%m%d")
	parser = argparse.ArgumentParser(description='Export SSM data into CSV files')
	parser.add_argument('-d','--database', required=True, help='database name')
	parser.add_argument('-u','--username', required=True, help='database username')
	parser.add_argument('-p','--password', nargs='?', default='', help='database password for username')
	parser.add_argument('-D','--DEBUG',  default=False, action='store_true', help='Debug mode logging')
	# optional folder to export to - otherwise it uses current working directory
	parser.add_argument('-f','--folder', nargs='?', default=os.getcwd(), help='Folder to import from (default: current working directory)')
	# optional archive folder
	parser.add_argument('-a','--archive', nargs='?', help='Folder to archive imported files to (default: current working directory)')
        parser.add_argument('-l','--log', nargs='?', default=os.getcwd(), help='Folder to log to (default: current working directory)')

	args = parser.parse_args()

	os.chdir('/ADT/scripts')

	if args.DEBUG:
		logging.basicConfig(
			filename=os.path.join(args.log,'ssm_'+YYYYMMDDSTR+'.log'),
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S', 
			level=logging.DEBUG
		)
	else:
		logging.basicConfig(
			filename=os.path.join(args.log,'ssm_'+YYYYMMDDSTR+'.log'),
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S', 
			level=logging.INFO
		)

	logger=logging.getLogger()
	
	logger.info("schd export %s %s", args.folder, args.archive)
	dbh=psycopg2.connect(
		database='mvt',
		user=os.environ['MVT_SCHEMA_SSM_USERNAME'],
		host=os.environ['RDS_POSTGRES_DATA_INGEST_HOST_NAME'],
		password=os.environ['MVT_SCHEMA_SSM_PASSWORD']
	)
	
	status, message=schd.aclmerge(dbh)

	if status == 1:
		status, message=schd.outputfile(dbh, args.folder, args.archive)

	dbh.close()

	logger.info("Status: %s Message: %s", status, message)

	print status
	print message

if __name__ == '__main__':
    main()
