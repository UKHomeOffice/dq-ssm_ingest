#!/usr/bin/python

# test acl import Script
# Version 1.3

# we only need the datetime class & the static function strptime from datetime module
from datetime import datetime
import re
import sys
import os
import logging
# best postgresql module so far, install it "yum install python-psycopg2"
import psycopg2
# argparse is std lib in 2.7 - install it for 2.6 "yum install python-argparse"
import argparse
import time

## local modules
import acl

def main():
	parser = argparse.ArgumentParser(description='Import ACL HOMEOFFICEROLL3_YYYYMMDD.CSV files')
	parser.add_argument('-d','--database', required=True, help='database name')
	parser.add_argument('-u','--username', required=True, help='database username')
	parser.add_argument('-p','--password', nargs='?', default='', help='database password for username')
	parser.add_argument('-D','--DEBUG',  default=False, action='store_true', help='Debug mode logging')
	# optional folder to import from - otherwise it uses current working directory
	parser.add_argument('-f','--folder', nargs='?', default=os.getcwd(), help='Folder to import from (default: current working directory)')
	# optional archive folder
	parser.add_argument('-a','--archive', nargs='?', help='Folder to archive imported files to (default: current working directory)')
	parser.add_argument('-l','--log', nargs='?', default=os.getcwd(), help='Folder to log to (default: current working directory)')

	args = parser.parse_args()
	YYYYMMDDSTR = time.strftime("%Y%m%d")
	os.chdir('/ADT/scripts')

	if args.DEBUG:
		logging.basicConfig(
			filename=os.path.join(args.log,'acl_'+YYYYMMDDSTR+'.log'),
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.DEBUG
		)
	else:
		logging.basicConfig(
			filename=os.path.join(args.log,'acl_'+YYYYMMDDSTR+'.log'),
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.INFO
		)

	dbh=psycopg2.connect(database= args.database, user=args.username, password=args.password)

	logger=logging.getLogger()
	
	logger.info("acl import %s %s", args.folder, args.archive)
	status, message=acl.import_folder(dbh, args.folder, args.archive)

	dbh.close()

	logger.info("Status %s", status)

	print status
	print message

if __name__ == '__main__':
    main()
