#!/usr/bin/python

# Perform housekeeping on mvt database
# Version 1.1
# Ian Relf

from datetime import datetime
import re
import sys
import os
import logging
import psycopg2
import argparse

_sql={
	'hk_acl_csv' : """
		delete from acl_csv
 		where acl_date < date %(retention_date)s
	""",

	'hk_schd' : """
		delete from schd
 		where flight_date < date %(retention_date)s
	""",

	'hk_oag_flight' : """
		delete from oag_flight
 		where flight_sent_datetime < date %(retention_date)s
	""",

}

def main():
	parser = argparse.ArgumentParser(description='Perform mvt database housekeeping deletion')
        parser.add_argument('-H','--host',     required=False,                     help='host name')
        parser.add_argument('-d','--database', required=True,                      help='database name')
        parser.add_argument('-u','--username', required=True,                      help='database username')
        parser.add_argument('-p','--password', nargs='?', default='',              help='database password for username')
        parser.add_argument('-r','--retain',   required=True,                      help='oldest date to retain')
        parser.add_argument('-D','--DEBUG',    default=False, action='store_true', help='Debug mode logging')

	args = parser.parse_args()

	os.chdir('/ADT/scripts')

	if args.DEBUG:
		logging.basicConfig(
			filename='hk.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.DEBUG
		)
	else:
		logging.basicConfig(
			filename='hk.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.INFO
		)

        logger=logging.getLogger()

        logger.debug("Connecting to MVT DB...")

        dbh=psycopg2.connect(host=args.host, database= args.database, user=args.username, password=args.password)

        logger.debug("Connected to MVT DB.")

	logger.info("mvt housekeeping %s", args.retain)

	logger.info("hk_acl_csv")

	csr=dbh.cursor()

	csr.execute(_sql['hk_acl_csv'], {'retention_date': args.retain })

	csr.close()

	logger.info("hk_schd")

	csr=dbh.cursor()

	csr.execute(_sql['hk_schd'], {'retention_date': args.retain })

	csr.close()

	logger.info("hk_oag_flight")

	csr=dbh.cursor()

	csr.execute(_sql['hk_oag_flight'], {'retention_date': args.retain })

	csr.close()

	logger.info("complete")

	dbh.commit()

	dbh.close()

	logger.info("exiting")

if __name__ == '__main__':
    main()
