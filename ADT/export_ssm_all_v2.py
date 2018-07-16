#!/usr/bin/python

# schd Output ALL Script
# Version 2

# we only need the datetime class & the static function strptime from datetime module
from datetime import datetime
import csv
import logging
import os
import re
import shutil
import sys
import uuid
# best postgresql module so far, install it "yum install python-psycopg2"
import psycopg2
# argparse is std lib in 2.7 - install it for 2.6 "yum install python-argparse"
import argparse

_sql = {
    'output_ALL': """
		select
			S1.voyageid,

			S1.source,

			S1.acl_id,

			S1.flight_id,
			S1.carrier_id,
			S1.leg_id,
			S1.oag_type,
			S1.flight_transid,

			S1.arr_dep,
			
			S1.airline_iata,
			S1.airline_icao,
			S1.flightnumber,
			S1.codeshare,
			S1.operating_flightnumber,
			S1.flight_date as date,
			S1.std,
			S1.off,
			S1.out,
			S1.down,
			S1.on,
			S1.sta,
			S1.eta,
			S1.ata,
			S1.origin_iata,
			S1.origin_icao,
			S1.destination_iata,
			S1.destination_icao,
			S1.airport_iata,
			S1.airport_icao,
			S1.last_next_iata,
			S1.last_next_icao,
			S1.orig_dest_iata,
			S1.orig_dest_icao,
			S1.aircraft_iata,
			S1.aircraft_icao,
			S1.seats,
			S1.pax,
			S1.lastupdated,
			S1.flight_type,
			S1.pax_flight,
			S1.origin_status,
			S1.destination_status,
			S1.etd,
			S1.atd
		from schd S1
		join (
			select
			distinct voyageid
			from schd S
			--where lastupdated > (select source_date from history where source_type='S')
			--and not exists ( select 1 from servicetypes C where C.service_type_exclude is not null and S.flight_type = C.servicetype_code )
			--where lastupdated::date = '2015-09-28'::date
			--limit 10
		) S2 on S1.voyageid=S2.voyageid
		--where S1.flight_date > S1.lastupdated
		--and S1.flight_date <= S1.lastupdated + interval '48 hours'
		--where S1.flight_date > '2015-09-28'::date
		--and S1.flight_date <= '2015-09-28'::date + interval '48 hours'		
		order by 
			S1.voyageid, 
			S1.lastupdated	
	"""
}

_dbfields = [
    'voyageid',
    'source',
    'acl_id',
    'flight_id',
    'carrier_id',
    'leg_id',
    'oag_type',
    'flight_transid',
    'arr_dep',
    'airline_iata',
    'airline_icao',
    'flightnumber',
    'codeshare',
    'operating_flightnumber',
    'date',
    'std',
    'off',
    'out',
    'down',
    'on',
    'sta',
    'eta',
    'ata',
    'origin_iata',
    'origin_icao',
    'destination_iata',
    'destination_icao',
    'airport_iata',
    'airport_icao',
    'last_next_iata',
    'last_next_icao',
    'orig_dest_iata',
    'orig_dest_icao',
    'aircraft_iata',
    'aircraft_icao',
    'seats',
    'pax',
    'lastupdated',
    'flight_type',
    'pax_flight',
    'origin_status',
    'destination_status',
    'etd',
    'atd'
]

_csvfields = [
    'airline_iata',
    'airline_icao',
    'flightnumber',
    'codeshare',
    'operating_flightnumber',
    'date',
    'std',
    'off',
    'out',
    'down',
    'on',
    'sta',
    'eta',
    'ata',
    'origin_iata',
    'origin_icao',
    'destination_iata',
    'destination_icao',
    'airport_iata',
    'airport_icao',
    'last_next_iata',
    'last_next_icao',
    'orig_dest_iata',
    'orig_dest_icao',
    'aircraft_iata',
    'aircraft_icao',
    'seats',
    'pax',
    'lastupdated',
    'flight_type',
    'pax_flight',
    'origin_status',
    'destination_status',
    'etd',
    'atd',
    'voyageid'
]


def outputfile_ALL(dbh, outputdir, archivedir=None):
    module_logger = logging.getLogger()
    status = 1
    message = 'ok'

    flights = {}

    # make output folder if it doesn't exist
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    archivemode = False

    if archivedir is not None and archivedir != outputdir:
        module_logger.debug("Archive Mode")

        archivemode = True

        if not os.path.exists(archivedir):
            os.makedirs(archivedir)

    # ssm_output_2015-09-30_23-50-00.csv

    if os.path.isdir(outputdir):
        timestr = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        csvfilename = os.path.join(outputdir, 'ssm_output_ALL_' + timestr + '.csv')
        trigfilename = os.path.join(outputdir, 'ssm_output_ALL_' + timestr + '.trg')

        archivefilename = None
        if archivemode:
            archivefilename = os.path.join(archivedir, 'ssm_output_ALL_' + timestr + '.csv')

        module_logger.info("Output to %s %s", csvfilename, trigfilename)

        csr = dbh.cursor()

        csr.execute(_sql['output_ALL'])

        # only dump a csv if we have data
        if csr.rowcount > 0:
            for rec in csr:
                voyageid = rec[0]

                if voyageid not in flights:
                    flights[voyageid] = {}

                for k, v in enumerate(_dbfields):
                    # if the value is not null then we can overwrite
                    if rec[k] is not None:
                        flights[voyageid][v] = rec[k]
                    # if the field hasn't been seen before, add it
                    elif v not in flights[voyageid]:
                        flights[voyageid][v] = None

                module_logger.debug("F %s %s", voyageid, flights[voyageid])
        else:
            status = 1
            message = 'No new data - no file produced'
            module_logger.warning("No data")

        csr.close()

        if len(flights.keys()) > 0:
            try:
                with open(csvfilename, 'wb') as csvfile:
                    csvwrite = csv.DictWriter(csvfile, fieldnames=_csvfields, extrasaction='ignore',
                                              quoting=csv.QUOTE_NONNUMERIC)

                    # header
                    # csvwrite.writeheader() # 2.7 only
                    csvwrite.writerow(dict((fn, fn) for fn in _csvfields))

                    # iterate the record set
                    for voyageid in flights:
                        guid = uuid.uuid5(uuid.NAMESPACE_DNS, voyageid)

                        flights[voyageid]['voyageid'] = guid

                        csvwrite.writerow(flights[voyageid])

                open(trigfilename, 'wb').close()

                if archivemode:
                    module_logger.info("Copying to archive %s", archivefilename)
                    shutil.copyfile(csvfilename, archivefilename)

            except:
                status = -2
                message = "Error creating CSV " + csvfilename
                module_logger.error("Error creating CSV %s %s", csvfilename, trigfilename)
    # end if

    else:
        status = -2
        message = "Not a valid output folder " + outputdir
        module_logger.error("Not a valid output folder %s", outputdir)

    # if debug:
    #	print "FS",flights

    return (status, message)


# end def outputfile

def main():
    parser = argparse.ArgumentParser(description='Export SSM data into CSV files')
    parser.add_argument('-d', '--database', required=True, help='database name')
    parser.add_argument('-u', '--username', required=True, help='database username')
    parser.add_argument('-p', '--password', nargs='?', default='', help='database password for username')
    parser.add_argument('-D', '--DEBUG', default=False, action='store_true', help='Debug mode logging')
    # optional folder to export to - otherwise it uses current working directory
    parser.add_argument('-f', '--folder', nargs='?', default=os.getcwd(),
                        help='Folder to import from (default: current working directory)')
    # optional archive folder
    parser.add_argument('-a', '--archive', nargs='?',
                        help='Folder to archive imported files to (default: current working directory)')

    args = parser.parse_args()

    os.chdir('/ADT/scripts')

    if args.DEBUG:
        logging.basicConfig(
            filename='ssm.log',
            format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.DEBUG
        )
    else:
        logging.basicConfig(
            filename='ssm.log',
            format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO
        )

    logger = logging.getLogger()

    logger.info("schd export %s %s", args.folder, args.archive)
    dbh = psycopg2.connect(database=args.database, user=args.username, password=args.password)

    status, message = outputfile_ALL(dbh, args.folder, args.archive)

    dbh.close()

    logger.info("Status: %s Message: %s", status, message)

    print
    status
    print
    message


if __name__ == '__main__':
    main()
