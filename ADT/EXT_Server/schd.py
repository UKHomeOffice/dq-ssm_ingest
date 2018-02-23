## Schedule Output Module

# SCHD Output Script
# Version 1.3

# we only need the datetime class & the static function strptime from datetime module
from datetime import datetime
import re
import sys
import os
import csv
import uuid
import shutil
import logging
# best postgresql module so far, install it "yum install python-psycopg2"
import psycopg2

module_logger = logging.getLogger('schd')

_sql={
	'output' : """
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
			where lastupdated > (select source_date from history where source_type='S')
			and not exists ( select 1 from servicetypes C where C.service_type_exclude is not null and S.flight_type = C.servicetype_code )
		) S2 on S1.voyageid = S2.voyageid
		order by
			S1.voyageid,
			S1.lastupdated
	""",

	'history' : """
		update history
		set source_date = %(lastupdated)s::timestamp
		where source_type='S'
	""",

	'aclmerge' : """
		insert into schd (
			voyageid,
			source,
			acl_id,
			flight_id,
			carrier_id,
			leg_id,
			oag_type,
			flight_transid,
			arr_dep,
			airline_iata,
			airline_icao,
			flightnumber,
			codeshare,
			operating_flightnumber,
			flight_date,
			std,
			"off",
			"out",
			down,
			"on",
			sta,
			eta,
			ata,
			origin_iata,
			origin_icao,
			destination_iata,
			destination_icao,
			airport_iata,
			airport_icao,
			last_next_iata,
			last_next_icao,
			orig_dest_iata,
			orig_dest_icao,
			aircraft_iata,
			aircraft_icao,
			seats,
			pax,
			lastupdated,
			flight_type,
			pax_flight,
			origin_status,
			destination_status,
			etd,
			atd
		)
		select
			voyageid,
			source,
			acl_id,
			flight_id,
			carrier_id,
			leg_id,
			oag_type,
			flight_transid,
			arr_dep,
			airline_iata,
			airline_icao,
			flightnumber,
			codeshare,
			operating_flightnumber,
			flight_date,
			std,
			"off",
			"out",
			down,
			"on",
			sta,
			eta,
			ata,
			origin_iata,
			origin_icao,
			destination_iata,
			destination_icao,
			airport_iata,
			airport_icao,
			last_next_iata,
			last_next_icao,
			orig_dest_iata,
			orig_dest_icao,
			aircraft_iata,
			aircraft_icao,
			seats,
			pax,
			NOW() as lastupdated,
			flight_type,
			pax_flight,
			origin_status,
			destination_status,
			etd,
			atd
		from acl_schd
	""",

	'acltruncate' : """
		truncate table acl_schd
	""",

}

_dbfields=[
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

_csvfields=[
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

def aclmerge(dbh):
	status = 1
	message = 'ok'

	module_logger.info("Looking for ACL Data To Merge")

	csr = dbh.cursor()

	try:
		csr.execute(_sql['aclmerge'])

		rowcount = csr.rowcount

		module_logger.info("ACL rows merged: %s", rowcount)

		module_logger.debug("Q %s", csr.query)
		module_logger.debug("R %s", csr.statusmessage)

		if rowcount > 0:
			module_logger.info("Purging ACL Merge")

			csr.execute(_sql['acltruncate'])

			module_logger.debug("Q %s", csr.query)
			module_logger.debug("R %s", csr.statusmessage)

		csr.close()

		module_logger.debug("Commit")
		dbh.commit()
		status = 1
	except:
		module_logger.exception("Rollback")
		csr.close()
		dbh.rollback()

		status=-2
		message = "Error processing ACL Merge"
		module_logger.error("Error processing ACL Merge")

	module_logger.info("ACL Merge Complete");

	return (status, message)
#end def aclmerge

def outputfile(dbh,outputdir, archivedir=None):

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
		csvfilename = os.path.join(outputdir,'ssm_output_'+timestr+'.csv')
		trigfilename = os.path.join(outputdir,'ssm_output_'+timestr+'.trg')

		archivefilename = None
		if archivemode:
			archivefilename = os.path.join(archivedir,'ssm_output_'+timestr+'.csv')

		module_logger.info("Output to %s %s",csvfilename, trigfilename)

		csr = dbh.cursor()

		csr.execute( _sql['output'] )

		lastupdate = None

		# only dump a csv if we have data
		if csr.rowcount > 0:
			for rec in csr:
				voyageid = rec[0]

				if voyageid not in flights:
					flights[voyageid] = {}

				for k,v in enumerate(_dbfields):
					# if the value is not null then we can overwrite
					if rec[k] is not None:
						flights[voyageid][v] = rec[k]
					# if the field hasn't been seen before, add it
					elif v not in flights[voyageid]:
						flights[voyageid][v] = None

				if lastupdate is not None:
					if lastupdate < flights[voyageid]['lastupdated']:
						lastupdate = flights[voyageid]['lastupdated']
				else:
					lastupdate = flights[voyageid]['lastupdated']

				module_logger.debug("F %s %s",voyageid,flights[voyageid])
		else:
			status = 1
			message = 'No new data - no file produced'
			module_logger.warning("No data")

		csr.close()

		if len(flights.keys()) > 0:
			try:
				with open(csvfilename,'wb') as csvfile:
					csvwrite = csv.DictWriter(csvfile, fieldnames=_csvfields, extrasaction='ignore', quoting=csv.QUOTE_NONNUMERIC)

					# header
					#csvwrite.writeheader() # 2.7 only
					csvwrite.writerow(dict((fn,fn) for fn in _csvfields))

					# iterate the record set
					for voyageid in flights:
						guid = uuid.uuid5(uuid.NAMESPACE_DNS, voyageid)

						flights[voyageid]['voyageid'] = guid

						csvwrite.writerow(flights[voyageid])

				if archivemode:
					module_logger.info("Copying to archive %s", archivefilename)
					shutil.copyfile(csvfilename, archivefilename)

				open(trigfilename,'wb').close()

				try:
					module_logger.debug("history update")

					csr = dbh.cursor()

					csr.execute(_sql['history'], {'lastupdated': lastupdate })

					csr.close()

					module_logger.debug("Commit")

					dbh.commit()

				except:
					module_logger.exception("Rollback")

					csr.close()

					dbh.rollback()

					status=-2
					message = "Error updating last output status"

			except:
				status=-2
				message = "Error creating CSV "+csvfilename
				module_logger.error("Error creating CSV %s %s", csvfilename, trigfilename)
		#end if

	else:
		status=-2
		message = "Not a valid output folder "+outputdir
		module_logger.error("Not a valid output folder %s",outputdir)

	#if debug:
	#	print "FS",flights


	return (status, message)
#end def outputfile
