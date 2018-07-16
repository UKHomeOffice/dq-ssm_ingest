## ACL Import Module

# ACL CSV Import 
# Version 5
# 2015-10-30

# we only need the datetime class & the static function strptime from datetime module
from datetime import datetime
import re
import sys
import os
import logging
# best postgresql module so far, install it "yum install python-psycopg2"
import psycopg2
import csv
import shutil
from tempfile import mkstemp 

module_logger = logging.getLogger('acl')

_sql={
	'schd' : """		
		insert into acl_schd
		(
			source,
			acl_id,
			flight_id,
			carrier_id,
			leg_id,
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
			atd,
			arr_dep,
			oag_type,
			flight_transid,
			voyageid
		)
		select
			source,
			acl_id as acl_id,
			null as flight_id,
			null as carrier_id,
			null as leg_id,		
			airline_iata,
			airline_icao,		 
			acl_flightnumber as flightnumber,
			0 as codeshare,
			null as operating_flightnumber,
			acl_date as flight_date,
			(case when acl_arrdep = 'D' then flight_datetime end) as std,
			null as off,
			null as out,
			null as down,
			null as on,
			(case when acl_arrdep = 'A' then flight_datetime end) as sta,
			null as eta,
			null as ata,
			acl_origin as origin_iata,
			acl_origin_icao as origin_icao,
			acl_dest as destination_iata,
			acl_dest_icao as destination_icao,		
			acl_airport as airport_iata,
			airport_icao as airport_icao,
			acl_last_next_iata as last_next_iata,
			acl_last_next_icao as last_next_icao,
			acl_orig_dest_iata as orig_dest_iata,
			acl_orig_dest_icao as orig_dest_icao,
			acl_aircraft_iata as aircraft_iata,
			acl_aircraft_icao as aircraft_icao,
			null as seats,
			null as pax,
			lastupdated at time zone 'UTC' as lastupdated,
			acl_service_type as flight_type,
			servicetype_pax::int as pax_flight,
			null as origin_status,
			null as destination_status,
			null as etd,
			null as atd,
			acl_arrdep as arr_dep,
			null as oag_type,
			null as flight_transid,
			( acl_origin || acl_dest || to_char(acl_date,'YYYYMMDD') || btrim(coalesce(airline_iata,airline_icao)) || btrim(acl_flightnumber) || acl_arrdep) as voyageid
		from (
				select 
					A.acl_id,
					A.acl_file_date,
					A.acl_date,
					A.acl_time,
					A.acl_arrdep,
					A.acl_airport,				
					A.acl_last_next_iata,
					A.acl_orig_dest_iata,
					A.acl_aircraft_iata,
					A.acl_last_next_icao,
					A.acl_orig_dest_icao,
					A.acl_aircraft_icao,
					A.acl_operator_airline_code,
					A.acl_flightnumber,
					A.acl_service_type,
					A.acl_aircraft_reg,
					A.acl_edit_date,				
					OFA.airline_iata,
					OFA.airline_icao,
					OF1.airport_icao,
					-- calculated fields
					A.acl_date + A.acl_time at time zone 'UTC' as flight_datetime,
					--A.acl_date + A.acl_time as flight_datetime,
					(case when A.acl_arrdep = 'D' then A.acl_airport else A.acl_last_next_iata end) as acl_origin,
					(case when A.acl_arrdep = 'A' then A.acl_airport else A.acl_last_next_iata end) as acl_dest,
					(case when A.acl_arrdep = 'D' then OF1.airport_icao else A.acl_last_next_icao end) as acl_origin_icao,
					(case when A.acl_arrdep = 'A' then OF1.airport_icao else A.acl_last_next_icao end) as acl_dest_icao,
					T.servicetype_pax,
					source_type as source,
					source_date as lastupdated
				from acl_csv A
				join history H
					on H.source_type='A'
				join of_airlines OFA
					on ( OFA.airline_iata = A.acl_operator_airline_code and OFA.airline_active='Y' )
					or OFA.airline_icao = A.acl_operator_airline_code
				left join of_airports OF1
					on OF1.airport_iata = A.acl_airport	
				left join servicetypes T 
					on acl_service_type = servicetype_code
				where acl_file_type = 3				
					and acl_file_date > coalesce(
					(
						select	
							A.acl_file_date
						from acl_csv A
						join (
							select
								max(acl_id) as acl_id,
								max(lastupdated) as lastupdated
							from (
							select 
								max(acl_id) as acl_id,
								max(lastupdated) as lastupdated 
							from schd 
							where source='A'
							group by lastupdated
							UNION
							select
								max(acl_id) as acl_id,
								max(lastupdated) as lastupdated
							from acl_schd
							where source='A'
							group by lastupdated
							) Z
						) B
						on B.acl_id = A.acl_id
					), '2015-01-01'::date)			
		-- 			and acl_file_date >= '2015-09-14'::date
					and acl_date > acl_file_date -- because acl data turns up nearly 24 hours late
		) Z	
	""",
	
	'copy' : """
		copy acl_csv (
			acl_aircraft_iata,
			acl_aircraft_reg,
			acl_airport,
			acl_arrdep,
			acl_created_date,
			acl_date,
			acl_doop,
			acl_edit_date,
			acl_aircraft_icao,
			acl_last_next_icao,
			acl_orig_dest_icao,
			acl_last_next_iata,
			acl_last_next_country,
			acl_operator_airline_code,
			acl_operator_group_name,
			acl_operator_name,
			acl_orig_dest_iata,
			acl_orig_dest_country,
			acl_terminal_name,
			acl_season,
			acl_seats,
			acl_flightnumber,
			acl_service_type,
			acl_turnaround,
			acl_terminal,
			acl_time,
			acl_turn_operator_airline_code,
			acl_turn_flightnumber,
			acl_flightdesignator,
			acl_loadfactor
		) from stdin
		with csv header	
	""",
	
	'history' : """
		update history 
		set source_date = now() 
		where source_type='A'
	""",
	
	'update' : """
		update acl_csv 
		set acl_filename = %s, 
			acl_file_date = %s, 
			acl_file_type = %s 
		where acl_filename is null
	""",
	
	'last' : """
		select 
			extract(epoch from source_date) as lastupdated 
		from history 
		where source_type='A'
	""",
	
	'file_check' : """
		select
			acl_filename 
		from acl_csv
		where acl_filename = %(filename)s
	""",	
	'lock_acl_schd' : """
		LOCK TABLE acl_schd IN ACCESS EXCLUSIVE MODE
	"""
}

# import an ACL CSV file
# Args:
#	dbh = database handle (psycopg2)
#	filename = csv filename  

def importfile(dbh, filename):
	module_logger.info("Import %s", filename)
	
	status=1
	message='OK'

	if os.path.isfile(filename):

		# break the file name into useful parts

		# optional airport & season prefix we actually ignore eg "LHRS15"
		# mandatory "HOMEOFFICEROLL"
		# mandatory integer filetype usually 1 or 3 or 180
		# manatory file date in YYYYMMDD
		# note case insensitive as we have been given files with both uppercase and lowercase filenames

		match = re.search('^(.*?)homeofficeroll(\d+)_(\d{4}\d{2}\d{2})\.csv$', os.path.basename(filename), re.I)
		
		if match is not None:		
			date = datetime.strptime(match.group(3), '%Y%m%d').date()
			filetype= match.group(2)
			
			module_logger.info("Importing %s", filename)

			module_logger.debug("Processing File %s Date %s File Type %s Filename %s", filename, date, filetype, os.path.basename(filename))

			# copy data use postgresql copy command, pyscopg2 allows use to do so from a python file handle via "stdin"

			csr=dbh.cursor()

			try:
				f=open(filename)
				csr.copy_expert(sql=_sql['copy'], file=f)

				# add file info to the rows we just imported
				csr.execute(_sql['update'], (os.path.basename(filename), date, filetype))

				module_logger.debug("Q %s", csr.query)
				module_logger.debug("R %s", csr.statusmessage)

				csr.execute(_sql['history'])


				module_logger.debug("Q %s", csr.query)
				module_logger.debug("R %s", csr.statusmessage)

				# gain an AccessExlusive lock on the acl_schd table to prevent race condition with schd.py
				csr.execute(_sql['lock_acl_schd'])

				module_logger.debug("Q %s", csr.query)
				module_logger.debug("R %s", csr.statusmessage)

				csr.execute(_sql['schd'])

				module_logger.debug("Q %s", csr.query)
				module_logger.debug("R %s", csr.statusmessage)

				csr.close()

				module_logger.debug("Commit")
				dbh.commit()
				status=1

			except:
				module_logger.exception("Rollback")
				csr.close()
				dbh.rollback()

				status=-2
				message="Error processing "+filename
				module_logger.error("Error processing %s", filename)

			finally:
				f.close()
		else:	
			status=-2
			message="Invalid filename skipping "+filename
			module_logger.warning("File name doesn't match %s", filename)
	else:
		status=-2
		message="File not found"+filename
		module_logger.error("File not found %s", filename)
			
	return status,message
# end def importfile

def _sortkey(filename):
	match = re.search('^(.*?)homeofficeroll(\d+)_(\d{4}\d{2}\d{2})(.*?)$', filename, re.I)
	
	key=''
	
	if match is not None:
		key=match.group(3)
	return key
#end def _sortkey 

def validate_file(f , renamebad=True, baddir=None):
	#logger=logging.getLogger()
	
	if baddir is not None:
		if not os.path.exists(baddir): 
			os.makedirs(baddir)
	
	module_logger.info("Validating %s", f)

	ok=True
	header=False
	rewritten=False
	header_row=0
	rowcount=0
	tf=None
	
	with open(f, 'rb') as file:
		reader = csv.reader(file)
		
		
		for row in reader:
			if not header:
				if len(row) == 30 and row[0] == 'A/C':
					header=True
				else:
					header_row+=1
			else:
				if len(row) != 30:
					ok=False
					module_logger.info("Incorrect Row Size %s", rowcount)
			rowcount+=1
		# end for
		
		if not header:
			ok=False
			module_logger.info("No Header found")
			
		if header and header_row > 0:
			module_logger.info("Extra Header Rows found")
			
			fh, tf = mkstemp()
			
			file.seek(0)			
			reader = csv.reader(file)
			
			with open(tf, 'wb') as tmpfile:
				writer = csv.writer(tmpfile)
				
				r=0
				for row in reader:
					if r >= header_row:
						writer.writerow(row)
					r+=1
					
			module_logger.info("Rewrite file")
			rewritten=True

	if ok and rewritten and tf is not None:
		nf=None
		if renamebad:
			nf=f+'.bad'
			os.rename(f,nf)
		else:
			nf=os.path.join(baddir, os.path.basename(f))
			os.rename(f,nf)
		
		os.rename(tf,f)
		module_logger.info("Corrected %s Moved Bad File to %s", f, nf)

	if not ok:
		nf=None
		if renamebad:
			nf=f+'.bad'
			os.rename(f,nf)
		else:
			nf=os.path.join(baddir, os.path.basename(f))
			os.rename(f,nf)
		module_logger.info("Moved Bad File %s to %s", f, nf)		
		
	return ok
#end def validate_file

def import_folder(dbh, sourcedir, archivedir=None, debug=False):
	
	status=1
	message='OK'
	
	archivemode=False
	
	if archivedir is not None and archivedir != sourcedir:
		archivemode=True
		
		if not os.path.exists(archivedir):
			os.makedirs(archivedir)	
			
			
	#retrieve last imported file date/time
	
	csr=dbh.cursor()
	
	filecount=0
	
	if os.path.isdir(sourcedir):
		filelist=sorted(os.listdir(sourcedir), key = _sortkey)
		
		if filelist is not None:
			for filename in filelist:
				f=os.path.join(sourcedir, filename)

				if os.path.isfile(f):
					# is it a .done file?
					
					match = re.search('^((.*?)homeofficeroll(\d+)_(\d{4}\d{2}\d{2})\.csv)\.done$', filename, re.I)

					if match is not None:
						# extract corresponding csv name & check it
						
						csvfilename=match.group(1)
						cf=os.path.join(sourcedir, csvfilename)
						
						if os.path.isfile(cf) and validate_file(cf, False, archivedir):
							csr.execute(_sql['file_check'], { 'filename': csvfilename } )
							
							# if the filename isn't found
							if csr.rowcount == 0: 
								module_logger.debug("F %s",cf)

								module_logger.info("Importing %s", cf)
								status, message=importfile(dbh, cf)

								if status != 1:
									module_logger.debug("Status %s bailing", status)
									break

								filecount+=1

								# only archive if status is good & archivedir
								if archivemode:
									nf=os.path.join(archivedir, csvfilename)
									os.rename(cf,nf)
								
						# remove the .done file
						os.unlink(f)
			#end for
			
			if filecount == 0:
				module_logger.error("No files imported")
				message='No files imported'
			else:
				module_logger.info("%s files imported", filecount)
				message="%s files imported" % filecount
		else:
			module_logger.error("No files found")
			status=-2
			message='No files found to import'
	else:
		status=-2
		module_logger.error("%s not a folder", sourcedir)
		message="Source Folder not found "+sourcedir
		
	csr.close()
		
	return status, message
	
# end def findfiles
