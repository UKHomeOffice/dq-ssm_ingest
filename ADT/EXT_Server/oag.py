## OAG Import Module

# OAG Flight Status XML Import
# Version 5
# 2015-10-30

# we use libxml2 directly rather than standard lxml as the libxml pull parser is most efficient "yum install libxml2-python"
import libxml2
# we only need the datetime class & the static function strptime from datetime module
from datetime import datetime
import time
import re
import sys
import os
import logging
# best postgresql module so far, install it "yum install python-psycopg2"
import psycopg2


# module globals

module_logger = logging.getLogger('oag')

# Build sql statements using named parameters

_sql={
	'oag_flight' : """
		insert into oag_flight
		(
			flight_transid,
			flight_sent_utc_datetime,
			flight_utcloc,
			flight_sent_datetime,
			flight_oag_type
		)
		values
		(
			%(flight_transid)s,
			%(flight_sent_utc_datetime)s,
			%(flight_utcloc)s,
			%(flight_sent_datetime)s,
			%(flight_oag_type)s
		)
		returning flight_id
	""",

	'oag_carrier' : """
		insert into oag_carrier
		(
			carrier_flight_id,
			carrier_code,
			carrier_flightnumber,
			carrier_codeshare_type
		)
		values
		(
			%(carrier_flight_id)s,
			%(carrier_code)s,
			%(carrier_flightnumber)s,
			%(carrier_codeshare_type)s
		)
	""",

	'oag_leg' : """
		insert into oag_leg
		(
			leg_arr_apt,
			leg_aircraft_ch,
			leg_dep_checkin_act,
			leg_dep_offblock_ch,
			leg_arr_onblock_est_datetime,
			leg_arr_delay_stat,
			leg_aircraft_reg_ch,
			leg_dep_trm_act,
			leg_dep_checkin_ch,
			leg_dep_delay_stat,
			leg_service_type,
			leg_arr_gate_ch,
			leg_dep_trm_ch,
			leg_arr_delay_det,
			leg_dep_airborne_ch,
			leg_arr_trm_act,
			leg_arr_trm_ch,
			leg_arr_down_ch,
			leg_flight_id,
			leg_arr_onblock_act_datetime,
			leg_dep_offblock_est_datetime,
			leg_dep_datetime_act_datetime,
			leg_arr_datetime_act_datetime,
			leg_dep_delay_catid,
			leg_arr_gate_act,
			leg_aircraft_schd,
			leg_dep_datetime_schd_datetime,
			leg_arr_city,
			leg_dep_datetime_est_datetime,
			leg_dep_offblock_act_datetime,
			leg_dep_delay_det,
			leg_dep_datetime_ch,
			leg_arr_claim_act,
			leg_arr_divert_city,
			leg_arr_delay_catid,
			leg_arr_datetime_ch,
			leg_arr_datetime_schd_datetime,
			leg_dep_trm_schd,
			leg_arr_datetime_est_datetime,
			leg_dep_gate_act,
			leg_arr_down_est_datetime,
			leg_dep_airborne_act_datetime,
			leg_arr_onblock_ch,
			leg_aircraft_act,
			leg_dep_city,
			leg_arr_down_act_datetime,
			leg_aircraft_reg_act,
			leg_arr_trm_schd,
			leg_dep_apt,
			leg_arr_divert_apt,
			leg_dep_airborne_est_datetime,
			leg_dep_gate_ch,
			leg_arr_claim_ch
		)
		values
		(
			%(leg_arr_apt)s,
			%(leg_aircraft_ch)s,
			%(leg_dep_checkin_act)s,
			%(leg_dep_offblock_ch)s,
			%(leg_arr_onblock_est_datetime)s,
			%(leg_arr_delay_stat)s,
			%(leg_aircraft_reg_ch)s,
			%(leg_dep_trm_act)s,
			%(leg_dep_checkin_ch)s,
			%(leg_dep_delay_stat)s,
			%(leg_service_type)s,
			%(leg_arr_gate_ch)s,
			%(leg_dep_trm_ch)s,
			%(leg_arr_delay_det)s,
			%(leg_dep_airborne_ch)s,
			%(leg_arr_trm_act)s,
			%(leg_arr_trm_ch)s,
			%(leg_arr_down_ch)s,
			%(leg_flight_id)s,
			%(leg_arr_onblock_act_datetime)s,
			%(leg_dep_offblock_est_datetime)s,
			%(leg_dep_datetime_act_datetime)s,
			%(leg_arr_datetime_act_datetime)s,
			%(leg_dep_delay_catid)s,
			%(leg_arr_gate_act)s,
			%(leg_aircraft_schd)s,
			%(leg_dep_datetime_schd_datetime)s,
			%(leg_arr_city)s,
			%(leg_dep_datetime_est_datetime)s,
			%(leg_dep_offblock_act_datetime)s,
			%(leg_dep_delay_det)s,
			%(leg_dep_datetime_ch)s,
			%(leg_arr_claim_act)s,
			%(leg_arr_divert_city)s,
			%(leg_arr_delay_catid)s,
			%(leg_arr_datetime_ch)s,
			%(leg_arr_datetime_schd_datetime)s,
			%(leg_dep_trm_schd)s,
			%(leg_arr_datetime_est_datetime)s,
			%(leg_dep_gate_act)s,
			%(leg_arr_down_est_datetime)s,
			%(leg_dep_airborne_act_datetime)s,
			%(leg_arr_onblock_ch)s,
			%(leg_aircraft_act)s,
			%(leg_dep_city)s,
			%(leg_arr_down_act_datetime)s,
			%(leg_aircraft_reg_act)s,
			%(leg_arr_trm_schd)s,
			%(leg_dep_apt)s,
			%(leg_arr_divert_apt)s,
			%(leg_dep_airborne_est_datetime)s,
			%(leg_dep_gate_ch)s,
			%(leg_arr_claim_ch)s
		)
	""",

	'history' : """
		update history
		set source_date=%(lastupdated)s
		where source_type='O'
	""",

	'schd' : """
		insert into schd
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
			null as acl_id,
			flight_id,
			carrier_id,
			leg_id,
			carrier_iata as airline_iata,
			carrier_icao as airline_icao,
			carrier_flightnumber as flightnumber,
			carrier_codeshare_type as codeshare,
			operating_flightnumber as operating_flightnumber,
			(case
				when arr_dep = 'A' then arr_datetime::date
				when arr_dep = 'D' then dep_datetime::date
			end) as flight_date,
			dep_datetime as std,
			offblock_datetime as off,
			airborne_datetime as out,
			down_datetime as down,
			on_datetime as on,
			arr_datetime as sta,
			arr_datetime as eta,
			arr_act_datetime as ata,
			leg_dep_apt as origin_iata,
			leg_dep_apt_icao as origin_icao,
			leg_arr_apt as destination_iata,
			leg_arr_apt_icao as destination_icao,
			(case
				when arr_dep = 'A' then leg_arr_apt
				when arr_dep = 'D' then leg_dep_apt
			end) as airport_iata,
			(case
				when arr_dep = 'A' then leg_arr_apt_icao
				when arr_dep = 'D' then leg_dep_apt_icao
			end) as airport_icao,
			(case
				when arr_dep = 'A' then leg_dep_apt
				when arr_dep = 'D' then leg_arr_apt
			end) as last_next_iata,
			(case
				when arr_dep = 'A' then leg_dep_apt_icao
				when arr_dep = 'D' then leg_arr_apt_icao
			end) as last_next_icao,
			null as orig_dest_iata,
			null as orig_dest_icao,
			aircraft_iata as aircraft_iata,
			aircraft_icao as aircraft_icao,
			null as seats,
			null as pax,
			flight_sent_utc_datetime  as lastupdated,
			leg_service_type as flight_type,
			servicetype_pax::int as pax_flight,
			leg_dep_delay_stat as origin_status,
			leg_arr_delay_stat as destination_status,
			dep_est_datetime as etd,
			dep_act_datetime as atd,
			arr_dep as arr_dep,
			flight_oag_type as oag_type,
			flight_transid as flight_transid,
			( leg_dep_apt || leg_arr_apt || to_char((case
				when arr_dep = 'A' then arr_datetime::date
				when arr_dep = 'D' then dep_datetime::date
			end),'YYYYMMDD') || btrim(coalesce(carrier_iata,carrier_icao)) || btrim(carrier_flightnumber) || arr_dep) as voyageid
		from (
				select
					F.flight_id,
					F.flight_transid,
					F.flight_sent_utc_datetime,
					F.flight_oag_type,
					C.carrier_id,
					C.carrier_code,
					lpad(C.carrier_flightnumber,4,'0') as carrier_flightnumber,
					coalesce(C.carrier_codeshare_type, 0) as carrier_codeshare_type,
					rpad(CS.carrier_code,3,' ') || lpad(CS.carrier_flightnumber,4,'0') as operating_flightnumber,
					OFA.airline_id,
					OFA.airline_iata as carrier_iata,
					OFA.airline_icao as carrier_icao,
					L.leg_id,
					L.leg_service_type,
					L.leg_dep_apt,
					L.leg_arr_apt,
					L.leg_dep_datetime_schd_datetime,
					L.leg_arr_datetime_schd_datetime,
					L.leg_aircraft_schd,
					L.leg_aircraft_act,
					OF1.airport_id as leg_dep_apt_id,
					OF2.airport_id as leg_arr_apt_id,
					OF1.airport_icao as leg_dep_apt_icao,
					OF2.airport_icao as leg_arr_apt_icao,
					OF1.airport_timezone_name,
					OF2.airport_timezone_name,
					L.leg_dep_delay_catid,
					L.leg_dep_delay_det,
					L.leg_dep_delay_stat,
					L.leg_arr_delay_catid,
					L.leg_arr_delay_det,
					L.leg_arr_delay_stat,
					-- calculated fields
					(case
						when length(C.carrier_flightnumber) < 4
						then '0' || C.carrier_flightnumber
						else C.carrier_flightnumber
					end) as oag_flightnumber,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_dep_datetime_schd_datetime
						else L.leg_dep_datetime_schd_datetime at time zone OF1.airport_timezone_name
					end) as dep_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_arr_datetime_schd_datetime
						else L.leg_arr_datetime_schd_datetime at time zone OF2.airport_timezone_name
					end) as arr_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_dep_datetime_est_datetime
						else L.leg_dep_datetime_est_datetime at time zone OF1.airport_timezone_name
					end) as dep_est_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_dep_datetime_act_datetime
						else L.leg_dep_datetime_act_datetime at time zone OF1.airport_timezone_name
					end) as dep_act_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_dep_offblock_act_datetime
						else L.leg_dep_offblock_act_datetime at time zone OF1.airport_timezone_name
					end) as offblock_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_dep_airborne_act_datetime
						else L.leg_dep_airborne_act_datetime at time zone OF1.airport_timezone_name
					end) as airborne_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_arr_down_act_datetime
						else L.leg_arr_down_act_datetime at time zone OF2.airport_timezone_name
					end) as down_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_arr_onblock_act_datetime
						else L.leg_arr_onblock_act_datetime at time zone OF2.airport_timezone_name
					end) as on_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_arr_datetime_est_datetime
						else L.leg_arr_datetime_est_datetime at time zone OF2.airport_timezone_name
					end) as arr_est_datetime,
					(case
						when F.flight_utcloc = 'UTC'
						then L.leg_arr_datetime_act_datetime
						else L.leg_arr_datetime_act_datetime at time zone OF2.airport_timezone_name
					end) as arr_act_datetime,
					(case
						when IA.airport_iata = leg_dep_apt then 'D'
						when IA.airport_iata = leg_arr_apt then 'A'
					end) as arr_dep,
					(case
						when leg_aircraft_ch = 'Y' then leg_aircraft_act
						else leg_aircraft_schd
					end) as aircraft_iata,
					(case
						when leg_aircraft_ch = 'Y' then AA1.aircraft_icao
						else AA2.aircraft_icao
					end) as aircraft_icao,
					T.servicetype_pax,
					'O' as source
				from oag_flight F
				join oag_leg L
					on L.leg_flight_id = F.flight_id
				join oag_carrier C
					on C.carrier_flight_id = F.flight_id
					and carrier_codeshare_type is not null
				join of_airlines OFA
					on OFA.airline_iata = C.carrier_code
					and airline_active='Y'
				join of_airports OF1
					on OF1.airport_iata = leg_dep_apt
				join of_airports OF2
					on OF2.airport_iata = leg_arr_apt
				join servicetypes T
					on L.leg_service_type = servicetype_code
				join interested_airports IA
					on IA.airport_iata = leg_dep_apt
					or IA.airport_iata = leg_arr_apt
				left join oag_carrier CS
					on CS.carrier_flight_id = F.flight_id
					and ( CS.carrier_codeshare_type is null or CS.carrier_codeshare_type = 0 )
					and C.carrier_codeshare_type = 1
				left join bb_aircraft AA1
					on leg_aircraft_act = AA1.aircraft_iata
				left join bb_aircraft AA2
					on leg_aircraft_schd = AA2.aircraft_iata
				where
					flight_sent_utc_datetime > (select coalesce(max(lastupdated),'2015-01-01'::date) as d from schd where source='O')
					-- flight_sent_utc_datetime::date >= '2015-09-14'::date
		) Z
		order by
			dep_datetime,
			arr_datetime,
			carrier_code,
			carrier_flightnumber,
			flight_sent_utc_datetime,
			flight_id
	""",

	'file_check' : """
		select
			*
		from oag_files
		where filename = %(filename)s
	""",

	'file_update' : """
		insert into oag_files
		(
			filename
		)
		values
		(
			%(filename)s
		)
	""",

}

# for each xml file
def _process_file(dbh,xml):
	module_logger.debug("processing file")
	status=1
	message='OK'

	flights={}
	sent=None
	sentutc=None
	locutc=None

	if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'FIMSSR':
		module_logger.debug('FIMSSR %s %s',xml.NodeType(),xml.Name())

		# process FIMSSR wrapper
		sent=xml.GetAttribute('Sent')
		sentutc=xml.GetAttribute('UTCSent')
		locutc=xml.GetAttribute('UTCLOCInd')

		# Flight Loop
		while xml.Read():
			module_logger.debug('X %s %s',xml.NodeType(),xml.Name())

			if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Flight':
				module_logger.debug('Flight %s %s',xml.NodeType(),xml.Name())

				carrier={
					'carrier_code' : None,
					'carrier_flightnumber' : None,
					'carrier_flight_id' : None,
					'carrier_codeshare_type' : 0
				}

				# transid is OAG's unique ID - but all codeshares have the same transid
				transid=xml.GetAttribute('TransId')

				oagtype='U'

				if transid[0] == 'S':
					oagtype='S'

				carrier['carrier_code']=xml.GetAttribute('Carrier')
				carrier['carrier_flightnumber']=xml.GetAttribute('FltNo')

				carrier_uniq=carrier['carrier_code']+'#'+carrier['carrier_flightnumber']

				module_logger.debug("F %s %s %s", transid, carrier, oagtype)

				flight={
					'flight_id' : None,
					'flight_transid' : transid,
					'flight_sent_datetime' : sent,
					'flight_sent_utc_datetime' : sentutc,
					'flight_utcloc' : locutc,
					'flight_oag_type' : oagtype,
					'carriers' : {},
					'legs' : [],
				}

				flight['carriers'][carrier_uniq]=carrier

				# Leg Loop
				while xml.Read():
					module_logger.debug('X %s %s',xml.NodeType(),xml.Name())

					if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Leg':
						module_logger.debug('Leg %s %s',xml.NodeType(),xml.Name())

						_process_leg(xml,flight)

					if xml.NodeType() == libxml2.XML_READER_TYPE_END_ELEMENT and xml.Name() == 'Flight':
						break
				# end Leg Loop

				if transid in flights:
					# if transid exists we have seen this flight before

					# add code share only - discard duplicated data
					for c in flight['carriers']:
						if c in flights[transid]['carriers']:
							# only if codeshare is set copy the value
							if flight['carriers'][c]['carrier_codeshare_type'] is not None:
								flights[transid]['carriers'][c]['carrier_codeshare_type']=flight['carriers'][c]['carrier_codeshare_type']
						else:
							flights[transid]['carriers'][c]=flight['carriers'][c]
				else:
					# new flight
					flights[transid]=flight

				module_logger.debug(flights[transid])

		#end Flight Loop

	# end if FIMSSR

	# insert flights into DB

	for transid in flights:

		try:
			module_logger.debug("Insert %s", transid)

			csr=dbh.cursor()

			# use named bind parameters
			csr.execute( _sql['oag_flight'],
				{
					'flight_transid' : flights[transid]['flight_transid'],
					'flight_sent_datetime' : flights[transid]['flight_sent_datetime'],
					'flight_sent_utc_datetime' : flights[transid]['flight_sent_utc_datetime'],
					'flight_utcloc' : flights[transid]['flight_utcloc'],
					'flight_oag_type' : flights[transid]['flight_oag_type'],
				}
			)

			module_logger.debug("Q %s", csr.query)
			module_logger.debug("R %s", csr.statusmessage)

			# get flight_id via the "returning" clause
			flight_id = csr.fetchone()[0]

			module_logger.debug("Flight ID %s", flight_id)

			if flight_id:
				for c in flights[transid]['carriers']:
					carrier=flights[transid]['carriers'][c]

					module_logger.debug("Carrier %s %s %s", c, carrier['carrier_code'], carrier['carrier_flightnumber'])

					carrier['carrier_flight_id']=flight_id

					# we can use a dictionary as named bind parameter
					csr.execute( _sql['oag_carrier'], carrier )

					module_logger.debug("Q %s", csr.query)
					module_logger.debug("R %s", csr.statusmessage)

				for leg in flights[transid]['legs']:
					module_logger.debug("Leg")

					leg['leg_flight_id']=flight_id

					# we can use a dictionary as named bind parameter
					csr.execute( _sql['oag_leg'], leg )

					module_logger.debug("Q %s", csr.query)
					module_logger.debug("R %s", csr.statusmessage)

				csr.execute( _sql['history'],  { 'lastupdated' : sentutc } )

				module_logger.debug("Q %s", csr.query)
				module_logger.debug("R %s", csr.statusmessage)


			csr.close()

			module_logger.debug("Commit")

			dbh.commit()

		except:
			module_logger.exception("Rollback")

			csr.close()

			dbh.rollback()

			status=-2
			message="Database error"

	#end for

	return status, message

# end def _process_file



# process the leg tag and build up leg record
def _process_leg(xml,flight):
	leg={
		'leg_flight_id' : None,
		'leg_service_type': None,

		'leg_arr_claim_act' : None,
		'leg_arr_claim_ch' : None,
		'leg_arr_datetime_act_datetime' : None,
		'leg_arr_datetime_ch' : None,
		'leg_arr_datetime_est_datetime' : None,
		'leg_arr_datetime_schd_datetime' : None,
		'leg_arr_delay_catid' : None,
		'leg_arr_delay_det' : None,
		'leg_arr_delay_stat' : None,
		'leg_arr_divert_apt' : None,
		'leg_arr_divert_city' : None,
		'leg_arr_down_act_datetime' : None,
		'leg_arr_down_ch' : None,
		'leg_arr_down_est_datetime' : None,
		'leg_arr_gate_act' : None,
		'leg_arr_gate_ch' : None,
		'leg_arr_onblock_act_datetime' : None,
		'leg_arr_onblock_ch' : None,
		'leg_arr_onblock_est_datetime' : None,
		'leg_arr_trm_act' : None,
		'leg_arr_trm_ch' : None,
		'leg_arr_trm_schd' : None,
		'leg_arr_apt' : None,
		'leg_arr_city' : None,

		'leg_dep_airborne_act_datetime' : None,
		'leg_dep_airborne_ch' : None,
		'leg_dep_airborne_est_datetime' : None,
		'leg_dep_checkin_act' : None,
		'leg_dep_checkin_ch' : None,
		'leg_dep_datetime_act_datetime' : None,
		'leg_dep_datetime_ch' : None,
		'leg_dep_datetime_est_datetime' : None,
		'leg_dep_datetime_schd_datetime' : None,
		'leg_dep_delay_catid' : None,
		'leg_dep_delay_det' : None,
		'leg_dep_delay_stat' : None,
		'leg_dep_gate_act' : None,
		'leg_dep_gate_ch' : None,
		'leg_dep_offblock_act_datetime' : None,
		'leg_dep_offblock_ch' : None,
		'leg_dep_offblock_est_datetime' : None,
		'leg_dep_trm_act' : None,
		'leg_dep_trm_ch' : None,
		'leg_dep_trm_schd' : None,
		'leg_dep_apt' : None,
		'leg_dep_city' : None,

		'leg_aircraft_reg_act' : None,
		'leg_aircraft_reg_ch' : None,
		'leg_aircraft_act' : None,
		'leg_aircraft_ch' : None,
		'leg_aircraft_schd' : None,
	}

	leg['leg_service_type']=xml.GetAttribute('SvcTypeCd')

	while xml.Read():
		module_logger.debug('X %s %s',xml.NodeType(),xml.Name())

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Equip':
			_process_leg_equip(xml,leg)
		elif xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Dep':
			_process_leg_dep(xml,leg)
		elif xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Arr':
			_process_leg_arr(xml,leg)
		elif xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'CodeShare':
			_process_leg_codeshare(xml,flight)

		if xml.NodeType() == libxml2.XML_READER_TYPE_END_ELEMENT and xml.Name() == 'Leg':
			break

	flight['legs'].append(leg)

# end def _process_leg

def _process_leg_equip(xml,leg):
	module_logger.debug('Equip %s %s',xml.NodeType(),xml.Name())

	leg['leg_aircraft_schd']=xml.GetAttribute('Schd')
	leg['leg_aircraft_ch']=xml.GetAttribute('Ch')
	leg['leg_aircraft_act']=xml.GetAttribute('Act')

	if not xml.IsEmptyElement():
		while xml.Read():
			module_logger.debug('X %s %s',xml.NodeType(),xml.Name())

			if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Reg':
				module_logger.debug('Reg %s %s',xml.NodeType(),xml.Name())

				leg['leg_aircraft_reg_ch']=xml.GetAttribute('Ch')
				leg['leg_aircraft_reg_act']=xml.GetAttribute('Act')
				break

			if xml.NodeType() == libxml2.XML_READER_TYPE_END_ELEMENT and xml.Name() == 'Equip':
				break

# end def _process_leg_equip

def _process_leg_codeshare(xml,flight):
	module_logger.debug('CodeShare %s %s',xml.NodeType(),xml.Name())

	carrier_codeshare_type=xml.GetAttribute('Type')

	module_logger.debug("CS Type %s", carrier_codeshare_type)

	if carrier_codeshare_type == '1':
		carrier_code=xml.GetAttribute('Desig')
		carrier_flightnumber=xml.GetAttribute('FltNo')

		carrier_uniq=carrier_code+'#'+carrier_flightnumber

		module_logger.debug("CS U %s",carrier_uniq)

		# flag all carriers that don't match the metal carrier
		# however only 1 carrier should be in the flight so we could flag them all
		for c in flight['carriers']:
			module_logger.debug("CS C %s", c)
			if c != carrier_uniq:
				flight['carriers'][c]['carrier_codeshare_type']=1 # code share

		if not carrier_uniq in flight['carriers']:
			carrier={
				'carrier_code' : carrier_code,
				'carrier_flightnumber' : carrier_flightnumber,
				'carrier_flight_id' : None,
				'carrier_codeshare_type' : None # metal for info only
			}
			flight['carriers'][carrier_uniq]=carrier

# end def _process_leg_codeshare

def _process_leg_dep(xml, leg):
	module_logger.debug('Dep %s %s',xml.NodeType(),xml.Name())

	leg['leg_dep_apt']=xml.GetAttribute('Apt')
	leg['leg_dep_city']=xml.GetAttribute('City')

	while xml.Read():
		module_logger.debug('X %s %s',xml.NodeType(),xml.Name())

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Trm':
			module_logger.debug('Trm %s %s',xml.NodeType(),xml.Name())

			leg['leg_dep_trm_ch']=xml.GetAttribute('Ch')
			leg['leg_dep_trm_act']=xml.GetAttribute('Act')
			leg['leg_dep_trm_schd']=xml.GetAttribute('Schd')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'CheckIn':
			module_logger.debug('CheckIn %s %s',xml.NodeType(),xml.Name())

			leg['leg_dep_checkin_ch']=xml.GetAttribute('Ch')
			leg['leg_dep_checkin_act']=xml.GetAttribute('Act')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Gate':
			module_logger.debug('Gate %s %s',xml.NodeType(),xml.Name())

			leg['leg_dep_gate_ch']=xml.GetAttribute('Ch')
			leg['leg_dep_gate_act']=xml.GetAttribute('Act')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'DateTime':
			module_logger.debug('DateTime %s %s',xml.NodeType(),xml.Name())

			leg['leg_dep_datetime_ch']=xml.GetAttribute('Ch')
			leg['leg_dep_datetime_act_datetime']=xml.GetAttribute('Act')
			leg['leg_dep_datetime_schd_datetime']=xml.GetAttribute('Schd')
			leg['leg_dep_datetime_est_datetime']=xml.GetAttribute('Est')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'OffBlock':
			module_logger.debug('OffBlock %s %s',xml.NodeType(),xml.Name())

			leg['leg_dep_offblock_ch']=xml.GetAttribute('Ch')
			leg['leg_dep_offblock_act_datetime']=xml.GetAttribute('Act')
			leg['leg_dep_offblock_est_datetime']=xml.GetAttribute('Est')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Airborne':
			module_logger.debug('Airborne %s %s',xml.NodeType(),xml.Name())

			leg['leg_dep_airborne_ch']=xml.GetAttribute('Ch')
			leg['leg_dep_airborne_act_datetime']=xml.GetAttribute('Act')
			leg['leg_dep_airborne_est_datetime']=xml.GetAttribute('Est')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Delay':
			module_logger.debug('Delay %s %s',xml.NodeType(),xml.Name())

			leg['leg_dep_delay_catid']=xml.GetAttribute('CatId')
			leg['leg_dep_delay_det']=xml.GetAttribute('Det')
			leg['leg_dep_delay_stat']=xml.GetAttribute('Stat')

		if xml.NodeType() == libxml2.XML_READER_TYPE_END_ELEMENT and xml.Name() == 'Dep':
			break

# end def _process_leg_dep

def _process_leg_arr(xml, leg):
	module_logger.debug('Arr %s %s',xml.NodeType(),xml.Name())

	leg['leg_arr_apt']=xml.GetAttribute('Apt')
	leg['leg_arr_city']=xml.GetAttribute('City')

	while xml.Read():
		module_logger.debug('X %s %s',xml.NodeType(),xml.Name())

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Trm':
			module_logger.debug('Trm %s %s',xml.NodeType(),xml.Name())

			leg['leg_arr_trm_ch']=xml.GetAttribute('Ch')
			leg['leg_arr_trm_act']=xml.GetAttribute('Act')
			leg['leg_arr_trm_schd']=xml.GetAttribute('Schd')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Claim':
			module_logger.debug('Claim %s %s',xml.NodeType(),xml.Name())

			leg['leg_arr_claim_ch']=xml.GetAttribute('Ch')
			leg['leg_arr_claim_act']=xml.GetAttribute('Act')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Gate':
			module_logger.debug('Gate %s %s',xml.NodeType(),xml.Name())

			leg['leg_arr_gate_ch']=xml.GetAttribute('Ch')
			leg['leg_arr_gate_act']=xml.GetAttribute('Act')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'DateTime':
			module_logger.debug('DateTime %s %s',xml.NodeType(),xml.Name())

			leg['leg_arr_datetime_ch']=xml.GetAttribute('Ch')
			leg['leg_arr_datetime_act_datetime']=xml.GetAttribute('Act')
			leg['leg_arr_datetime_schd_datetime']=xml.GetAttribute('Schd')
			leg['leg_arr_datetime_est_datetime']=xml.GetAttribute('Est')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'OnBlock':
			module_logger.debug('OnBlock %s %s',xml.NodeType(),xml.Name())

			leg['leg_arr_onblock_ch']=xml.GetAttribute('Ch')
			leg['leg_arr_onblock_act_datetime']=xml.GetAttribute('Act')
			leg['leg_arr_onblock_est_datetime']=xml.GetAttribute('Est')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Down':
			module_logger.debug('Down %s %s',xml.NodeType(),xml.Name())

			leg['leg_arr_down_ch']=xml.GetAttribute('Ch')
			leg['leg_arr_down_act_datetime']=xml.GetAttribute('Act')
			leg['leg_arr_down_est_datetime']=xml.GetAttribute('Est')

		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Delay':
			module_logger.debug('Delay %s %s',xml.NodeType(),xml.Name())

			leg['leg_arr_delay_catid']=xml.GetAttribute('CatId')
			leg['leg_arr_delay_det']=xml.GetAttribute('Det')
			leg['leg_arr_delay_stat']=xml.GetAttribute('Stat')


		if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'Divert':
			module_logger.debug('Divert %s %s',xml.NodeType(),xml.Name())

			leg['leg_arr_divert_apt']=xml.GetAttribute('Apt')
			leg['leg_arr_divert_city']=xml.GetAttribute('City')

		if xml.NodeType() == libxml2.XML_READER_TYPE_END_ELEMENT and xml.Name() == 'Arr':
			break

# end def _process_leg_arr

def _schdupdate(dbh):
	module_logger.info("schd update")

	status=1
	message='OK'

	try:
		module_logger.debug("SCHD update")

		csr=dbh.cursor()

		csr.execute( _sql['schd'] )

		module_logger.debug("Q %s", csr.query)
		module_logger.debug("R %s", csr.statusmessage)

		csr.close()

		module_logger.debug("Commit")

		dbh.commit()

	except:
		module_logger.exception("Rollback")

		csr.close()

		dbh.rollback()

		status=-2
		message="Error updating schd table"

	return status, message
# end def _schdupdate

def _xmlerror(ctx, str):
	module_logger.error(str)

	raise Exception(str)
# end def _xmlerror

def validate_file(f , renamebad=True, baddir=None):
	#logger=logging.getLogger()

	if baddir is not None:
		if not os.path.exists(baddir):
			os.makedirs(baddir)

	module_logger.info("Validating %s", f)

	ok=True

	with open(f, 'rb') as file:
		x = file.read()

		match=re.search('<FIMSSR(.*?)>(.*?)</FIMSSR>', x, re.MULTILINE|re.DOTALL)

		if match is None:
			ok=False

	if not ok:
		file_bad=None
		if renamebad:
			file_bad=f+'.bad'
			os.rename(f,file_bad)
		else:
			file_bad=os.path.join(baddir, os.path.basename(f))
			os.rename(f,file_bad)
		module_logger.info("Moved Bad File %s to %s", f, file_bad)

	return ok
#end def validate_file

def importfile(dbh, filename, batch=False):
	module_logger.info("Importing %s Batch %s", filename, batch)

	status=1
	message='OK'

	if os.path.isfile(filename) and os.path.getsize(filename) > 0:
		try:
			module_logger.debug("Parsing")

			xml=libxml2.newTextReaderFilename(filename)
			libxml2.registerErrorHandler(_xmlerror, "")

			while xml.Read():
				module_logger.debug('X %s %s',xml.NodeType(),xml.Name())

				if xml.NodeType() == libxml2.XML_READER_TYPE_ELEMENT and xml.Name() == 'FIMSSR':
					module_logger.debug("Found FIMSSR %s %s",xml.NodeType(),xml.Name())
					status, message=_process_file(dbh,xml)

					if status != 1:
						module_logger.debug("Status %s bailing", status)
						break
		except:
			module_logger.exception("Parse error %s", filename)
			status=-2
			message="XML Parser Error on file "+filename
		finally:
			module_logger.debug("Cleanup")
			libxml2.cleanupParser()
	else:
		module_logger.error("Zero size file %s", filename)
		status=-2
		message="Zero size or truncated file "+filename

	if status == 1:
		try:
			csr=dbh.cursor();

			module_logger.debug("Store filename %s", os.path.basename(filename))

			csr.execute( _sql['file_update'], { 'filename': os.path.basename(filename) } )

			csr.close()

			module_logger.debug("Commit")

			dbh.commit()

		except:
			module_logger.warning("Rollback")

			csr.close()

			dbh.rollback()

			status=-1
			message="DB error"

	# if not in batch the SCHD update after file
	if status == 1 and not batch:
		module_logger.debug("Not Batch mode, updating schd after file %s", filename)
		status, message=_schdupdate(dbh)

	return status, message
#end def importfile

def _sortkey(filename):
	match=re.search('^(\d+)_(SH)?(\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)(.*?)$', filename, re.I)

	key=''

	if match is not None:
		key=match.group(3) + match.group(4) + match.group(5) + match.group(6) + match.group(7) + match.group(8)
	return key
#end def _sortkey

def import_folder(dbh, sourcedir, archivedir=None):
	module_logger.info("Import Folder %s %s", sourcedir, archivedir)
	status=1
	message='OK'

	archivemode=False

	if archivedir is not None and archivedir != sourcedir:
		archivemode=True

		if not os.path.exists(archivedir):
			os.makedirs(archivedir)

	csr=dbh.cursor()

	filecount=0

	if os.path.isdir(sourcedir):
		filelist=sorted(os.listdir(sourcedir), key = _sortkey)

		if filelist is not None:
			for filename in filelist:
				file_done=os.path.join(sourcedir, filename)

				if os.path.isfile(file_done):
					# is it a .done file?

					match=re.search('^((\d+)_(SH)?(\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)(.*?)\.xml)\.done$', filename, re.I)

					if match is not None:

						# extract corresponding csv name & check it

						file_xml=match.group(1)
						file_xml_source=os.path.join(sourcedir, file_xml)

						if os.path.isfile(file_xml_source) and validate_file(file_xml_source, False, archivedir):
							csr.execute(_sql['file_check'], { 'filename': file_xml } )

							# if the filename isn't found
							if csr.rowcount == 0:
								module_logger.debug("File found %s",file_xml_source)

								module_logger.info("Importing %s", file_xml_source)
								status,message=importfile(dbh, file_xml_source, True)

								if status != 1:
									module_logger.debug("Status %s bailing", status)
									break

								filecount+=1

								# only archive if status is good & archivedir
								if archivemode:
									file_xml_archive=os.path.join(archivedir, file_xml)
									os.rename(file_xml_source,file_xml_archive)
									module_logger.debug("Archived %s", file_xml_archive)

						# remove the .done file
						os.unlink(file_done)
			#end for

			if filecount == 0:
				module_logger.error("No files imported")
				message="No files imported"
			else:
				module_logger.info("%s files imported", filecount)

				# batch update SCHD
				if status == 1:
					status,message=_schdupdate(dbh)

				# if still ok, use old message
				if status == 1:
					message="%s files imported" % filecount
		else:
			module_logger.error("No files found")
			message='No files found to import'
	else:
		status=-2
		module_logger.error("Source Folder not found %s", sourcedir)
		message="Source Folder not found "+sourcedir

	csr.close()

	return status, message

# end def findfiles
