#!/usr/bin/python

# FTP OAG Script
# Version 1

# we only need the datetime class & the static function strptime from datetime module
from datetime import datetime
import dateutil.parser
import re
import time
import sys
import os
import ftputil
# argparse is std lib in 2.7 - install it for 2.6 "yum install python-argparse"
import argparse
import logging
import gdbm
import subprocess

# local module
import ProxySock

log_file='/ADT/scripts/ftp_oag.log'

# OAG FTP details - purge downloaded files
server=os.environ['server']
username=os.environ['username']
password=os.environ['password']

#download_dir=time.strftime('%Y%m%d%H%I%S')
download_dir='/ADT/data/oag'
staging_dir='/ADT/stage/oag'
archive_dir='/ADT/archive/oag'
quarantine_dir='/ADT/quarantine/oag'

vscanexe='/usr/bin/clamdscan'
vscanopt=''

def run_virus_scan(vscanexe, option, filename):
	logger=logging.getLogger()
	logger.debug("Virus Scanning %s", filename)

	# do quarantine move using via the virus scanner
	virus_scan_return_code = subprocess.call([vscanexe,'--quiet','--move='+quarantine_dir, option, filename])

	logger.debug("Virus scan result %s", virus_scan_return_code)

	if virus_scan_return_code != 0: # Exit script if virus scan exe fails
		logger.error('VIRUS SCAN FAILED %s', filename)
		return False
	else:
		logger.debug('Virus scan OK')

	return True
# end def run_virus_scan

def main():
	parser = argparse.ArgumentParser(description='OAG FTP Downloader')
	parser.add_argument('-D','--DEBUG',  default=False, action='store_true', help='Debug mode logging')

	args = parser.parse_args()

	if args.DEBUG:
		logging.basicConfig(
			filename='/ADT/scripts/ftp_oag.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.DEBUG
		)
	else:
		logging.basicConfig(
			filename='/ADT/scripts/ftp_oag.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.INFO
		)


	logger=logging.getLogger()

	logger.info("Starting")

	status=1

	# Main

	ProxySock.setup_http_proxy(os.environ['proxy'], 3128)

	oaghistory=gdbm.open('oaghistory.db','c')

	if not os.path.exists(download_dir):
		os.makedirs(download_dir)

	if not os.path.exists(archive_dir):
		os.makedirs(archive_dir)

	if not os.path.exists(staging_dir):
		os.makedirs(staging_dir)

	if not os.path.exists(quarantine_dir):
		os.makedirs(quarantine_dir)

	# process download folder for downloaded files and move to archive folder
	logger.debug("Scanning download folder %s", download_dir)
	for f in os.listdir(download_dir):
		logger.debug("File %s", f)

		match = re.search('^(1124_(SH)?(\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)(.*?)\.xml)\.done$', f, re.I)

		if match is not None:
			filename=match.group(1)

			logger.info("File %s has been downloaded %s file found", filename, f)

			nf=os.path.join(archive_dir, filename)

			lf=os.path.join(download_dir, filename)
			lfd=os.path.join(download_dir, f)

			os.rename(lf,nf)

			logger.info("Archived %s", filename)

			os.unlink(lfd)

			oaghistory[filename]='D' # downloaded

	downloadcount=0

	with ftputil.FTPHost(server, username, password) as ftp_host:

		try:
			files=ftp_host.listdir(ftp_host.curdir)
			for f in files:
				match = re.search('^1124_(SH)?(\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)(.*?)\.xml$', f, re.I)

				download=False

				if match is not None:
					if f not in oaghistory.keys():
						oaghistory[f]='N' # new

					if oaghistory[f] == 'N':
						download=True
					else:
						logger.debug("Skipping %s", f)
						continue

					lf=os.path.join(download_dir,f)
					slf=os.path.join(staging_dir,f)

					#protection against redownload
					if os.path.isfile(lf) and os.path.getsize(lf) > 0 and os.path.getsize(lf) == ftp_host.stat(f).st_size:
						logger.info("File exists")

						download=False

						oaghistory[f]='R' # ready

						logger.debug("purge %s", f)
						ftp_host.remove(f)

					if download:
						logger.info("Downloading %s to %s", f, lf)

						ftp_host.download(f, slf) # remote, local

						if os.path.isfile(slf) and os.path.getsize(slf) > 0 and os.path.getsize(slf) == ftp_host.stat(f).st_size:
							logger.debug("purge %s", f)
							ftp_host.remove(f)
			# end for

			# batch virus scan on staging_dir for OAG
			logger.debug("before virus scan")
			if run_virus_scan(vscanexe, vscanopt, staging_dir):
				for f in os.listdir(staging_dir):
					oaghistory[f]='R'
					lf=os.path.join(download_dir,f)
					sf=os.path.join(staging_dir,f)
					logger.debug("move %s from staging to download %s",sf ,lf)
					os.rename(sf,lf)
					downloadcount+=1
		except:
			logger.exception("Failure")
			status=-2

	# end with

	oaghistory.close()

	logger.info("Downloaded %s files", downloadcount)

	if downloadcount == 0:
		status=-1

	logger.info("Done Status %s", status)

	print status

# end def main

if __name__ == '__main__':
    main()
