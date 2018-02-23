#!/usr/bin/python

# FTP ACL Script
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


# ACL FTP details - do not purge data
server=os.environ['server']
username=os.environ['username']
password=os.environ['password']

download_dir='/ADT/data/acl'
staging_dir='/ADT/stage/acl'
quarantine_dir='/ADT/quarantine/acl'
archive_dir='/ADT/archive/acl'

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
	parser = argparse.ArgumentParser(description='ACL FTP Downloader')
	parser.add_argument('-D','--DEBUG',  default=False, action='store_true', help='Debug mode logging')

	args = parser.parse_args()

	if args.DEBUG:
		logging.basicConfig(
			filename='/ADT/scripts/ftp_acl.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.DEBUG
		)
	else:
		logging.basicConfig(
			filename='/ADT/scripts/ftp_acl.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.INFO
		)

	logger=logging.getLogger()

	logger.info("Starting")

	status=1

	ProxySock.setup_http_proxy(os.environ['proxy'], 3128)

	aclhistory=gdbm.open('aclhistory.db','c')

	if not os.path.exists(download_dir):
		os.makedirs(download_dir)

	if not os.path.exists(staging_dir):
		os.makedirs(staging_dir)

	if not os.path.exists(quarantine_dir):
		os.makedirs(quarantine_dir)

	if not os.path.exists(archive_dir):
		os.makedirs(archive_dir)

	# process download folder for downloaded files and move to archive folder
	logger.debug("Scanning download folder %s", download_dir)
	for f in os.listdir(download_dir):
		logger.debug("File %s", f)

		match = re.search('^((.*?)homeofficeroll(\d+)_(\d{4}\d{2}\d{2})\.csv)\.done$', f, re.I)

		if match is not None:
			filename=match.group(1)

			logger.info("File %s has been downloaded %s file found", filename, f)

			nf=os.path.join(archive_dir, filename)

			lf=os.path.join(download_dir, filename)
			lfd=os.path.join(download_dir, f)

			os.rename(lf,nf)

			logger.info("Archived %s", filename)

			os.unlink(lfd)

			aclhistory[filename]='D' # downloaded

	downloadcount=0

	with ftputil.FTPHost(server, username, password) as ftp_host:

		try:
			ftp_host.chdir('3_Days')

			files=ftp_host.listdir(ftp_host.curdir)
			for f in files:

				match = re.search('^(.*?)homeofficeroll(\d+)_(\d{4}\d{2}\d{2})\.csv$', f, re.I)

				download=False

				if match is not None:
					if f not in aclhistory.keys():
						aclhistory[f]='N' # new

					if aclhistory[f] == 'N':
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
						aclhistory[f]='R' # ready

					if download:
						logger.info("Downloading %s to %s", f, lf)

						ftp_host.download(f, slf) # remote, local (staging)

						logger.debug("downloaded %s to %s", f, slf)

						if os.path.isfile(slf) and os.path.getsize(slf) > 0  and os.path.getsize(slf) == ftp_host.stat(f).st_size:
							logger.debug("before virus scan")
							if run_virus_scan(vscanexe, vscanopt, slf):
								aclhistory[f]='R' # ready

								# move from staging to live
								os.rename(slf,lf)

								downloadcount+=1
			# end for
		except:
			logger.exception("Failure")
			status=-2

	# end with

	aclhistory.close()

	logger.info("Downloaded %s files", downloadcount)

	if downloadcount == 0:
		status=-1

	logger.info("Done Status %s", status)

	print status

# end def main

if __name__ == '__main__':
    main()
