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
server = os.environ['server']
username = os.environ['username']
password = os.environ['password']

download_dir = '/ADT/data/acl'
staging_dir = '/ADT/stage/acl'
quarantine_dir = '/ADT/quarantine/acl'
archive_dir = '/ADT/archive/acl'

vscanexe = '/usr/bin/clamdscan'
vscanopt = ''

def run_virus_scan(vscanexe, option, filename):
	logger = logging.getLogger()
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
	parser.add_argument('-D','--DEBUG', default = False, action='store_true', help='Debug mode logging')

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

	logger = logging.getLogger()

	logger.info("Starting")

	status = 1

	ProxySock.setup_http_proxy(os.environ['proxy'], 3128)

	aclhistory = gdbm.open('aclhistory.db','c')

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
	for file_done in os.listdir(download_dir):
		logger.debug("File %s", file_done)

		match = re.search('^((.*?)homeofficeroll(\d+)_(\d{4}\d{2}\d{2})\.csv)\.done$', file_done, re.I)

		if match is not None:
			file_csv = match.group(1)

			logger.info("File %s has been downloaded %s file found", file_csv, file_done)

			nf = os.path.join(archive_dir, file_csv)

			file_csv_download = os.path.join(download_dir, file_csv)
			file_done_download = os.path.join(download_dir, file_done)

			os.rename(file_csv_download,nf)

			logger.info("Archived %s", file_csv)

			os.unlink(file_done_download)

			aclhistory[file_csv]='D' # downloaded

	downloadcount = 0

	with ftputil.FTPHost(server, username, password) as ftp_host:

		try:
			ftp_host.chdir('3_Days')

			files = ftp_host.listdir(ftp_host.curdir)
			for file_csv in files:

				match = re.search('^(.*?)homeofficeroll(\d+)_(\d{4}\d{2}\d{2})\.csv$', file_csv, re.I)

				download = False

				if match is not None:
					if file_csv not in aclhistory.keys():
						aclhistory[file_csv] = 'N' # new

					if aclhistory[file_csv] == 'N':
						download = True
					else:
						logger.debug("Skipping %s", file_csv)
						continue

					file_csv_download = os.path.join(download_dir,file_csv)
					file_csv_staging = os.path.join(staging_dir,file_csv)

					#protection against redownload
					if os.path.isfile(file_csv_download) and os.path.getsize(file_csv_download) > 0 and os.path.getsize(file_csv_download) == ftp_host.stat(file_csv).st_size:
						logger.info("File exists")
						download = False
						aclhistory[file_csv]='R' # ready

					if download:
						logger.info("Downloading %s to %s", file_csv, file_csv_download)

						ftp_host.download(file_csv, file_csv_staging) # remote, local (staging)

						logger.debug("downloaded %s to %s", file_csv, file_csv_staging)

						if os.path.isfile(file_csv_staging) and os.path.getsize(file_csv_staging) > 0 and os.path.getsize(file_csv_download) == ftp_host.stat(file_csv).st_size:
							logger.debug("before virus scan")
							if run_virus_scan(vscanexe, vscanopt, file_csv_staging):
								aclhistory[file_csv]='R' # ready

								# move from staging to live
								os.rename(file_csv_staging,file_csv_download)

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
