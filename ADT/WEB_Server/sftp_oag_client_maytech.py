#!/usr/bin/python

# FTP OAG Script
# Version 2 - maytech copy
# ben.baylis@flightregister.net
# ben@velvetbug.com
# Flight Register/Velvet Bug Ltd

# we only need the datetime class & the static function strptime from datetime module

from datetime import datetime
import dateutil.parser
import re
import time
import sys
import os
import argparse
import logging
import gdbm
import subprocess
import paramiko


ssh_remote_host='<see document for details: /Dropbox/Aker Systems (Home Office)/DQ Transition Project/notes>'
ssh_remote_user='<see document for details: /Dropbox/Aker Systems (Home Office)/DQ Transition Project/notes>'
ssh_private_key='<see document for details: /Dropbox/Aker Systems (Home Office)/DQ Transition Project/notes>'
ssh_landing_dir='/'
download_dir='/ADT/data/oag'
staging_dir='/ADT/stage/oag'
archive_dir='/ADT/archive/oag'
quarantine_dir='/ADT/quarantine/oag'
vscanexe='/usr/bin/clamdscan'
vscanopt=''

def ssh_login(in_host, in_user, in_keyfile):
        logger=logging.getLogger()
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy()) ## This line can be removed when the host is added to the known_hosts file
        privkey = paramiko.RSAKey.from_private_key_file (in_keyfile)
        try:
                ssh.connect(in_host, username=in_user,pkey=privkey )
        except Exception, e:
                logger.exception('SSH CONNECT ERROR')
                os._exit(1)
        return ssh


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
	parser = argparse.ArgumentParser(description='OAG SFTP Downloader')
	parser.add_argument('-D','--DEBUG',  default=False, action='store_true', help='Debug mode logging')
	args = parser.parse_args()
	if args.DEBUG:
		logging.basicConfig(
			filename='/ADT/scripts/sftp_oag_maytech.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.DEBUG
		)
	else:
		logging.basicConfig(
			filename='/ADT/scripts/sftp_oag_maytech.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.INFO
		)


	logger=logging.getLogger()
	logger.info("Starting")
	status=1

	# Main
	os.chdir('/ADT/scripts')
	oaghistory=gdbm.open('/ADT/scripts/oaghistory.db','c')
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
        logger.debug("Connecting via SSH")
        ssh=ssh_login(ssh_remote_host, ssh_remote_user, ssh_private_key)
        logger.debug("Connected")
        sftp = ssh.open_sftp()

	try:
		sftp.chdir(ssh_landing_dir)
		files=sftp.listdir()
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
				if os.path.isfile(slf) and os.path.getsize(slf) > 0 and os.path.getsize(slf) == sftp.stat(f).st_size:
					logger.info("File exists")
					download=False
					oaghistory[f]='R' # ready
					logger.debug("purge %s", f)
					sftp.remove(f)
				if download:
					logger.info("Downloading %s to %s", f, slf)
					sftp.get(f, slf) # remote, local
					if os.path.isfile(slf) and os.path.getsize(slf) > 0 and os.path.getsize(slf) == sftp.stat(f).st_size:
						logger.debug("purge %s", f)
						sftp.remove(f)

		# end for
	except:
		logger.exception("Failure")
		status=-2
	# end with

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

	oaghistory.close()
	logger.info("Downloaded %s files", downloadcount)

	if downloadcount == 0:
		status=-1

	logger.info("Done Status %s", status)
	print status

# end def main

if __name__ == '__main__':
    main()
