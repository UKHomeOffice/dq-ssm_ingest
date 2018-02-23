#!/usr/bin/python

# FTP OAG Script
# Version 1
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
import ftputil
# argparse is std lib in 2.7 - install it for 2.6 "yum install python-argparse"
import argparse
import logging
import gdbm
#import subprocess

import paramiko # reqd for SSH/SFTP "yum install python-paramiko"

download_dir='/ADT/data/oag'

ssh_remote_host = os.environ['ssh_remote_host']
ssh_remote_user = os.environ['ssh_remote_user']
ssh_private_key = os.environ['ssh_private_key']

def ssh_login(in_host, in_user, in_keyfile):
	logger = logging.getLogger()

	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy()) ## This line can be removed when the host is added to the known_hosts file
	privkey = paramiko.RSAKey.from_private_key_file (in_keyfile)
	try:
		ssh.connect(in_host, username=in_user,pkey=privkey )
	except Exception, e:
		logger.exception('SSH CONNECT ERROR')
		os._exit(1)

	return ssh
# end def ssh_login

def main():
	YYYYMMDDSTR = time.strftime("%Y%m%d")
	parser = argparse.ArgumentParser(description='OAG SFTP Downloader')
	parser.add_argument('-D','--DEBUG',  default=False, action='store_true', help='Debug mode logging')

	args = parser.parse_args()

	if args.DEBUG:
		logging.basicConfig(
			filename='/ADT/log/sftp_oag_'+YYYYMMDDSTR+'.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.DEBUG
		)
	else:
		logging.basicConfig(
			filename='/ADT/log/sftp_oag_'+YYYYMMDDSTR+'.log',
			format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
			datefmt='%Y-%m-%d %H:%M:%S',
			level=logging.INFO
		)


	logger = logging.getLogger()

	logger.info("Starting")

	status = 1
	downloadcount = 0

	# Main
	oaghistory = gdbm.open('oaghistory.db','c')

	if not os.path.exists(download_dir):
		os.makedirs(download_dir)

	logger.debug("Connecting via SSH")

	ssh = ssh_login(ssh_remote_host, ssh_remote_user, ssh_private_key)

	logger.debug("Connected")

	sftp = ssh.open_sftp()

	try:
		sftp.chdir(download_dir)

		sftp_dir_list = sftp.listdir()

		for file_xml in sftp_dir_list:
			logger.debug("File %s", file_xml)

			match = re.search('^1124_(SH)?(\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)(.*?)\.xml$', file_xml, re.I)

			download = False

			if match is not None:
				if file_xml not in oaghistory.keys():
					oaghistory[file_xml]='N' # new

				if oaghistory[file_xml] == 'N':
					download = True
				else:
					logger.debug("Skipping %s", file_xml)
					continue

				file_xml_download = os.path.join(download_dir,file_xml)

				#protection against redownload
				if os.path.isfile(file_xml_download) and os.path.getsize(file_xml_download) > 0 and os.path.getsize(file_xml_download) == sftp.stat(file_xml).st_size:
					logger.info("File exists")
					download = False
					oaghistory[file_xml]='R' # ready

				if download: # and match.group(3) == '20151005':
					logger.info("Downloading %s to %s", file_xml, file_xml_download)

					sftp.get(file_xml, file_xml_download) # remote, local

					logger.debug("downloaded %s to %s", file_xml, file_xml_download)

					if os.path.isfile(file_xml_download) and os.path.getsize(file_xml_download) > 0 and os.path.getsize(file_xml_download) == sftp.stat(file_xml).st_size:
						logger.debug("before virus scan")
						#if run_virus_scan(vscanexe, vscanopt, file_xml_download):
						oaghistory[file_xml]='R' # ready
						downloadcount+=1

						file_done_download = file_xml_download + '.done'
						open(file_done_download,'w').close()

						sftp.put(file_done_download, os.path.basename(file_done_download)) # local, remote
		# end for
	except:
		logger.exception("Failure")
		status=-2

	oaghistory.close()

	logger.info("Downloaded %s files", downloadcount)

	if downloadcount == 0:
		status=-1

	logger.info("Status %s", status)

	print status
# end def main


if __name__ == '__main__':
    main()
