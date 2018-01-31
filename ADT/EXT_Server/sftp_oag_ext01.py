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

ssh_remote_host=os.environ['ssh_remote_host']
ssh_remote_user=os.environ['ssh_remote_user']
ssh_private_key=os.environ['ssh_private_key']

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


	logger=logging.getLogger()
	
	logger.info("Starting")
	
	status=1
	downloadcount=0

	# Main
	oaghistory=gdbm.open('oaghistory.db','c')

	if not os.path.exists(download_dir):
		os.makedirs(download_dir)
	
	logger.debug("Connecting via SSH")
	
	ssh=ssh_login(ssh_remote_host, ssh_remote_user, ssh_private_key)
	
	logger.debug("Connected")
	
	sftp = ssh.open_sftp()
	
	try:
		sftp.chdir(download_dir)

		sftp_dir_list = sftp.listdir()

		for f in sftp_dir_list:
			logger.debug("File %s", f)

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

				#protection against redownload
				if os.path.isfile(lf) and os.path.getsize(lf) > 0 and os.path.getsize(lf) == sftp.stat(f).st_size:
					logger.info("File exists")
					download=False
					oaghistory[f]='R' # ready

				if download: # and match.group(3) == '20151005':
					logger.info("Downloading %s to %s", f, lf)

					sftp.get(f, lf) # remote, local

					logger.debug("downloaded %s to %s", f, lf)

					if os.path.isfile(lf) and os.path.getsize(lf) > 0 and os.path.getsize(lf) == sftp.stat(f).st_size:
						logger.debug("before virus scan")
						#if run_virus_scan(vscanexe, vscanopt, lf):
						oaghistory[f]='R' # ready
						downloadcount+=1
						
						lfd=lf+'.done'
						open(lfd,'w').close()
						
						sftp.put(lfd, os.path.basename(lfd)) # local, remote
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
