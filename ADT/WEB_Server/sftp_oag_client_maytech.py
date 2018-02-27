#!/usr/bin/python

# FTP OAG Script
# Version 2 - maytech copy

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


ssh_remote_host_maytech = os.environ['MAYTECH_HOST']
ssh_remote_user_maytech = os.environ['MAYTECH_USER']
ssh_private_key = os.environ['MAYTECH_OAG_PRIVATE_KEY_PATH']
ssh_landing_dir = os.environ['MAYTECH_OAG_LANDING_DIR']
download_dir = '/ADT/data/oag'
staging_dir = '/ADT/stage/oag'
archive_dir = '/ADT/archive/oag'
quarantine_dir = '/ADT/quarantine/oag'
vscanexe = '/usr/bin/clamdscan'
vscanopt = ''

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

    logger = logging.getLogger()
    logger.info("Starting")
    status = 1

    # Main
    os.chdir('/ADT/scripts')
    oaghistory = gdbm.open('/ADT/scripts/oaghistory.db','c')
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
    for file_done in os.listdir(download_dir):
        logger.debug("File %s", file_done)
        match = re.search('^(1124_(SH)?(\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)(.*?)\.xml)\.done$', file_done, re.I)

        if match is not None:
            file_xml = match.group(1)
            logger.info("File %s has been downloaded %s file found", file_xml, file_done)
            file_xml_archive = os.path.join(archive_dir, file_xml)
            file_xml_download = os.path.join(download_dir, file_xml)
            file_done_download = os.path.join(download_dir, file_done)
            os.rename(file_xml_download,file_xml_archive)
            logger.info("Archived %s", file_xml)
            os.unlink(file_done_download)
            oaghistory[file_xml]='D' # downloaded

    downloadcount = 0
    logger.debug("Connecting via SSH")
    ssh = ssh_login(ssh_remote_host_maytech, ssh_remote_user_maytech, ssh_private_key)
    logger.debug("Connected")
    sftp = ssh.open_sftp()

    try:
        sftp.chdir(ssh_landing_dir)
        files = sftp.listdir()
        for file_xml in files:
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
        file_xml_staging = os.path.join(staging_dir,file_xml)

        #protection against redownload
        if os.path.isfile(file_xml_staging) and os.path.getsize(file_xml_staging) > 0 and os.path.getsize(file_xml_staging) == sftp.stat(file_xml).st_size:
            logger.info("File exists")
            download = False
            oaghistory[file_xml] = 'R' # ready
            logger.debug("purge %s", file_xml)
            sftp.remove(file_xml)
        if download:
            logger.info("Downloading %s to %s", file_xml, file_xml_staging)
            sftp.get(file_xml, file_xml_staging) # remote, local
            if os.path.isfile(file_xml_staging) and os.path.getsize(file_xml_staging) > 0 and os.path.getsize(file_xml_staging) == sftp.stat(file_xml).st_size:
                logger.debug("purge %s", file_xml)
                sftp.remove(file_xml)
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
            file_download = os.path.join(download_dir,f)
            file_staging = os.path.join(staging_dir,f)
            logger.debug("move %s from staging to download %s",file_staging ,file_download)
            os.rename(file_staging,file_download)
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
