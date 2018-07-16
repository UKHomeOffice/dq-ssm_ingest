## SSM FTP Scripts

In Sungard, the SSM FTP scripts were run on two servers:
1. LFTP01
 - Externally facing in the DMZ, routing via the server WEB02
1. EXT01
 - Internally facing

SSM files are FTP'd from the outside world onto LFTP01 then FTP'd to EXT01.

Hence, there are four scripts involved in downloading SSM data:
1. ACL - External
 - `ftp_acl_web02.py`
1. ACL - Internal
 - `sftp_acl_ext01.py`
1. OAG - External
 - `sftp_oag_prod_maytech1.py`
1. OAG - Internal
 - `sftp_oag_ext01.py`

In AWS, the SSM FTP scripts run on a single server:
1. Linux_Data_Ingest

Hence, there are only two scripts involved in downloading SSM data:
1. ACL
 - `ftp_acl_web02.py`
1. OAG
 - `sftp_oag_client_maytech.py`
