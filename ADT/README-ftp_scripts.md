# SSM FTP Scripts

In Sungard, the SSM FTP scripts were run on two servers:

- *LFTP01* - Externally facing in the DMZ, routing via the server `WEB02`
- *EXT01* - Internally facing

SSM files were collected from the third party servers onto `LFTP01` then transferred internally to `EXT01`.

Hence, there are four scripts involved in downloading SSM data:

1. *ACL - External* - `ftp_acl_web02.py`
2. *ACL - Internal* - `sftp_acl_ext01.py`
3. *OAG - External* - `sftp_oag_prod_maytech1.py`
4. *OAG - Internal* - `sftp_oag_ext01.py`

In AWS, the SSM FTP scripts run on a single server:

- `Linux_Data_Ingest`

Hence, there are only two scripts involved in downloading SSM data:

1. *ACL* - `ftp_acl_web02.py`
2. *OAG* - `sftp_oag_client_maytech.py`
