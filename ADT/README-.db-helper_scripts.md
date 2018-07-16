## .db Helper Scripts

The scripts that download SSM data (`ftp_acl_web02.py` & `sftp_oag_client_maytech.py`) keep track of which files they have downloaded from the FTP server by using a GDBM database.

Hence there are two .db files:
1. aclhistory.db
1. oaghistory.db

These simple database files hold name value pairs. The name is the name of the ACL/OAG file; the value is a letter indicating a status, as understood by our download/import scripts.

There are some simple helper scripts:
 - `aclhistory.py`
  - simply outputs a list of the acl files in the db (ie that have been downloaded). It is best to sort the output, hence run `./aclhistory.py | sort`
 - `aclhistory-edit.py`
  - edit the value. For a given key (filename), set the value (status). The script must be edited to chose the filename and status values.
 - `aclhistory-del.py`
  - delete an entry. For a given key (filename), remove the name/value pair from the database. The result will be that the file is downloaded again (if it is still on the FTP server)

Equivalent scripts exist for `oaghistory.db`.
