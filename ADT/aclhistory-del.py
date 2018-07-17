#!/usr/bin/python

import gdbm
import sys
import os

db_filename = "aclhistory.db"
example_filename = "HOMEOFFICEROLL3_20180521.CSV"

if len(sys.argv) != 2:
	scriptname = os.path.basename(str(sys.argv[0]))
	print "usage:", scriptname, "<FILENAME>"
	print "\t Pass in the filename to be removed from the .db file(" + db_filename + ")"
	print "\t Example: ", scriptname, example_filename
	print "\t to delete file", example_filename, "from", db_filename
	os._exit(1)

file_to_remove = str(sys.argv[1])

db_file = gdbm.open(db_filename,'c')

for f in db_file.keys():
	if f == file_to_remove:
		print "Deleting the key", f
		del db_file[f]
