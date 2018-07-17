#!/usr/bin/python

import gdbm
import sys
import os

db_filename = "oaghistory.db"
example_filename = "1124_2018_05_23_"
example_status = "D"

if len(sys.argv) != 3:
	scriptname = os.path.basename(str(sys.argv[0]))
	print "usage:", scriptname, "<FILENAME>", "<STATUS>"
	print "\t Pass in the begining of the filenames and status to be set in the .db file(" + db_filename + ")"
	print "\t Example: ", scriptname, example_filename, example_status
	print "\t to set files 1124_2018_05_23_09_35_50.xml,  124_2018_05_23_09_36_10.xml,  124_2018_05_23_09_36_19.xml, etc", "=", example_status, "in", db_filename
	os._exit(1)

file_to_set = str(sys.argv[1])
status_to_set = str(sys.argv[2])

db_file = gdbm.open(db_filename,'c')

for f in db_file.keys():
	if f == file_to_set:
		print "Updating the key", f
		db_file[f] = status_to_set
	print "File", f, "State", db_file[f]
