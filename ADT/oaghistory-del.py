#!/usr/bin/python

import gdbm
import sys
import os

db_filename = "oaghistory.db"
example_filename = "1124_2018_05_23_"

if len(sys.argv) != 2:
	scriptname = os.path.basename(str(sys.argv[0]))
	print "usage:", scriptname, "<FILENAME>"
	print "\t Pass in the begining of the filenames to be removed from the .db file (" + db_filename + ")"
	print "\t Example: ", scriptname, example_filename
	print "\t to delete files: 1124_2018_05_23_09_35_50.xml,  124_2018_05_23_09_36_10.xml,  124_2018_05_23_09_36_19.xml, etc from", db_filename
	os._exit(1)

file_to_remove = str(sys.argv[1])

db_file = gdbm.open(db_filename,'c')

for f in db_file.keys():
	if file_to_remove in f:
                print "Deleting the key", f
                del db_file[f]
