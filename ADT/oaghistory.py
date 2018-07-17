#!/usr/bin/python

import gdbm
import sys
import os

db_filename = "oaghistory.db"

if len(sys.argv) != 1:
	scriptname = os.path.basename(str(sys.argv[0]))
	print "usage: " + scriptname
	print "\t to display the files in", db_filename
	print "\t note: files will not be sorted"
	os._exit(1)

db_file=gdbm.open(db_filename,'c')

for f in db_file.keys():
	print "File", f, "State", db_file[f]
