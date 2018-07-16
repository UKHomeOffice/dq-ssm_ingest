#!/usr/bin/python

import gdbm

oaghistory=gdbm.open('oaghistory.db','c')

for f in oaghistory.keys():
	if "1124_2018_05_23_" in f:
		print "Updating the key", f
		oaghistory[f] = "D"
	print "File", f, "State", oaghistory[f]
