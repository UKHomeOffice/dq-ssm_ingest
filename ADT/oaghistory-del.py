#!/usr/bin/python

import gdbm

oaghistory=gdbm.open('oaghistory.db','c')

for f in oaghistory.keys():
	if "1124_2018_05_23_" in f:
                print "Deleting the key", f
                del oaghistory[f]
