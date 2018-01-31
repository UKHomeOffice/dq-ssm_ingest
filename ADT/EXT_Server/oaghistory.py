#!/usr/bin/python

import gdbm

oaghistory=gdbm.open('oaghistory.db','c')

for f in oaghistory.keys():
	print "File", f, "State", oaghistory[f]
