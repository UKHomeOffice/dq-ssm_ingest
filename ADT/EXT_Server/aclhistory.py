#!/usr/bin/python

import gdbm

aclhistory=gdbm.open('aclhistory.db','c')

for f in aclhistory.keys():
	print "File", f, "State", aclhistory[f]
