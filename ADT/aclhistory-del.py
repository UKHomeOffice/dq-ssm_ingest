#!/usr/bin/python

import gdbm

aclhistory=gdbm.open('aclhistory.db','c')

for f in aclhistory.keys():
	if f == 'HOMEOFFICEROLL3_20180521.CSV':
		print "Deleting the key", f
		del aclhistory[f]
