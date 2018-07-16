#!/usr/bin/python

import gdbm

aclhistory=gdbm.open('aclhistory.db','c')

for f in aclhistory.keys():
	if f == 'HOMEOFFICEROLL3_20180423.CSV':
		print "Updating the key", f
		aclhistory[f] = "D"
	print "File", f, "State", aclhistory[f]
