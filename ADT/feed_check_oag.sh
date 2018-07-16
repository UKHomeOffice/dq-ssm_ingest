#Nagios NCPA Monitoring Plugin
#OAG/ACL datafeed activity check
#This script will monitor a UNIX directory for recently created files
#Version: 1.0


#Directory to monitor
DIR=/ADT/archive/oag/
DELAY_WRN=40
DELAY_CRT=60
MSG_OK="OK: Transfer is active"
SUBJECT="OAG"
HOST="EXT01"

MSG_WRN="WARNING: $SUBJECT feed on $HOST is down for over $DELAY_WRN minutes"
MSG_CRT="CRITICAL: $SUBJECT feed on $HOST is down for over $DELAY_CRT minutes"
MSG_UKN="UNKNOWN: An unhandled error has occured"

#Capture status of dir
COUNT_WRN=`find $DIR -type f -mmin -$DELAY_WRN | wc -l`
COUNT_CRT=`find $DIR -type f -mmin -$DELAY_CRT | wc -l`

#Test for alert condition
if [ $COUNT_CRT -lt 1 ] && [ $COUNT_WRN -lt 1 ]
then
   echo $MSG_CRT
   exit 2
elif
[ $COUNT_WRN -lt 1 ] && [ $COUNT_CRT -ge 1 ]
then
    echo $MSG_WRN
    exit 1
elif
[ $COUNT_CRT -ge 1 ] && [ $COUNT_WRN -ge 1 ]
then
    echo $MSG_OK
    exit 0
else
    echo $MSG_UKN
    exit 3
fi

