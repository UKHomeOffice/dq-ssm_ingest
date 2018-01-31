#!/bin/bash

# Module Type : Shell script
# Module Name : deploy_APD-4607.sh
# Description : This script is used for installing SSM update APD-4607 into Postgres. 
# Run Env.    : UNIX

date=$(date +%Y%m%d)
script=deploy_APD-4607

function log {
    echo $1 >> ${script}_${date}.log
    echo $1 >> ${script}_${date}.err
}

function pinstall {
    log "`date +"%Y-%m-%d %H:%M:%S"` $2"
    psql -f $2 $1 >> ${script}_${date}.log 2>> ${script}_${date}.err
}

while true
do

echo Installing to database $1, is this correct?
read choice
case $choice in

"n")

exit 0
;;

"y")

log "==================== BEGIN DEPLOYMENT ===================="

#Connecting to database
echo connecting to $1
log "connecting to $1"

pinstall $1 APD-4607_data_fix.sql

echo Deploy complete!
echo
echo ============================== Error Log ==============================
cat ${script}_${date}.err

exit 0
;;

*)
echo Please enter y or n
;;

esac
done
