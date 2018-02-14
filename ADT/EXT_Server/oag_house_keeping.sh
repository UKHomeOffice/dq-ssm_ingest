#!/bin/bash
#Maintaining 90 days of ssm data only due to capacity
find /appdata/ADT/archive/oag/ -type f -mtime +90 -delete
