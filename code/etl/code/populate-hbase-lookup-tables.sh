#!/bin/bash

batchid=$(date '+%Y%m%d%H')
LOGFILE=/mnt/bigdatapgp/saurav_510636/project_$batchid

echo "Creating LookUp Tables" >> $LOGFILE

echo "create 'channel-geo-map', 'geo'" | hbase shell
echo "create 'subscribed-users', 'subscn'" | hbase shell
echo "create 'video-creator-map', 'creator'" | hbase shell


echo "Populating LookUp Tables" >> $LOGFILE

file="/mnt/bigdatapgp/saurav_510636/project/Lookup data/channel-geocd.txt"
while IFS= read -r line
do
 channelid=`echo $line | cut -d',' -f1`
 geocd=`echo $line | cut -d',' -f2`
 echo "put 'channel-geo-map', '$channelid', 'geo:geo_cd', '$geocd'" | hbase shell
done <"$file"


file="/mnt/bigdatapgp/saurav_510636/project/Lookup data/video-creator.txt"
while IFS= read -r line
do
 videoid=`echo $line | cut -d',' -f1`
 creatorid=`echo $line | cut -d',' -f2`
 echo "put 'video-creator-map', '$videoid', 'creator:creatorid', '$creatorid'" | hbase shell
done <"$file"


file="/mnt/bigdatapgp/saurav_510636/project/Lookup data/user-subscn.txt"
while IFS= read -r line
do
 userid=`echo $line | cut -d',' -f1`
 startdt=`echo $line | cut -d',' -f2`
 enddt=`echo $line | cut -d',' -f3`
 echo "put 'subscribed-users', '$userid', 'subscn:startdt', '$startdt'" | hbase shell
 echo "put 'subscribed-users', '$userid', 'subscn:enddt', '$enddt'" | hbase shell
done <"$file"
