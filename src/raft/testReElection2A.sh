#!/usr/bin/env bash

int=1
cur_time="`date +%Y-%m-%d`"
rootPath="debugLog/ReElection2A"
sudo rm -rf ${rootPath}
sudo mkdir ${rootPath}
sudo chmod 777 ${rootPath}
while(($int<=100))
do
#    mkdir debugLog/ConcurrentStarts2B
#    sudo chmod 777 debugLog/ConcurrentStarts2B
    file="${rootPath}/${cur_time}_${int}.txt"
    sudo touch ${file}
    sudo chmod 777 ${file}
    go test -run ReElection2A > ${file}
    let "int++"
done