#!/usr/bin/env bash

int=1
cur_time="`date +%Y-%m-%d`"
file="debugLog/BasicAgree2B/${cur_time}..txt"
while(($int<=100))
do
    go test -run BasicAgree2B
    let "int++"
done
