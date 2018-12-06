#!/usr/bin/env bash

int=1
file="/debugLog/BasicAgree2B/${cur_time}.txt"
while(($int<=10))
do
    cur_time="`date +%Y-%m-%d`"
    go test -run BasicAgree2B
    let "int++"
done
