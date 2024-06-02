#!/bin/bash

mkdir -p ./csv/framework
mkdir -p ./csv/nginx

for i in {1..20}
do
    echo "Running test $i for the framework"
    hey -n 100000 -c 250 -m GET -o csv http://127.0.0.1:8080 > ./csv/framework/$i.csv
done

sleep 1

for i in {1..20}
do
    echo "Running test $i for nginx"
    hey -n 100000 -c 250 -m GET -o csv http://127.0.0.1:8081 > ./csv/nginx/$i.csv
done
