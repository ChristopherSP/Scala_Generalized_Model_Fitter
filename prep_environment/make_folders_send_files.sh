#!/bin/bash

pem_file=$1
master_dns=$2

ssh -i $pem_file $master_dns \
        "mkdir -p aijusProd/output/performance && \
        mkdir aijusProd/confFiles && \
        mkdir aijusProd/jars && \
        hdfs dfs -mkdir -p /user/hadoop/aijusProd/output/ && \
        hdfs dfs -mkdir /user/hadoop/aijusProd/confFiles"

this_path=$(dirname "$0")
cd $this_path
cd ..

parameters_path=$PWD
parameters_file="$parameters_path/parameters.yml"

ls $parameters_file

scp -i $pem_file "$parameters_file $master_dns:/home/hadoop/aijusProd/confFiles/"

scp -i $pem_file "$parameters_path/target/scala-2.11/aijus_prod_v01-assembly-0.1.jar" "$master_dns:/home/hadoop/aijusProd/jars"

ssh -i $pem_file $master_dns "hdfs dfs -put ~/aijusProd/confFiles/parameters.yml /user/hadoop/aijusProd/confFiles/"

