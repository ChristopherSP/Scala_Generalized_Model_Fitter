#!/bin/bash

pem_file=$1
master_dns=$2

ssh -i $pem_file $master_dns \
        "mkdir -p main/output/performance && \
        mkdir main/confFiles && \
        mkdir main/jars && \
        hdfs dfs -mkdir -p /user/hadoop/main/output/ && \
        hdfs dfs -mkdir /user/hadoop/main/confFiles"

this_path=$(dirname "$0")
cd $this_path
cd ..

parameters_path=$PWD
parameters_file="$parameters_path/parameters.yml"

ls $parameters_file

scp -i $pem_file "$parameters_file $master_dns:/home/hadoop/main/confFiles/"

scp -i $pem_file "$parameters_path/target/scala-2.11/spark_auto_ml-assembly-0.1.jar" "$master_dns:/home/hadoop/main/jars"

ssh -i $pem_file $master_dns "hdfs dfs -put ~/main/confFiles/parameters.yml /user/hadoop/main/confFiles/"

