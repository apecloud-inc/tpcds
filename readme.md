# TPC-DS Benchmark

## Introduction
This repository contains the scripts to quickly execute the TPC-DS benchmark on Mysql or Postgres databases.

## How to use
```shell
$ git clone https://github.com/apecloud-inc/tpcds.git
$ cd tpcds
$ # run the benchmark on Mysql
$ python3 -u main.py --scale 1 --driver mysql --user xxx --password xxx --database test
$ # run the benchmark on Postgres
$ python3 -u main.py --scale 1 --driver postgres --user xxx --password xxx --database test
```

## Parameters
- `--scale`: the scale factor of the benchmark, default is 1
- `--driver`: the database driver, only support `mysql` and `postgres`
- `--user`: the database user
- `--password`: the database password
- `--database`: the database name
- `--host`: the database host, default is `localhost`
- `--port`: the database port, default is `3306` .
- `usekey`: create pk and fk, default is `False`. If set to `True`, the script will create primary key and foreign key for the tables, which will take extra time, especially for mysql.
- `step`: the step of the benchmark, support `cleanup`, `prepare`, `run` and `all`, default is `all`.