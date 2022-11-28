# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
from os.path import abspath
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: load_ec_file_validate_result <loadPath> <grass_date> <db> <table>.")
        sys.exit(-1)

    loadPath = sys.argv[1]
    grass_date = sys.argv[2]
    db = sys.argv[3]
    table = sys.argv[4]

    warehouse_location = abspath('spark-warehouse')

    spark = SparkSession \
        .builder \
        .appName("Python load_ec_file_validate_result") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    create_sql = ("create external table if not exists {}.{} (ns string, path string, status string, failed string, corrupt string, under string) partitioned by (grass_date string) row format delimited fields terminated by '|'stored as textfile location '{}'".format(db, table, loadPath, grass_date))
    spark.sql(create_sql)

    partitionPath = loadPath + "/" + grass_date
    spark.sql("load data inpath '{}' into table {}.{} partition(grass_date='{}')".format(partitionPath, db, table, grass_date))

    spark.stop()
