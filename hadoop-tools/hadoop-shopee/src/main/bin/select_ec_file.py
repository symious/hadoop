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
from pyspark.sql import Row

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: select_ec_file <inputPath> <grass_date> .")
        sys.exit(-1)

    path = sys.argv[1]
    grass_date = sys.argv[2]
    path = path + "/" + grass_date

    warehouse_location = abspath('spark-warehouse')

    spark = SparkSession \
        .builder \
        .appName("Python Spark query ec file") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.sql("select ns, path from data_metamart.ods_nn_fsimage_df where grass_date='{}' and type=1 and erasure_coding_policy is not null".format(grass_date))
    df.write.options(header='False', delimiter='\t').csv(path)
    spark.stop()