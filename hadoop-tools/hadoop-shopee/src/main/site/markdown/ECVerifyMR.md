<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Hadoop EC FILE VALIDATOT TOOL Guide
<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

## Overview
The `ECValidatorJob` is a tool used for ec files to be verified in parallel.

### Verifiy principle
For example, with a EC 6-3 file: EC(RS-6-3-1024k):[d1,d2,d3,d4,d5,d6,p0,p1,p2]
* The parity blocks are p0,p1,p2.
* Use data blocks d1 - d5 to regenerate the parity blocks p0', p1' and p2'.
* Compare [p0, p1, p2] and [p0', p1', p2'] to check whether the blocks are consistent.

### Usage

The usage instructions are:
Here let $HADOOP_ROOT represent the Hadoop install directory. If you build Hadoop yourself, $HADOOP_ROOT is hadoop-dist/target/hadoop-$VERSION. 
The location of the hadoop-shopee, $ShopeeServiceHome, is $HADOOP_ROOT/share/hadoop/tools/shopee
* `bin` contains the running scripts

Usage: `sh bin/ec_verify_mr.sh [-q <yarn queue>] [-i <input_path>] [-o <output_path>]  [-m <map_tasks>]  [-r <reduce_tasks>]`

#### Required command line arguments:

| COMMAND\_OPTIONS | Description                                            |
|:-----------------|:-------------------------------------------------------|
| -q               | Queue to which a job is submitted.                     |
| -i               | Input directory containing all file paths to validate. |
| -o               | Directory to write the results in.                     |
| -m               | Number of mappers to use in the job.                   |
| -r               | Number of reducers to use in the job.                  |

#### Example:

##### COMMAND: 

    $ cd $ShopeeServiceHome
    $ sh bin/ec_verify_mr.sh
      -q infra 
      -i /projects/test/hdfs/stag/inputec/2022-11-23
      -o /projects/test/hive/test/ec_file_validate_result/2022-11-23
      -m 100 
      -r 100

###### OPTION: `-i /projects/test/hdfs/stag/inputec/2022-11-23`

```bash
Input directory containing all file paths, we can get all the ec files of the current cluster through the following program .

spark-submit \
  --deploy-mode cluster \
  --executor-memory 10G \
  --executor-cores 3 \
  --queue infra \
  bin/select_ec_file.py /projects/test/hdfs/stag/inputec 2022-11-23
```
Detailed data such as: total of 2 fields(ns, hdfs path), each fields is divided according to "\t"

```bash
hadoop fs -cat hdfs:/projects/test/hdfs/stag/inputec/2022-11-23/part-1-00000
ecdev1 /user/test/ec/XOR-2-1-1024k/LICENSE.txt
ecdev1 /user/test/ec/XOR-2-1-1024k/XOR-2-1-1024k
ecdev1 /user/test/rep/LICENSE.txt
ecdev1 /user/test/ec/XOR-2-1-1024k/XOR-2-1-1024k_compute
ecdev1 /user/test/ec/XOR-2-1-1024k/
```

###### OPTION: `-o /projects/test/hive/test/ec_file_validate_result/2022-11-23`

```bash
For the result data, hive table can bgenerated to facilitate subsequent analysis, through the following program .

spark-submit \
  --deploy-mode cluster \
  --executor-memory 10G \
  --executor-cores 3 \
  --queue infra \
  bin/load_ec_file_validate_result.py /projects/test/hive/test/ec_file_validate_result 2022-11-23 test ec_file_validate_result
```
Detailed data such as: Output in the format defined below

`ns|file|status(healthy or failed)|failed block groups|corrupt block groups|under-erasure-coded block groups`
* failed block groups: block_group_id$block_internal_id1$datanodeip:xferPort$message,block_group_id$block_internal_id2$datanodeip:xferPort$message
* corrupt block groups: block_group_id$block_internal_id1$datanodeip:xferPort$N/A,block_group_id$block_internal_id2$datanodeip:xferPort$N/A
* under-erasure-coded block groups: block_group_id$block_internal_id1$datanodeip:xferPort$N/A,block_group_id$block_internal_id2$datanodeip:xferPort$N/A

```bash
hadoop fs -text /projects/test/hive/test/ec_file_validate_result/2022-11-23/part-r-00000.zst
ecdev1|/user/test/ec/XOR-2-1-1024k/|failed|File /user/test/ec/XOR-2-1-1024k/ is not a regular file.|N/A|N/A
ecdev1|/user/test/rep/LICENSE.txt|failed|File /user/test/rep/LICENSE.txt is not erasure coded.|N/A|N/A
ecdev1|/user/test/ec1|failed|no valid|N/A|N/A
ecdev1|/user/test/ec/XOR-2-1-1024k/LICENSE.txt|healthy|N/A|N/A|N/A
ecdev1|/user/test/ec/XOR-2-1-1024k/XOR-2-1-1024k|failed|N/A|N/A|blk_-9223372036842590800,
ecdev1|/user/test/ec/XOR-2-1-1024k/XOR-2-1-1024k_compute|failed|[blk_-9223372036842590638$blk_-9223372036842590638_1009399$ip:9866$org.apache.hadoop.fs.ChecksumException: Checksum error: /user/test/ec/XOR-2-1-1024k/XOR-2-1-1024k_compute at 0 exp: 1446139980 got: -1748092015],|[blk_-9223372036842590670$blk_-9223372036842590670_1009395$ip:9866$N/A],|N/A

or query hive table
select * from test.ec_file_validate_result where grass_date='2022-11-23';
```