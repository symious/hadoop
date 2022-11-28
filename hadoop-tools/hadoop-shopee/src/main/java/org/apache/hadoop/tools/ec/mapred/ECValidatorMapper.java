/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools.ec.mapred;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.ECBlockValidatorReport;
import org.apache.hadoop.hdfs.util.ECFileValidator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Mapper class that executes the ec file validate operation.
 * Implements the Mapper interface.
 */
public class ECValidatorMapper extends Mapper<LongWritable, Text, Text, ECBlockValidatorReport> {

  private static Logger LOG = LoggerFactory.getLogger(ECValidatorMapper.class);

  private Configuration conf;
  private ECFileValidator ecFileValidator;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    conf = context.getConfiguration();
    ecFileValidator = new ECFileValidator(conf);
  }

  /**
   * Implementation of the Mapper::map() method.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String fileStr = value.toString();
    LOG.info("map received {}.", fileStr);
    String fileStrS[] = fileStr.split("\t");
    if (fileStrS.length == 2) {
      String file = fileStrS[1];
      List<ECBlockValidatorReport> ecBlockValidatorReports = ecFileValidator.verifyECFile(file,
          true);
      if (!CollectionUtils.isEmpty(ecBlockValidatorReports)) {
        for (ECBlockValidatorReport ecBlockValidatorReport : ecBlockValidatorReports) {
          context.write(value, ecBlockValidatorReport);
        }
        return;
      }
    }
    LOG.warn("map received {} is not valid.", fileStr);
    context.write(value, ecFileValidator.createFailedReport("no valid"));
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    super.cleanup(context);
    this.ecFileValidator.close();
  }
}
