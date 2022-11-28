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
package org.apache.hadoop.tools.ec;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.tools.ec.mapred.ECValidatorMapper;
import org.apache.hadoop.tools.ec.mapred.ECValidatorReducer;
import org.apache.hadoop.hdfs.util.ECBlockValidatorReport;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides that can be used to execute ec file validate operation.
 */
public class ECValidatorJob extends Configured implements Tool {

  private static Logger LOG = LoggerFactory.getLogger(ECValidatorJob.class);

  private static final String OPT_OUT_PUT_DIR = "output_dir";
  private static final String OPT_IN_PUT_DIR = "input_dir";
  private static final String OPT_MAPPERS = "mappers";
  private static final String OPT_REDUCES = "reduces";
  private static final String OPT_HELP = "help";

  private FileSystem fs = null;
  private Options options = new Options();

  private String inputDir ;
  private String outputDir;
  private final int defaultValue = 100;
  private int numMapTasks = defaultValue;
  private int numReduceTasks = defaultValue;

  private static final String usage = "ecvalidator"
      + " <-i <input hdfs dir>> <-o <output hdfs dir>> <-m <arg>>  <-r <arg>>";

  @VisibleForTesting
  public ECValidatorJob(Configuration conf) {
    setConf(conf);
  }

  public ECValidatorJob() {}

  private CommandLine setupOptions(String[] args) throws ParseException {
    Option opt;
    opt = new Option("i", OPT_IN_PUT_DIR, true,
        "Input directory containing all file paths to validate.");
    opt.setRequired(true);
    options.addOption(opt);

    opt = new Option("o", OPT_OUT_PUT_DIR, true,
        "Directory to write the results in.");
    opt.setRequired(true);
    options.addOption(opt);

    opt = new Option("m", OPT_MAPPERS, true,
        "Number of mappers to use in the job, default is " + defaultValue);
    opt.setRequired(false);
    options.addOption(opt);

    opt = new Option("r", OPT_REDUCES, true,
        "Number of reduces to use in the job, default is " + defaultValue);
    opt.setRequired(false);
    options.addOption(opt);

    opt = new Option("h", OPT_HELP, false,
        "Show the usage.");
    opt.setRequired(false);
    options.addOption(opt);

    return new GnuParser().parse(options, args);
  }

  private void printUsage(boolean printDetailed) {
    if (printDetailed) {
      new HelpFormatter().printHelp("org.apache.hadoop.fs.tools.ec.ECValidatorJob",
          options);
    } else {
      System.out.println(usage);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage(false);
      return 1;
    }

    CommandLine opts;
    try {
      opts = setupOptions(args);
    } catch(ParseException e) {
      LOG.error("Exception in ECValidatorJob ", e);
      printUsage(true);
      return 1;
    }

    if (opts.hasOption(OPT_HELP)) {
      printUsage(true);
      return 1;
    }

    Configuration conf = getConf();
    fs = FileSystem.get(conf);
    inputDir = opts.getOptionValue(OPT_IN_PUT_DIR);
    outputDir = opts.getOptionValue(OPT_OUT_PUT_DIR);

    if (fs.exists(new Path(outputDir))) {
      LOG.error("Output directory already exists. Please remove it and rerun.");
      return 1;
    }

    if (!fs.exists(new Path(inputDir))) {
      LOG.error("Input directory not exists. Please confirm.");
      return 1;
    }

    String strMaps = opts.getOptionValue(OPT_MAPPERS);
    if (strMaps != null) {
      numMapTasks = Integer.parseInt(strMaps);
      if (numMapTasks < 1) {
        LOG.warn("Maps is less than 1 and set default value {}.", defaultValue);
        numMapTasks = defaultValue;
      }
    }

    String strReduces = opts.getOptionValue(OPT_REDUCES);
    if (strReduces != null) {
      numReduceTasks = Integer.parseInt(strReduces);
      if (numReduceTasks < 1) {
        LOG.warn("Reduces is less than 1 and set default value {}.", defaultValue);
        numReduceTasks = defaultValue;
      }
    }

    LOG.info("Input Options: inputDir=" + inputDir + ", outputDir=" + outputDir + ", " +
        "numMapTasks=" + numMapTasks + ", " + "numReduceTasks=" + numReduceTasks);

    // Create job
    Job job = Job.getInstance(conf, "EC-Validate-Job");
    job.setJarByClass(getClass());

    // Setup MapReduce
    job.setMapperClass(ECValidatorMapper.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ECBlockValidatorReport.class);
    job.getConfiguration().set(JobContext.NUM_MAPS, String.valueOf(numMapTasks));
    job.setNumReduceTasks(numReduceTasks);
    job.setReducerClass(ECValidatorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Input
    FileInputFormat.addInputPath(job, new Path(inputDir));
    job.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, new Path(outputDir));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ECValidatorJob(), args);
    System.exit(exitCode);
  }
}
