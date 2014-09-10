package org.commoncrawl.mklab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.examples.mapreduce.TagCounterMap;
import org.commoncrawl.examples.mapreduce.WARCTagCounter;
import org.commoncrawl.warc.WARCFileInputFormat;

import java.text.DateFormat;

/**
 * Created by kandreadou on 9/4/14.
 */
public class AWSMediaExtractor extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(WARCTagCounter.class);

    /**
     * Main entry point that uses the {@link org.apache.hadoop.util.ToolRunner} class to run the Hadoop job.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WARCTagCounter(), args);
        System.exit(res);
    }

    /**
     * Builds and runs the Hadoop job.
     * @return	0 if the Hadoop job completes successfully and 1 otherwise.
     */
    @Override
    public int run(String[] args) throws Exception {

        String configFile = null;

        // Read the command line arguments.
        if (args.length < 1)
            throw new IllegalArgumentException("Example JAR must be passed an output path.");

       String outputPath = args[0];

        if (args.length >= 2)
            configFile = args[1];

        if (configFile != null) {
            LOG.info("adding config parameters from '" + configFile + "'");
            this.getConf().addResource(configFile);
        }

        Configuration conf = getConf();
        //
        Job job = new Job(conf);
        job.setJarByClass(WARCTagCounter.class);
        job.setNumReduceTasks(1);

        //String inputPath = "data/*.warc.gz";
        String inputPath = "s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-23/segments/1404776400583.60/warc/CC-MAIN-20140707234000-00023-ip-10-180-212-248.ec2.internal.warc.gz";
        //inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.wet.gz";
        //inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/*.warc.wet.gz";
        LOG.info("Input path: " + inputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));

        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(TagCounterMap.TagCounterMapper.class);
        job.setReducerClass(LongSumReducer.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}