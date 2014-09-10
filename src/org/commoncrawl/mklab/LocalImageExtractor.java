package org.commoncrawl.mklab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.warc.WARCFileInputFormat;

/**
 * Created by kandreadou on 9/5/14.
 */
public class LocalImageExtractor extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(LocalImageExtractor.class);

    /**
     * Main entry point that uses the {@link org.apache.hadoop.util.ToolRunner} class to run the Hadoop job.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LocalImageExtractor(), args);
        System.exit(res);
    }

    /**
     * Builds and runs the Hadoop job.
     * @return	0 if the Hadoop job completes successfully and 1 otherwise.
     */
    @Override
    public int run(String[] args) throws Exception {

        String outputPath = "/output";
        String inputPath = "/input/*.warc.gz";

        Configuration conf = getConf();
        //
        Job job = new Job(conf);
        job.setJarByClass(LocalImageExtractor.class);
        job.setNumReduceTasks(1);

        //String inputPath = "data/*.warc.gz";
        //String inputPath = "https://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-data/CC-MAIN-2014-23/segments/1404776400583.60/warc/CC-MAIN-20140707234000-00023-ip-10-180-212-248.ec2.internal.warc.gz";
        //inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.wet.gz";
        //inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/*.warc.wet.gz";
        LOG.info("Input path: " + inputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));

        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job,  GzipCodec.class);

        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ImageMap.ImageMapper.class);
        job.setReducerClass(ImageReduce.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}