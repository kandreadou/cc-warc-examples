package org.commoncrawl.mklab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
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
import org.commoncrawl.mklab.mapreduce.MediaCombine;
import org.commoncrawl.mklab.mapreduce.MediaMap;
import org.commoncrawl.mklab.mapreduce.MediaReduce;
import org.commoncrawl.warc.WARCFileInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.util.Scanner;

/**
 * Created by kandreadou on 9/4/14.
 */
public class AWSMediaExtractor extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(AWSMediaExtractor.class);

    /**
     * Main entry point that uses the {@link org.apache.hadoop.util.ToolRunner} class to run the Hadoop job.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AWSMediaExtractor(), args);
        System.exit(res);
    }

    /**
     * Builds and runs the Hadoop job.
     *
     * @return 0 if the Hadoop job completes successfully and 1 otherwise.
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

        int start = 50001;
        int end = 63500;
        int step = 500;
        boolean success = true;

        while (start < end) {
            int intermediate = start+step;
            if(intermediate>end)
                intermediate = end;
            Job job = createJob(outputPath, start, intermediate);
            start += step;
            success &= job.waitForCompletion(true);
        }
        return success ? 0 : -1;
    }

    private Job createJob(String outputPath, int start, int end) throws Exception {
        outputPath += start;
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(AWSMediaExtractor.class);
        job.setNumReduceTasks(15);

        /*String start = "s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-23/segments/1404776400583.60/warc/CC-MAIN-20140707234000-0000";
        String end = "-ip-10-180-212-248.ec2.internal.warc.gz";
        String inputPath = "";
        for (int i = 0; i < 10; i++) {
            inputPath += start + i + end;
            if (i != 9)
                inputPath += ',';
        }*/

        addPaths(job, start, end); //1101, 2100

        FileSystem fs = FileSystem.get(new URI(outputPath), conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MediaMap.ImageMapper.class);
        job.setReducerClass(MediaReduce.class);

        return job;
    }

    private void addPaths(Job job, int start, int end) {
        try {
            int counter = 0;
            InputStream configStream = getClass().getResourceAsStream("/warc.path");
            Scanner scanner = new Scanner(configStream);
            while (scanner.hasNextLine() && counter < end) {
                counter++;
                String line = scanner.nextLine();
                if (counter >= start)
                    FileInputFormat.addInputPath(job, new Path("s3n://aws-publicdatasets/" + line));
            }
        } catch (IOException ioe) {
            //ignore
        }
    }
}