package org.commoncrawl.mklab.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


/**
 * Created by kandreadou on 9/8/14.
 */
public class MediaReduce extends Reducer<Text, Text, Text, NullWritable>
{
    public void reduce(Text key, Iterable<Text> list ,Context context) throws java.io.IOException ,InterruptedException
    {
        if(list.iterator().hasNext())
            context.write(list.iterator().next(), NullWritable.get());
    }
}