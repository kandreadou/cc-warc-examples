package org.commoncrawl.mklab;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


/**
 * Created by kandreadou on 9/8/14.
 */
public class ImageReduce extends Reducer<Text, Text, Text, NullWritable>
{
    public void reduce(Text key, Iterable<Text> list ,Context context) throws java.io.IOException ,InterruptedException
    {
        if(list.iterator().hasNext())
            context.write(list.iterator().next(), NullWritable.get());
    }
}