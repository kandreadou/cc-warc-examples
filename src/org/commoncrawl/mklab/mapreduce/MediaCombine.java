package org.commoncrawl.mklab.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by kandreadou on 10/3/14.
 */
public class MediaCombine extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> list ,Context context) throws java.io.IOException ,InterruptedException
    {
        if(list.iterator().hasNext())
            context.write(key, list.iterator().next());
    }
}