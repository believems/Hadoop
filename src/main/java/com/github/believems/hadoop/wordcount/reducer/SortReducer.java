package com.github.believems.hadoop.wordcount.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by xiangli on 4/1/14.
 */
public class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable> {
    private MultipleOutputs<Text,IntWritable> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos=new MultipleOutputs<Text,IntWritable>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            if(key.get()>100)
                mos.write("More",value,key);
            else
                mos.write("Less",value,key);
            context.write(value,key);
        }
    }
}
