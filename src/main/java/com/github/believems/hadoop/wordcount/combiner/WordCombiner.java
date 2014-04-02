package com.github.believems.hadoop.wordcount.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WordCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(WordCombiner.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
//            LOG.info(value.get()+"");
        }

        context.write(key, new IntWritable(sum));
    }
}
