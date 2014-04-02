package com.github.believems.hadoop.wordcount;

import com.github.believems.hadoop.helper.HadoopHelper;
import com.github.believems.hadoop.wordcount.combiner.WordCombiner;
import com.github.believems.hadoop.wordcount.comparator.IntWritableDecreasingComparator;
import com.github.believems.hadoop.wordcount.mapper.WordMapper;
import com.github.believems.hadoop.wordcount.partitioner.WordPartitioner;
import com.github.believems.hadoop.wordcount.reducer.CountReducer;
import com.github.believems.hadoop.wordcount.reducer.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by xiangli on 4/1/14.
 */
public class WordCountRunner implements Tool {
    private static final Configuration conf = HadoopHelper.getConf();
    private static final Path tmpPath = HadoopHelper.getTmpPath();
    private static final String dir = "word_count";
    private Job getSortJob() throws IOException {
        Job job = Job.getInstance(conf);

        // 设置reducer读个数。每个reducer最终会产生一个输出文件
        job.setNumReduceTasks(1);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // 自定义分区类
        job.setJobName("Words Sort");
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, tmpPath);
        FileOutputFormat.setOutputPath(job, HadoopHelper.getOutputPath(dir));
        MultipleOutputs.addNamedOutput(job, "More", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"Less",TextOutputFormat.class,Text.class,IntWritable.class);
        job.setReducerClass(SortReducer.class);
        job.setMapperClass(InverseMapper.class);
        job.setSortComparatorClass(IntWritableDecreasingComparator.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    private Job getCountJob() throws IOException {
        Job job = Job.getInstance(conf);

        // 设置reducer读个数。每个reducer最终会产生一个输出文件
        job.setNumReduceTasks(1);

        // 自定义分区类
        // 本例中，长度相同的单词会被同一个reducer处理，最终也会出现在同一个输出文件中
        job.setPartitionerClass(WordPartitioner.class);

        job.setJobName("Words Count");
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, HadoopHelper.getInputPath(dir));
        FileOutputFormat.setOutputPath(job, tmpPath);


        job.setMapperClass(WordMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setCombinerClass(WordCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        HadoopHelper.delete(HadoopHelper.getOutputPath(dir));
        int exitCode = ToolRunner.run(new WordCountRunner(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {
        try {
            Job countJob = getCountJob();
            if (countJob.waitForCompletion(true)) {
                Job sortJob = getSortJob();
                sortJob.waitForCompletion(true);
                return 1;
            }
            return 0;
        } finally {
            HadoopHelper.delete(tmpPath);
        }
    }

    @Override
    public void setConf(Configuration entries) {

    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
