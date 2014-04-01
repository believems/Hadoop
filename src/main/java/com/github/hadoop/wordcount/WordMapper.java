package com.github.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by xiangli on 4/1/14.
 */
public class WordMapper extends Mapper<Object,Text,Text,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String pattern = "[^\\w]"; // 正则表达式，代表不是0-9, a-z, A-Z的所有其它字符,其中还有下划线
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase(); // 全部转为小写字母
        line = line.replaceAll(pattern, " "); // 将非0-9, a-z, A-Z的字符替换为空格
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
