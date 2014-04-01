package com.github.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by xiangli on 4/1/14.
 */
public class ConfHelper {
    public static Configuration getConf(){
        Configuration conf = new Configuration();
        conf.set("mapreduce.jobtracker.address", "127.0.0.1:9001");
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        return conf;
    }
}
