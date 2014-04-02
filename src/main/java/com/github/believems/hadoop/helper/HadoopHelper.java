package com.github.believems.hadoop.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Random;

/**
 * Created by Administrator on 2014/4/2.
 */
public class HadoopHelper {
    private static final Properties hadoopProperties = PropertryHelper.loadProperties("hadoop.properties");
    private static final String inputPathBase = hadoopProperties.getProperty("input_path");
    private static final String outputPathBase = hadoopProperties.getProperty("output_path");

    public static Path getInputPath(String filename) {
        return new Path(inputPathBase, filename);
    }

    public static Path getOutputPath(String filename) {
        return new Path(outputPathBase, filename);
    }

    public static void delete(Path path) throws IOException {
        FileSystem.get(getConf()).delete(path, true);
    }

    public static void printDir(Path path) throws IOException {
        assert path != null;
        FileSystem fileSystem = FileSystem.get(getConf());
        if (!fileSystem.exists(path)) {
            System.out.println("Path: " + path + " not exists.");
            return;
        }
        System.out.println(path);
        // 如果是目录
        if (fileSystem.isDirectory(path)) {
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            if (fileStatuses.length > 0) {
                for (FileStatus file : fileStatuses) {
                    printDir(file.getPath());
                }
            }
        } else {
            printFile(path);
        }
    }

    public static void printDir(FileStatus fileStatus) throws IOException {
        assert fileStatus != null;
        printFile(fileStatus.getPath());
    }

    public static void printFile(FileStatus fileStatus) throws IOException {
        printFile(fileStatus.getPath());
    }

    public static void printFile(Path innerPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(getConf());
        if (fileSystem.isFile(innerPath)) {
            String fileName = innerPath.getName();
            InputStream inputStream = fileSystem.open(innerPath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while (bufferedReader.ready() && (line = bufferedReader.readLine()) != null) {
                System.out.println(fileName + ":" + line);
            }
            bufferedReader.close();
            inputStream.close();
        }
    }

    public static Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set("mapreduce.jobtracker.address", hadoopProperties.getProperty("mapreduce.jobtracker.address"));
        conf.set("fs.defaultFS", hadoopProperties.getProperty("fs.defaultFS"));
        return conf;
    }

    public static Path getTmpPath() {
        String tmpDir = hadoopProperties.getProperty("tmpdir.path");
        Path tempDir = new Path(tmpDir + "/temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
        return tempDir;
    }
}
