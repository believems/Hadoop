package com.github.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;

/**
 * Created by xiangli on 4/1/14.
 */
public class FileHelper {
    private static Configuration configuration = ConfHelper.getConf();

    public static final Path InputPath = new Path("/txt/doc.txt");
    public static final Path OutputPath = new Path("/out/test/wordcount/");

    public static Path getTmpPath() {
        Path tempDir = new Path("/tmp/temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
        return tempDir;
    }

    public static void delete(Path path) throws IOException {
        FileSystem.get(configuration).delete(path,true);
    }

    public static void emptyTestDir() throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(OutputPath))
            fileSystem.delete(OutputPath,true);
//        fileSystem.mkdirs(OutputPath);
    }
    public static void print(Path path) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        if (!fileSystem.exists(path))
            return;
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        System.out.println(path);
        for (FileStatus fileStatus : fileStatuses) {
            Path innerPath = fileStatus.getPath();
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
    }
}
