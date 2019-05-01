package com.naumov.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class WriteToFile {
    public static void main(String[] args) throws IOException {
        String text = "Hello world in HDFS";
        InputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(text.getBytes()));

        Path path = new Path("user/folder/file.txt");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        FSDataOutputStream outputStream = fileSystem.create(path);
        IOUtils.copyBytes(inputStream, outputStream, configuration);
    }
}
