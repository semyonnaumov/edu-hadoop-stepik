package com.naumov.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;

public class ReadFile {
    public static void main(String[] args) throws IOException {
        Path path = new Path("/path/to/file.txt");
        FileSystem fileSystem = FileSystem.get(new Configuration());

        InputStream inputStream = null;
        try {
            inputStream = fileSystem.open(path);
            IOUtils.copyBytes(inputStream, System.out, 4096);
        } finally {
            IOUtils.closeStream(inputStream);
        }
    }
}
