package com.naumov.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SimpleLocalLs {
    public static void main(String[] args) throws Exception {
        /* Path is actually URI in FS
        # hdfs://localhost/user/file1
        # file:///user/file1
        Path path = new Path("/test/file1"); Default - shcheme + authority from configs
        Path path = new Path("hdfs://localhost:9000/test/file1");
        */
        Path path = new Path("/");

        if(args.length == 1){
            path = new Path(args[0]);
        }

        /**
         * Stores all hadoop cluster configs.
         * Key-value map
         * String name = conf.get("param_name"); -> null if param is absent
         * String name = conf.get("param_name", "default_param_value_if_absent");
         */
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] files = fs.listStatus(path);
        for (FileStatus file : files) {
            System.out.println(file.getPath().getName());
        }
    }
}