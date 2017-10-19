package com.briup.bd1702.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class DataIntegrity_Put_0010 extends Configured implements Tool {
    private FileSystem fs;
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String output1 = conf.get("output1");
        String output2 = conf.get("output2");
        fs = new RawLocalFileSystem();
        fs.initialize(URI.create(output1),conf);
        FSDataOutputStream fdos = fs.create(new Path(output1));
        fdos.write("rrrrrrrrrrrrrrrrrrrrrrrrrrrrr".getBytes());
        fdos.flush();
        fdos.close();

        fs = new LocalFileSystem(fs);
        FSDataOutputStream fdos1 = fs.create(new Path(output2));
        fdos1.write("lllllllllllllllllllllllllll".getBytes());
        fdos1.flush();
        fdos1.close();



        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DataIntegrity_Put_0010(),args));
    }
}
