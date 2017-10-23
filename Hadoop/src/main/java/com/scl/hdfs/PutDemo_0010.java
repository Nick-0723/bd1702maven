package com.scl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class PutDemo_0010 extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input","/home/hadoop/id_rsa.pub");
        String output = conf.get("output","hdfs://10.10.2.80:9000:/user/hadoop/b.txt");
        FileSystem fs = FileSystem.get(URI.create(output),conf);
        FSDataOutputStream out = fs.create(new Path(output));
        LocalFileSystem lfs = LocalFileSystem.getLocal(conf);
        FSDataInputStream in = lfs.open(new Path(input));
        IOUtils.copyBytes(in,out,1024,true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PutDemo_0010(),args));
    }
}
