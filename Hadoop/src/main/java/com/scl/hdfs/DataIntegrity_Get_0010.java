package com.scl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.InputStream;
import java.net.URI;

public class DataIntegrity_Get_0010 extends Configured implements Tool {
    private FileSystem fs;
    private InputStream in;
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String output1 = conf.get("output1");
        String output2 = conf.get("output2");
        fs = new RawLocalFileSystem();
        fs.initialize(URI.create(output1),conf);
        in = fs.open(new Path(output1));
        byte[] b = new byte[1024];
        int len = in.read(b);
        System.out.println(new String(b,0,len));


        fs = new LocalFileSystem(fs);
        in = fs.open(new Path(output2));
        len = in.read(b);
        System.out.println(new String(b,0,len));

        in.close();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DataIntegrity_Get_0010(),args));
    }
}
