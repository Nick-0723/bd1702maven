package com.briup.bd1702.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class WriteDemo_0010 extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");
        String output = conf.get("output");
        System.out.println(input);
        System.out.println(output);
        LocalFileSystem local = FileSystem.getLocal(conf);
        FileSystem fileSystem = FileSystem.get(URI.create(output), conf, "hadoop");
        FSDataInputStream lin = local.open(new Path(input), 1024);
        FSDataOutputStream dfsout = fileSystem.create(new Path(output));
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(new Path(output));
        CompressionOutputStream out = codec.createOutputStream(dfsout);
        IOUtils.copyBytes(lin,out,1024,true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WriteDemo_0010(),args));
    }
}
