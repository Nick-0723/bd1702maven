package com.scl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class GetDemo_0010 {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://10.10.2.80:9000/user/hadoop/a.txt"), configuration, "hadoop");
//        这个可以直接拷贝
//        fileSystem.copyToLocalFile(new Path("hdfs://10.10.2.80:9000/user/hadoop/a.txt"),new Path("/home/hadoop/a.txt"));
        FSDataInputStream inputStream = fileSystem.open(new Path("hdfs://10.10.2.80:9000/user/hadoop/a.txt"));
//        这个是拿到Windows下
//        FileOutputStream out = new FileOutputStream("a.txt");

//        这个要有Hadoop环境才能用,并且会有一个.开头crc后缀的隐藏文件
        LocalFileSystem local = FileSystem.getLocal(configuration);
        FSDataOutputStream out = local.create(new Path("/home/hadoop/a.txt"));
        byte[] b = new byte[1024];
        int len = -1;
        while ((len = inputStream.read(b))!=-1){
            out.write(b,0,len);
        }
        out.flush();
        out.close();
        inputStream.close();
    }
}
