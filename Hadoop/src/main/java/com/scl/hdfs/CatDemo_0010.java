package com.scl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CatDemo_0010 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //创建configuration
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://10.10.2.80:9000/user/hadoop/a.txt"),conf,"hadoop");
        FSDataInputStream in = fs.open(new Path("hdfs://10.10.2.80:9000/user/hadoop/a.txt"));
        byte[] buff = new byte[1024];
        int length = -1;
        while ((length=in.read(buff))!=-1){
            System.out.println(new String(buff,0,length));
        }


//        if(fs.delete(new Path(args[0]),true)) System.out.println("删除成功");

        in.close();

    }
}
