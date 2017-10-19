package com.briup.bd1702.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

public class ListFileStatus_0010 extends Configured implements Tool {
    private FileSystem fs = null;
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ListFileStatus_0010(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = "hdfs://10.10.2.80:9000/";
        fs = FileSystem.get(URI.create(input),conf,"hadoop");
        FileStatus[] fileStatuses = fs.listStatus(new Path(input));
        for(FileStatus fileStatuse:fileStatuses){
            process(fileStatuse);
        }


        return 0;
    }

    private void process(FileStatus fileStatus) throws IOException {
        if(fileStatus.isFile()){
            System.out.println("----------------------");
            System.out.println("getAccessTime--"+new Date(fileStatus.getAccessTime()));
            System.out.println("getOwner--"+fileStatus.getOwner());
            System.out.println("getGroup--"+fileStatus.getGroup());
            System.out.println("getPermission--"+fileStatus.getPermission());
            String[] s = fileStatus.getPath().toString().split("[/]");
            System.out.println("getPath--"+s[s.length-1]);
            System.out.println("getReplication--"+fileStatus.getReplication());
        }else if(fileStatus.isDirectory()){
            FileStatus[] fileStatuses = fs.listStatus(fileStatus.getPath());
            for(FileStatus status:fileStatuses){
                process(status);
            }
        }
    }
}
