package com.briup.bd1702.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.List;

public class ListBlocks_0010 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ListBlocks_0010(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");
        FileSystem fs = FileSystem.get(URI.create(input),conf);
        //块信息在HdfsDataInputStream流里面
        HdfsDataInputStream in = (HdfsDataInputStream)fs.open(new Path(input));
        List<LocatedBlock> blocks = in.getAllBlocks();
//        Stream<LocatedBlock> stream = blocks.stream();
        blocks.forEach((block)->{
            ExtendedBlock extendedBlock = block.getBlock();
            System.out.println("================");
            System.out.println("getBlockId:"+extendedBlock.getBlockId());
            System.out.println("getBlockName:"+extendedBlock.getBlockName());
            System.out.println("getBlockSize:"+block.getBlockSize());
            System.out.println("getStartOffset:"+block.getStartOffset());


            DatanodeInfo[] infos = block.getLocations();
            for(DatanodeInfo info : infos){
                System.out.println("getIpAddr:"+info.getIpAddr());
                System.out.println("getHostName:"+info.getHostName());
            }
        });
        /*for(LocatedBlock block : blocks){
            ExtendedBlock extendedBlock = block.getBlock();
            System.out.println("================");
            System.out.println("getBlockId:"+extendedBlock.getBlockId());
            System.out.println("getBlockName:"+extendedBlock.getBlockName());
            System.out.println("getBlockSize:"+block.getBlockSize());
            System.out.println("getStartOffset:"+block.getStartOffset());


            DatanodeInfo[] infos = block.getLocations();
            for(DatanodeInfo info : infos){
                System.out.println("getIpAddr:"+info.getIpAddr());
                System.out.println("getHostName:"+info.getHostName());
            }
        }*/


        return 0;
    }
}
