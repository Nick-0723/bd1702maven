package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartitioner extends Partitioner<OneThree, Text> {
    @Override
    public int getPartition(OneThree oneThree, Text text, int numPartitions) {
        return Math.abs(oneThree.getOne().hashCode()*127)%numPartitions;
    }
}
