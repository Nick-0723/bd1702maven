package com.scl.mapreduce.join.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartitioner extends Partitioner<IdTag, Text> {
    @Override
    public int getPartition(IdTag idTag, Text text, int numPartitions) {
        return Math.abs(idTag.getId().hashCode()*127)%numPartitions;
    }
}
