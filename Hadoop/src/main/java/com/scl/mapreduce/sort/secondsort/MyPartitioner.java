package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartitioner extends Partitioner<YearTemperature, Text> {
    @Override
    public int getPartition(YearTemperature yearTemperature, Text text, int numPartitions) {
        return Math.abs(yearTemperature.getYear().hashCode()*127)%numPartitions;
    }
}
