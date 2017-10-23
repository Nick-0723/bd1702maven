package com.scl.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class AV implements Writable {
    private DoubleWritable num;
    private DoubleWritable avg;

    public AV() {
        this.num = new DoubleWritable();
        this.avg = new DoubleWritable();
    }

    public AV(DoubleWritable num,DoubleWritable avg) {
        this.avg = avg;
        this.num = num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(num.get());
        out.writeDouble(avg.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.num.set(in.readDouble());
        this.avg.set(in.readDouble());
    }

    public DoubleWritable getNum() {
        return num;
    }

    public DoubleWritable getAvg() {
        return avg;
    }
}
