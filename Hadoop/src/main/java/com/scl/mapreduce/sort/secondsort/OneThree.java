package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("all")
public class OneThree implements WritableComparable<OneThree> {
    private IntWritable one;
    private DoubleWritable three;

    OneThree(){
        this.three = new DoubleWritable();
        this.one = new IntWritable();
    }

    public OneThree(int one, double three) {
        this.three = new DoubleWritable(three);
        this.one = new IntWritable(one);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.one.get());
        out.writeDouble(this.three.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.one.set(in.readInt());
        this.three.set(in.readDouble());
    }

    public IntWritable getOne() {
        return one;
    }

    public DoubleWritable getThree() {
        return three;
    }

    @Override
    public String toString() {
        return one + "\t" + three;
    }

    @Override
    public int compareTo(OneThree o) {
        if(o==null)return 1;
        if(this.one.compareTo(o.one)==0)return this.three.compareTo(o.three);
        else return this.one.compareTo(o.one);
    }
}
