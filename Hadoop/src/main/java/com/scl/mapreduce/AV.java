package com.scl.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;
@SuppressWarnings("all")
public class AV implements Writable {
    private DoubleWritable num;
    private DoubleWritable avg;

    AV(){
        this.avg = new DoubleWritable();
        this.num = new DoubleWritable();
    }

    public AV(DoubleWritable avg,DoubleWritable num) {
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AV av = (AV) o;

        if (getNum() != null ? !getNum().equals(av.getNum()) : av.getNum() != null) return false;
        return getAvg() != null ? getAvg().equals(av.getAvg()) : av.getAvg() == null;
    }

    @Override
    public int hashCode() {
        int result = getNum() != null ? getNum().hashCode() : 0;
        result = 31 * result + (getAvg() != null ? getAvg().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return num + "\t" + avg;
    }
}
