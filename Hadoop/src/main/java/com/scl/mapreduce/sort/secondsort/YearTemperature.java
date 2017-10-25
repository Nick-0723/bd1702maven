package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("all")
public class YearTemperature implements WritableComparable<YearTemperature> {
    private IntWritable year;
    private DoubleWritable tempe;

    YearTemperature(){
        this.tempe = new DoubleWritable();
        this.year = new IntWritable();
    }

    public YearTemperature(int year, double tempe) {
        this.tempe = new DoubleWritable(tempe);
        this.year = new IntWritable(year);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year.get());
        out.writeDouble(this.tempe.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year.set(in.readInt());
        this.tempe.set(in.readDouble());
    }

    public IntWritable getYear() {
        return year;
    }

    public DoubleWritable getTempe() {
        return tempe;
    }

    @Override
    public String toString() {
        return year + "\t" + tempe;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YearTemperature that = (YearTemperature) o;

        if (getYear() != null ? !getYear().equals(that.getYear()) : that.getYear() != null) return false;
        if (getTempe() != null ? !getTempe().equals(that.getTempe()) : that.getTempe() != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getYear() != null ? getYear().hashCode() : 0;
        result = 31 * result + (getTempe() != null ? getTempe().hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(YearTemperature o) {
        if(o==null)return 1;
        if(this.year.compareTo(o.year)==0)return this.tempe.compareTo(o.tempe);
        else return this.year.compareTo(o.year);
    }
}
