package com.scl.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("all")
public class YearStation implements WritableComparable<YearStation> {
    private IntWritable year;
    private LongWritable station;

    YearStation(){
        this.station = new LongWritable();
        this.year = new IntWritable();
    }

    public YearStation(int year, long station) {
        this.station = new LongWritable(station);
        this.year = new IntWritable(year);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year.get());
        out.writeLong(this.station.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year.set(in.readInt());
        this.station.set(in.readLong());
    }

    public IntWritable getYear() {
        return year;
    }

    public LongWritable getStation() {
        return station;
    }

    @Override
    public String toString() {
        return year + "\t" + station;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YearStation that = (YearStation) o;

        if (getYear() != null ? !getYear().equals(that.getYear()) : that.getYear() != null) return false;
        if (getStation() != null ? !getStation().equals(that.getStation()) : that.getStation() != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getYear() != null ? getYear().hashCode() : 0;
        result = 31 * result + (getStation() != null ? getStation().hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(YearStation o) {
        if(o==null)return 1;
        if(this.year.compareTo(o.year)==0)return this.station.compareTo(o.station);
        else return this.year.compareTo(o.year);
    }
}
