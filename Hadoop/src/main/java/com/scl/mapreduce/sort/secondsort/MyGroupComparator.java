package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
    MyGroupComparator(){
        super(YearTemperature.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        YearTemperature y1 = (YearTemperature)a;
        YearTemperature y2 = (YearTemperature)b;
        return y1.getYear().get()-y2.getYear().get();
    }
}
