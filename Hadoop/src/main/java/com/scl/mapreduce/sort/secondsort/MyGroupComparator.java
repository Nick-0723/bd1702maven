package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
    MyGroupComparator(){
        super(OneThree.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OneThree y1 = (OneThree)a;
        OneThree y2 = (OneThree)b;
        return y1.getOne().get()-y2.getOne().get();
    }
}
