package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySortComparator extends WritableComparator {

    MySortComparator(){
        super(OneThree.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OneThree y1 = (OneThree)a;
        OneThree y2 = (OneThree)b;
        if(y1.getOne().compareTo(y2.getOne())==0)return y2.getThree().compareTo(y1.getThree());
        else return y1.getOne().compareTo(y2.getOne());
    }
}
