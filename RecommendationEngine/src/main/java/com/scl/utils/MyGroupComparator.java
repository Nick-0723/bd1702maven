package com.scl.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
    MyGroupComparator(){
        super(IdTag.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IdTag y1 = (IdTag)a;
        IdTag y2 = (IdTag)b;
        return y1.getId().get()-y2.getId().get();
    }
}
