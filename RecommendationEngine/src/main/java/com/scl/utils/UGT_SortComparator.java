package com.scl.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UGT_SortComparator extends WritableComparator {

    UGT_SortComparator(){
        super(UserGoodsTag.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UserGoodsTag y1 = (UserGoodsTag)a;
        UserGoodsTag y2 = (UserGoodsTag)b;
        return y1.compareTo(y2);
    }
}
