package com.scl.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UGT_GroupComparator extends WritableComparator {
    UGT_GroupComparator(){
        super(UserGoodsTag.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UserGoodsTag y1 = (UserGoodsTag)a;
        UserGoodsTag y2 = (UserGoodsTag)b;
        return y1.getUser_goods().compareTo(y2.getUser_goods());
    }
}
