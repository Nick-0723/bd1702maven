package com.scl.mapreduce.libimseti.cz;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

class RatParser {
    private Text userId;
    private Text profileId;
    private DoubleWritable rat;
    private boolean valid;

    RatParser(){
        userId = new Text();
        profileId = new Text();
        rat = new DoubleWritable();
    }

    void parser(String line) {
        String[] strs = line.split(",");
        if(strs.length!=3)valid=false;
        else {
            valid = true;
            userId.set(strs[0]);
            profileId.set(strs[1]);
            rat.set(Integer.parseInt(strs[2]));
        }

    }

    Text getUserId() {
        return userId;
    }

    Text getProfileId() {
        return profileId;
    }

    DoubleWritable getRat() {
        return rat;
    }

    boolean isValid() {
        return valid;
    }
}
