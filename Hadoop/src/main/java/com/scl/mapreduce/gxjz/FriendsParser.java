package com.scl.mapreduce.gxjz;

import org.apache.hadoop.io.Text;

public class FriendsParser {
    private Text stu1 = new Text();
    private Text stu2 = new Text();
    private boolean valid;

    public void parse(String line){
        String[] strs = line.split("[,]");
        if(strs.length!=2) valid=false;
        else {
            valid=true;
            stu1.set(strs[0].trim());
            stu2.set(strs[1].trim());
        }
    }

    public Text getStu1() {
        return stu1;
    }

    public Text getStu2() {
        return stu2;
    }

    public boolean isValid() {
        return valid;
    }
}
