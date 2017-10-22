package com.briup.bd1702.hadoop.mapreduce;

import org.apache.hadoop.io.Text;

@SuppressWarnings("all")
public class PatentRecordParser {
    //表示数据中的第一列
    private String patentId;
    //表示数据中的第二列
    private String refPatentId;
    //表示解析的当前行的数据是否有效
    private boolean valid;

    public void parse(String line){
        String[] strs = line.split("[,]");
        if(strs.length==2){
            patentId = strs[0].trim();
            refPatentId = strs[1].trim();
            if(patentId.length()>0 && refPatentId.length()>0){
                valid = true;
            }
        }
    }

    public void parse(Text text){
        parse(text.toString());
    }

    public String getPatentId() {
        return patentId;
    }

    public String getRefPatentId() {
        return refPatentId;
    }

    public boolean isValid() {
        return valid;
    }
}
