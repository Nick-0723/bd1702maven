package com.scl.mapreduce.gxjz;

public class GxjzParser {
    private String[] friends;

    public String[] getFriends() {
        return friends;
    }

    public void paser(String line) {
        String[] strs = line.split("[,]");
        friends = new String[strs.length-1];
        for (int i = 0; i < strs.length; i++) {
            if(i == 0) continue;
            friends[i-1] = strs[i];
        }
    }
}
