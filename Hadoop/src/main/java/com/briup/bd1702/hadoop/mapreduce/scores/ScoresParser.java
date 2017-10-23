package com.briup.bd1702.hadoop.mapreduce.scores;

public class ScoresParser {
    private String course;
    private String name;
    private int score;
    private boolean valid;

    public String getCourse() {
        return course;
    }

    public String getName() {
        return name;
    }

    public int getScore() {
        return score;
    }

    public boolean isValid() {
        return valid;
    }

    public void parser(String line) {
        String[] strs = line.split("[|]");
        if(strs.length!=3)valid=false;
        else {
            valid = true;
            switch (strs[0].trim()) {
                case "a" : course = "语文";break;
                case "b" : course = "英语";break;
                case "c" : course = "数学";break;
                default:valid=false;
            }
            name = strs[1].trim();
            score = Integer.parseInt(strs[2].trim());
            if(score<0||score>100)valid=false;
        }
    }

}
