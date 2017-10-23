package com.scl.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@SuppressWarnings("all")
public class WeatherParser {
    private Text stationId;
    private Text year;
    private DoubleWritable temperature;
    private boolean valid;

    public WeatherParser() {
        stationId = new Text();
        year = new Text();
        temperature = new DoubleWritable();
    }

    public WeatherParser(WeatherParser wp){
        stationId = new Text(wp.stationId);
        year = new Text(wp.year);
        temperature = new DoubleWritable(wp.temperature.get());
    }

    /*（0， 15）气象站编号
    （15，19）年份
    （87， 92) 检查到的温度，如果为+9999则表示没有检测到温度
    （92， 93）温度数据质量，为【01459】表示该温度是合理温度*/

    public void parse(String line){
        if(line.length()<94)return;
        stationId.set(line.substring(0,15));
        year.set(line.substring(15,19));
        String temp = line.substring(87,92);
        if(!temp.equals("+9999")){
            if("01459".contains(line.substring(92,93))){
                valid = true;
                temperature.set(Integer.parseInt(temp));
            }
        }
    }

    public Text getStationId() {
        return stationId;
    }

    public Text getYear() {
        return year;
    }

    public DoubleWritable getTemperature() {
        return temperature;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }
}
