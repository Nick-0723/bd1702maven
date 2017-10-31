package com.scl.mapreduce.db;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TemperatureDB implements DBWritable,WritableComparable<TemperatureDB> {
    private IntWritable year;
    private Text stationId;
    private IntWritable tempe;
    TemperatureDB(){
        year = new IntWritable();
        stationId = new Text();
        tempe = new IntWritable();
    }

    TemperatureDB(IntWritable year, Text stationId, IntWritable tempe){
        this.year.set(year.get());
        this.stationId.set(stationId);
        this.tempe.set(tempe.get());
    }

    TemperatureDB(int year, String stationId, int tempe) {
        this.year = new IntWritable(year);
        this.stationId = new Text(stationId);
        this.tempe = new IntWritable(tempe);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,year.get());
        statement.setString(2,stationId.toString());
        statement.setInt(3,tempe.get());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        year = new IntWritable(resultSet.getInt(1));
        stationId = new Text(resultSet.getString(2));
        tempe = new IntWritable(resultSet.getInt(3));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        stationId.write(out);
        tempe.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        stationId.readFields(in);
        tempe.readFields(in);
    }

    @Override
    public int compareTo(TemperatureDB o) {
        int yearComp=year.compareTo(o.year);
        int sidComp=stationId.compareTo(o.stationId);
        int tempeComp=tempe.compareTo(o.tempe);
        return yearComp!=0?yearComp:(sidComp!=0?sidComp:tempeComp);
    }

    @Override
    public String toString(){
        return year+"\t"+stationId+"\t"+tempe;
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    public Text getStationId() {
        return stationId;
    }

    public void setStationId(Text stationId) {
        this.stationId = stationId;
    }

    public IntWritable getTempe() {
        return tempe;
    }

    public void setTempe(IntWritable tempe) {
        this.tempe = tempe;
    }
}
