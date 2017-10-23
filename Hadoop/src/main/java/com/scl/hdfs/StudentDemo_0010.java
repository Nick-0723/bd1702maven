package com.scl.hdfs;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StudentDemo_0010 {
    public static void main(String[] args) throws IOException {
        Student student = new Student();
        student.setId(new IntWritable(10));
        student.setName(new Text("nick"));
        student.setGender(false);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        student.write(dos);
        byte[] data = baos.toByteArray();
        System.out.println(data.length+":"+Arrays.toString(data));
    }
}

@SuppressWarnings("all")
class Student implements Writable {
    private IntWritable id;
    private Text name;
    private boolean gender;
    private List<Text> list = new ArrayList<>();

    Student(){
        id = new IntWritable();
        name = new Text();
    }

    Student(Student student){
        id = new IntWritable(student.id.get());
        name = new Text(student.name);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        name.write(dataOutput);
        new BooleanWritable(gender).write(dataOutput);
        //先把list集合的大小写进去
        new IntWritable(list.size()).write(dataOutput);
        for(Text t : list){
            t.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        name.readFields(dataInput);
        BooleanWritable bw = new BooleanWritable();
        bw.readFields(dataInput);
        gender = bw.get();
        //先清空list
        list.clear();
        //反序列化,先把list的大小反序列化出来
        IntWritable size = new IntWritable();
        size.readFields(dataInput);
        for (int i = 0; i < size.get(); i++) {
            Text text = new Text();
            text.readFields(dataInput);
            list.add(text);
        }
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public boolean isGender() {
        return gender;
    }

    public void setGender(boolean gender) {
        this.gender = gender;
    }

    public List<Text> getList() {
        return list;
    }

    public void setList(List<Text> list) {
        this.list = list;
    }
}
