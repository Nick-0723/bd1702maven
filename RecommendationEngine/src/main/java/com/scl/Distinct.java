package com.scl;

import com.scl.utils.UserGoodsTag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

class Distinct {
    static Path input = VectorSum.output;
    static Path output = new Path("hdfs://1.1.1.5:9000/user/nick/recommendation_engine/res_"+Distinct.class.getSimpleName());


    static class FMapper extends Mapper<LongWritable,Text,UserGoodsTag,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[ ]");
            System.out.println("first: "+strs[0].trim()+","+strs[1].trim()+new Text(strs[2]));
            context.write(new UserGoodsTag(strs[0].trim()+","+strs[1].trim(),1),new Text(strs[2]));
        }
    }
    static class SMapper extends Mapper<LongWritable,Text,UserGoodsTag,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            String[] s = strs[0].split(",");
            System.out.println("first: "+s[0]+","+s[1]+new Text(strs[1]));
            context.write(new UserGoodsTag(s[0]+","+s[1],2),new Text(strs[1]));
        }
    }

    static class DemoReducer extends Reducer<UserGoodsTag,Text,Text,IntWritable> {
        @Override
        protected void reduce(UserGoodsTag key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            String num = iterator.next().toString();
            System.out.println(num);
            if (!iterator.hasNext()){
                System.out.println(key.getUser_goods()+"\t"+new IntWritable(Integer.parseInt(num)));
                context.write(key.getUser_goods(),new IntWritable(Integer.parseInt(num)));
            }
        }
    }

}
