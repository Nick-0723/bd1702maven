package com.scl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

class VectorSum {
    static Path input = GoodsSimilarityMatrixByGoodsVector.output;
    static Path output = new Path("hdfs://1.1.1.5:9000/user/nick/recommendation_engine/res_"+VectorSum.class.getSimpleName());

    static class DemoMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            context.write(new Text(strs[0]),new IntWritable(Integer.parseInt(strs[1])));
        }
    }

    static class DemoCombi extends Reducer<Text,IntWritable,Text,IntWritable> {
        int sum;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            values.forEach(a->sum+=a.get());
            context.write(key,new IntWritable(sum));
        }
    }

    static class DemoReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        int sum;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            values.forEach(a->sum+=a.get());
            context.write(key,new IntWritable(sum));
        }
    }

}
