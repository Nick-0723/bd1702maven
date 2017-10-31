package com.scl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class GoodsSimilarityMatrix {
    static Path input = Cooccurrence.output;
    static Path output = new Path("hdfs://1.1.1.5:9000/user/nick/recommendation_engine/res_"+GoodsSimilarityMatrix.class.getSimpleName());

    static class DemoMapper extends Mapper<LongWritable,Text,Text,Text> {
        private Text key = new Text();
        private Text value = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            this.key.set(strs[0]);
            this.value.set(strs[1]+":"+strs[2]);
            //这里写出去的格式是 goods   goods:num
            context.write(this.key,this.value);
        }
    }

    static class DemoCombi extends Reducer<Text,Text,Text,Text> {
        StringBuilder sb;
        Text value = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            sb = new StringBuilder();
            values.forEach(a->sb.append(a.toString()).append(","));
            this.value.set(sb.toString().substring(0,sb.toString().length()-1));
            //这里写出去的格式是 goods   goods:num,goods:num
            context.write(key,this.value);
        }
    }

    static class DemoReducer extends Reducer<Text,Text,Text,Text> {
        StringBuilder sb;
        Text value = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            sb = new StringBuilder();
            values.forEach(a->sb.append(a.toString()).append(","));
            this.value.set(sb.toString().substring(0,sb.toString().length()-1));
            //这里写出去的格式是 goods   goods:num,goods:num
            context.write(key,this.value);
        }
    }

}
