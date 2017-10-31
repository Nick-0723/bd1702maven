package com.scl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class Cooccurrence {
    static Path input = UserVector.output;
    static Path output = new Path("hdfs://1.1.1.5:9000/user/nick/recommendation_engine/res_"+Cooccurrence.class.getSimpleName());

    static class DemoMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        private Text key = new Text();
        private IntWritable value = new IntWritable();
        /*  10001   20001:1,20005:1,20006:1,20007:1,20002:1
            10002   20006:1,20003:1,20004:1
            10003   20002:1,20007:1
            10004   20001:1,20002:1,20005:1,20006:1
            10005   20001:1
            10006   20004:1,20007:1
            */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t")[1].split(",");
            for (int i = 0; i < strs.length; i++){
                String[] goods_nums_i = strs[i].split(":");
                for (int j = 0; j < strs.length; j++) {
                    String[] goods_nums_j = strs[j].split(":");
                    this.key.set(goods_nums_i[0]+"\t"+goods_nums_j[0]);
                    this.value.set(1);
                    context.write(this.key,this.value);
                }
            }
        }
    }

    static class DemoReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private int sum;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            values.forEach(a->sum += a.get());
            context.write(key,new IntWritable(sum));
        }
    }
}
