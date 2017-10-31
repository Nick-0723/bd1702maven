package com.scl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class GoodsVector {
    static Path output = new Path("hdfs://1.1.1.5:9000/user/nick/recommendation_engine/res_"+GoodsVector.class.getSimpleName());

    static class DemoMapper extends Mapper<LongWritable,Text,Text,Text> {
        private Text key = new Text();
        private Text value = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[ ]");
            this.key.set(strs[1].trim());
            this.value.set(strs[0].trim()+":"+strs[2].trim());
            //这里写出的格式 goods  user:num
            context.write(this.key,this.value);
        }

        //用UserVector的Combi和Reduce类
        //这里写出的格式 goods  user:num,user:num
    }
}
