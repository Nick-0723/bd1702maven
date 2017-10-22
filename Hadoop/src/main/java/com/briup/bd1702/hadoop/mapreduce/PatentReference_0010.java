package com.briup.bd1702.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PatentReference_0010 extends Configured implements Tool {

    public static class PatentMapper extends Mapper<LongWritable, Text, Text,IntWritable>{

        private PatentRecordParser parser = new PatentRecordParser();
        private Text key = new Text();
        private IntWritable value = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if(parser.isValid()){
                this.key.set(parser.getRefPatentId());
                context.write(this.key,this.value);
            }
        }
    }


    public static class PatentReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
               int count = 0;
               for(IntWritable iw : values){
                   count += iw.get();
               }
               context.write(key,new IntWritable(count));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        //构建作业的输入输出路径
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));
        //构建作业配置
        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+":nick");
        //设置该作业所要执行的类
        job.setJarByClass(this.getClass());
        //设置自定义的Mapper类以及Map端数据输出的类型
        job.setMapperClass(PatentMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置自定义的Reducer类以及数据输出的数据类型
        job.setReducerClass(PatentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置读取最原始数据的格式信息,以及数据输出到HDFS集群中的格式信息
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置数据读入和输出的路径,到相关的Format类中
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        //提交作业
        return job.waitForCompletion(true)?0:1;// 控制台是否需要等待作业完成
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PatentReference_0010(),args));
    }
}
