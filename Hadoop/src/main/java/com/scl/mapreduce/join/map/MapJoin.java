package com.scl.mapreduce.join.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapJoin extends Configured implements Tool {

    private static class PrepareMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[,]");
            context.write(new Text(strs[0].trim()),new Text(strs[1].trim()+","+strs[2].trim()));
        }
    }

    private static class DemoMapper extends Mapper<Text,TupleWritable,Text,Text> {
        @Override
        protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            value.forEach(a->sb.append(a.toString()).append(","));
            String v = sb.substring(0, sb.length() - 1);
            context.write(key,new Text(v));
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inputPath = "hdfs://172.16.0.4:9000/data/join/";
        String outputPath = "hdfs://172.16.0.4:9000/user/nick/res_"+this.getClass().getSimpleName()+"_debug_3";
        Path output = new Path(outputPath);

        Path firstInput = new Path(inputPath+"user_artist.txt");
        Path secondInput = new Path(inputPath+"artist.txt");
        Path firstOutput = new Path(outputPath+"_first_debug_3");
        Path secondOutput = new Path(outputPath+"_second_debug_3");

        //配置第一个job
        Job job1 = Job.getInstance(conf,this.getClass().getSimpleName()+"_job1");
        job1.setJarByClass(this.getClass());
        job1.setMapperClass(PrepareMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job1,firstInput);
        TextOutputFormat.setOutputPath(job1,firstOutput);
        TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);

        //配置第二个job
        Job job2 = Job.getInstance(conf,this.getClass().getSimpleName()+"_job2");
        job2.setJarByClass(this.getClass());
        job2.setMapperClass(PrepareMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job2,secondInput);
        TextOutputFormat.setOutputPath(job2,secondOutput);
        TextOutputFormat.setOutputCompressorClass(job2, GzipCodec.class);

        //配置主要job
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","\t");
        //这里这两个路径的先后顺序影响读文件的顺序 也就影响TupleWritable数据values的顺序
        String compose = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, secondOutput, firstOutput);
        conf.set("mapreduce.join.expr",compose);
        Job job3 = Job.getInstance(conf,this.getClass().getSimpleName()+"_mainJob");
        job3.setJarByClass(this.getClass());
        job3.setMapperClass(DemoMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setInputFormatClass(CompositeInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3,firstOutput,secondOutput);
        TextOutputFormat.setOutputPath(job3,output);

        // 三个Job按照顺序提交
        List<Job> jobs=new ArrayList<>();
        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);

        int exitCode=0;
        for(Job job:jobs){
            boolean success=job.waitForCompletion(true);
            if(!success){
                System.out.println(
                        "Error with Job："+job.getJobName()
                                +"，"+
                                job.getStatus().getFailureInfo());
                exitCode=1;
                break;
            }
        }
        return exitCode;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MapJoin(),args));
    }
}
