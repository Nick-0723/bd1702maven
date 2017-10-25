package com.scl.mapreduce.libimseti.cz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ProfileList extends Configured implements Tool {

    private static class DemoMapper extends Mapper<LongWritable,Text,Text,Text> {
        private RatParser parser = new RatParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parser(value.toString());
            if (parser.isValid()){
                context.write(parser.getUserId(),parser.getProfileId());
            }
        }
    }

    private static class DemoCombi extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text v : values){
                sb.append(",").append(v.toString());
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    private static class DemoReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text v : values){
                sb.append(v.toString());
            }
            context.write(key,new Text(sb.toString()));
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inputPath = "hdfs://172.16.0.4:9000/data/libimseti/ratings.dat";
        String outputPath = "hdfs://172.16.0.4:9000/temp/res_"+this.getClass().getSimpleName();
        Path input = new Path(inputPath);
        Path output = new Path(outputPath);

        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(DemoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(DemoCombi.class);

        job.setReducerClass(DemoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ProfileList(),args));
    }
}
