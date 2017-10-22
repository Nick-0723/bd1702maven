package com.briup.bd1702.hadoop.mapreduce;

import org.apache.commons.collections.list.TreeList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


public class Friends extends Configured implements Tool {
    public static class FriendsMapper extends Mapper<LongWritable,Text,Text,Text>{
        private FriendsParser parser = new FriendsParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if(parser.isValid()){
                context.write(parser.getStu1(),parser.getStu2());
            }
        }
    }

    public static class FriendsReducer extends Reducer<Text,Text,NullWritable,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Text> list = new TreeSet<>();

            for(Text v : values){
                list.add(new Text(v+":"+key));
            }
            for (Text value : list){
                context.write(NullWritable.get(),value);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path input = new Path("hdfs://172.16.0.4:9000/data/test/dupdata/friends.txt");
        Path output = new Path("hdfs://172.16.0.4:9000/temp/res_"+this.getClass().getSimpleName()+"_nick");
        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(FriendsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FriendsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Friends(),args));
    }
}
