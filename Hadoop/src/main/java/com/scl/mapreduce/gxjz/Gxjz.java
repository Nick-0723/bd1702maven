package com.scl.mapreduce.gxjz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class Gxjz extends Configured implements Tool {
    public static class FriendsMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private GxjzParser parser = new GxjzParser();
        private Text key = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.paser(value.toString());
            String[] friends = parser.getFriends();
            for (int i = 0; i < friends.length; i++) {
                for (int j = i+1; j < friends.length; j++) {
                    this.key.set(friends[i].compareTo(friends[j])>0?friends[i]+","+friends[j]:friends[j]+","+friends[i]);
                    context.write(this.key,new IntWritable(1));
                }
            }

        }
    }

    public static class FriendsReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private int sum;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            values.forEach(a->sum += a.get());
            context.write(key,new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path input = new Path("hdfs://172.16.0.4:9000/temp/res_Friends_nick/part-r-00000");
        Path output = new Path("hdfs://172.16.0.4:9000/temp/res_"+this.getClass().getSimpleName()+"_nick");
        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(FriendsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(FriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setCombinerClass(FriendsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Gxjz(),args));
    }
}
