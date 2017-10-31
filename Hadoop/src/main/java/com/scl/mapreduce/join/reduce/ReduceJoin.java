package com.scl.mapreduce.join.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class ReduceJoin extends Configured implements Tool {

    private static class FMapper extends Mapper<LongWritable,Text,IdTag,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(",");
            context.write(new IdTag(Integer.parseInt(strs[0]),1),new Text(strs[1]+","+strs[2]));
        }
    }
    private static class SMapper extends Mapper<LongWritable,Text,IdTag,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(",");
            context.write(new IdTag(Integer.parseInt(strs[0]),2),new Text(strs[1]+","+strs[2]));
        }
    }

    private static class DemoReducer extends Reducer<IdTag,Text,IntWritable,Text> {
        @Override
        protected void reduce(IdTag key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            String f = iterator.next().toString();
            while (iterator.hasNext()){
                context.write(key.getId(),new Text(f+","+iterator.next()));
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inputPath = "hdfs://172.16.0.4:9000/data/join/";
        String outputPath = "hdfs://172.16.0.4:9000/user/nick/res_"+this.getClass().getSimpleName();
        Path output = new Path(outputPath);

        Path firstInput = new Path(inputPath+"artist.txt");
        Path secondInput = new Path(inputPath+"user_artist.txt");

        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapOutputKeyClass(IdTag.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DemoReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, firstInput,TextInputFormat.class,FMapper.class);
        MultipleInputs.addInputPath(job, secondInput,TextInputFormat.class,SMapper.class);
        TextOutputFormat.setOutputPath(job,output);

        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupComparator.class);
        job.setSortComparatorClass(MySortComparator.class);


        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReduceJoin(),args));
    }
}
