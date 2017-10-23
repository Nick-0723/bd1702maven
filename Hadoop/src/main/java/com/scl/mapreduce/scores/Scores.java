package com.scl.mapreduce.scores;

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
import java.util.HashMap;
import java.util.Map;

public class Scores extends Configured implements Tool {

    private static class ScoMapper extends Mapper<LongWritable,Text,Text,Text> {
        private ScoresParser parser = new ScoresParser();
        private Text key = new Text();
        private Text value = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parser(value.toString());
            if(parser.isValid()){
                this.key.set(parser.getName());
                this.value.set(parser.getCourse()+":"+parser.getScore());
            }
            context.write(this.key,this.value);
        }
    }

    private static class ScoReducer extends Reducer<Text,Text,Text,Text> {
            private Map<String,Integer> map = new HashMap<>();
            private float avg;
            private Integer sum = 0;
            private Text value = new Text();
            StringBuilder sb = new StringBuilder();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            map.clear();
            sb = new StringBuilder();
            values.forEach(a->{
                String[] strs = a.toString().split("[:]");
                map.put(strs[0],Integer.parseInt(strs[1]));
                sb.append(strs[0]).append(":").append(strs[1]).append(" ");
            });
            map.forEach((s,i)->sum += i);
            avg = sum/3.0F;
            sb.append("总分:").append(sum).append(" 平均分:").append(avg);
            value.set(sb.toString());
            context.write(key,this.value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String path = "hdfs://1.1.1.5:9000/data/d_scores/";
        Path output = new Path("hdfs://1.1.1.5:9000/temp/res_"+this.getClass().getSimpleName()+"_nick");
        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(ScoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ScoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPaths(job,path+"chinese.txt,"+path+"english.txt,"+path+"math.txt");
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Scores(),args));
    }
}
