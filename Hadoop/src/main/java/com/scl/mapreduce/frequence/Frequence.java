package com.scl.mapreduce.frequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Frequence extends Configured implements Tool {

    private static class FreMapper extends Mapper<LongWritable,Text,Text,Text> {
        private Text key = new Text();
        private Text value = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("[ ]");
            for (String word : words){
                this.key.set(word.trim());
                context.write(this.key,this.value);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.value.set(((FileSplit)context.getInputSplit()).getPath().getName());
        }
    }


    private static class FreReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<>();
            values.forEach(fileName->{
                System.out.println(fileName);
                String s = fileName.toString();
                if(map.containsKey(s)) map.put(s,map.get(s)+1);
                else map.put(s,1);
            });
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<String,Integer> entry : map.entrySet()){
                sb.append(",").append(entry.getKey()).append(":").append(entry.getValue());
            }
            context.write(key,new Text(sb.substring(0, sb.length() - 1)));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String input = "hdfs://1.1.1.5:9000/data/b_frequence/";
        Path output = new Path("hdfs://1.1.1.5:9000/temp/res_"+this.getClass().getSimpleName()+"_debug_3");

        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(FreMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPaths(job,input+"file_1,"+input+"file_2,"+input+"file_3,"+input+"file_4");
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Frequence(),args));
    }
}
