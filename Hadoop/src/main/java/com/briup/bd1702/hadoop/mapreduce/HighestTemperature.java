package com.briup.bd1702.hadoop.mapreduce;

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


/*1.以/data/weather/999999-99999-1992数据，请计算出每个气象站检测到的最高气温*/
public class HighestTemperature extends Configured implements Tool {
    public static class WeatherMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private WeatherParser parser = new WeatherParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if(parser.isValid()){
                context.write(parser.getStationId(),new IntWritable(parser.getTemperature().get()));
                parser.setValid(false);
            }
        }
    }

    public static class WeatherReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable value = new IntWritable(0);
            for(IntWritable v : values){
                if(v.compareTo(value)>0) value = v;
            }
            value.set(value.get()/10);
            context.write(key,value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path input = new Path("hdfs://172.16.0.101:9000/data/weather/999999-99999-1992");
        Path output = new Path("hdfs://172.16.0.101:9000/temp/res_"+this.getClass().getSimpleName()+"_nick");
        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new HighestTemperature(),args));
    }
}
