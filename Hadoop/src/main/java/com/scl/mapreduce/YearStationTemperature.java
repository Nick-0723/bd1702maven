package com.scl.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;


public class YearStationTemperature extends Configured implements Tool {

    public static class YearMapper extends Mapper<LongWritable,Text,YearStation,DoubleWritable>{
        private WeatherParser parser = new WeatherParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if(parser.isValid()) {
                context.write(
                        new YearStation(Integer.parseInt(parser.getYear().toString()),
                                Long.parseLong(parser.getStationId().toString())),
                        parser.getTemperature());
                parser.setValid(false);
            }
        }
    }

    public static class Comb extends Reducer<YearStation,DoubleWritable,YearStation,DoubleWritable>{
        private DoubleWritable max;
        @Override
        protected void reduce(YearStation key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            max = new DoubleWritable(0);
            values.forEach(a->max=max.compareTo(a)>0?max:a);
            context.write(key,max);
        }
    }

    public static class YearReducer extends Reducer<YearStation,DoubleWritable,Text,DoubleWritable>{
        private DoubleWritable max;
        @Override
        protected void reduce(YearStation key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            max = new DoubleWritable(0);
            values.forEach(a->max=max.compareTo(a)>0?max:a);
            context.write(new Text(key.toString()),max);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String path = "hdfs://172.16.0.101:9000/data/weather/999999-99999-199";
//        Path input = new Path("hdfs://172.16.0.4:9000/data/weather");
        Path output = new Path("hdfs://172.16.0.4:9000/user/nick/res_"+this.getClass().getSimpleName());
        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(YearMapper.class);
        job.setMapOutputKeyClass(YearStation.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(Comb.class);

        job.setReducerClass(YearReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPaths(job,path+"0,"+path+"1,"+path+"2,"+path+"3,"+path+"4");
//        TextInputFormat.addInputPaths(job,path+"2");
//        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        InputSampler.Sampler<LongWritable,Text> sampler = new InputSampler.RandomSampler<>(0.8,1000,2);
        InputSampler.writePartitionFile(job,sampler);
        URI uri = new URI(TotalOrderPartitioner.getPartitionFile(conf));
        job.addCacheFile(uri);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new YearStationTemperature(),args));
    }
}
