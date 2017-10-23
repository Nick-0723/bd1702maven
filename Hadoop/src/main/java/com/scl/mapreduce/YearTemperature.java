package com.scl.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


/*1.以/data/weather/999999-99999-1992数据，请计算出每个气象站检测到的平均气温*/
public class YearTemperature extends Configured implements Tool {
    public static class YearMapper extends Mapper<LongWritable,Text,Text,AV>{
        private WeatherParser parser = new WeatherParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if(parser.isValid()) {
                context.write(parser.getYear(),new AV(parser.getTemperature(),new DoubleWritable(1)));
                parser.setValid(false);
            }
        }
    }

    public static class Comb extends Reducer<Text,AV,Text,AV>{
        @Override
        protected void reduce(Text key, Iterable<AV> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            double avg = 0;
            for(AV av : values) {
                count += av.getNum().get();
                sum += av.getAvg().get();
            }
            avg = sum/count;
            context.write(key,new AV(new DoubleWritable(count),new DoubleWritable(avg)));
        }
    }

    public static class YearReducer extends Reducer<Text,AV,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<AV> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            double avg = 0;
            for(AV av : values) {
                count += av.getNum().get();
                sum += av.getAvg().get();
            }
            avg = sum/count;
            context.write(key,new DoubleWritable(avg));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String path = "hdfs://172.16.0.101:9000/data/weather/999999-99999-199";
        Path output = new Path("hdfs://172.16.0.4:9000/temp/res_"+this.getClass().getSimpleName()+"_nick_debug_5");
        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick_debug_5");
        job.setJarByClass(this.getClass());

        job.setMapperClass(YearMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AV.class);

        job.setReducerClass(YearReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(Comb.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPaths(job,path+"0,"+path+"1,"+path+"2,"+path+"3,"+path+"4");
//        TextInputFormat.addInputPaths(job,path+"2");

        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new YearTemperature(),args));
    }
}
