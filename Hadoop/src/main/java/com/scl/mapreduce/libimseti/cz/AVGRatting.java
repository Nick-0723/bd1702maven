package com.scl.mapreduce.libimseti.cz;

import com.scl.mapreduce.AV;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AVGRatting extends Configured implements Tool {

    private static class DemoMapper extends Mapper<LongWritable,Text,Text,AV> {
        private RatParser parser = new RatParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parser(value.toString());
            if (parser.isValid()){
                context.write(parser.getUserId(),new AV(parser.getRat(),new DoubleWritable(1)));
            }
        }
    }

    private static class DemoCombi extends Reducer<Text,AV,Text,AV> {
        @Override
        protected void reduce(Text key, Iterable<AV> values, Context context) throws IOException, InterruptedException {
            double num = 0;
            double sum = 0;
            for (AV av : values){
                num += av.getNum().get();
                sum += av.getAvg().get()*av.getNum().get();
            }
            context.write(key,new AV(new DoubleWritable(sum/num),new DoubleWritable(sum)));
        }
    }

    private static class DemoReducer extends Reducer<Text,AV,Text,DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<AV> values, Context context) throws IOException, InterruptedException {
            double num = 0;
            double sum = 0;
            for (AV av : values){
                num += av.getNum().get();
                sum += av.getAvg().get()*av.getNum().get();
            }
            context.write(key,new DoubleWritable(sum/num));
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
        job.setMapOutputValueClass(AV.class);

        job.setCombinerClass(DemoCombi.class);

        job.setReducerClass(DemoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AVGRatting(),args));
    }
}
