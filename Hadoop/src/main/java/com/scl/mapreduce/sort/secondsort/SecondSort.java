package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SecondSort extends Configured implements Tool {

    private static class DemoMapper extends Mapper<Text,DoubleWritable,YearTemperature,Text> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            String[] year_station = key.toString().split("\t");
            YearTemperature yearTemperature = new YearTemperature(Integer.parseInt(year_station[0]),value.get());
            context.write(yearTemperature,new Text(year_station[1]));
        }
    }

    private static class DemoReducer extends Reducer<YearTemperature,Text,Text,Text> {
        @Override
        protected void reduce(YearTemperature key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values){
                context.write(new Text(key.toString()),value);
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inputPath = "hdfs://172.16.0.4:9000/user/nick/res_YearStationTemperature_seq";
        String outputPath = "hdfs://172.16.0.4:9000/user/nick/res_"+this.getClass().getSimpleName();
        Path input = new Path(inputPath);
        Path output = new Path(outputPath);

        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(DemoMapper.class);
        job.setMapOutputKeyClass(YearTemperature.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DemoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);


        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupComparator.class);
        job.setSortComparatorClass(MySortComparator.class);


        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SecondSort(),args));
    }
}
