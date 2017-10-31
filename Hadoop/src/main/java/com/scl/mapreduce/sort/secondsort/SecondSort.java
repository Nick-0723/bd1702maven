package com.scl.mapreduce.sort.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import java.net.URI;

public class SecondSort extends Configured implements Tool {

    private static class DemoMapper extends Mapper<LongWritable,Text,OneThree,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] one_three = value.toString().split("\t");
            OneThree oneThree = new OneThree(Integer.parseInt(one_three[0]),Integer.parseInt(one_three[2]));
            context.write(oneThree,new Text(one_three[1]));
        }
    }

    private static class DemoReducer extends Reducer<OneThree,Text,Text,Text> {
        @Override
        protected void reduce(OneThree key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("for循环外面的: "+key);
            for (Text value : values){
                System.out.println("for循环里面的: "+key);
                context.write(new Text(key.toString()),value);
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();


        String inputPath = "/user/nick/data.txt";
        String outputPath = "/user/nick/res_"+this.getClass().getSimpleName();

        // 创建文件系统
        FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
        // 如果输出目录存在，我们就删除
        if (fileSystem.exists(new Path(outputPath))) {
            fileSystem.delete(new Path(outputPath), true);
        }

        Path input = new Path(inputPath);
        Path output = new Path(outputPath);

        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(DemoMapper.class);
        job.setMapOutputKeyClass(OneThree.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DemoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,input);
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
