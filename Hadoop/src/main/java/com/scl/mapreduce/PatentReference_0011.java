package com.scl.mapreduce;

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
import java.util.ArrayList;
import java.util.List;

public class PatentReference_0011 extends Configured implements Tool {

    public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
        private PatentRecordParser parser = new PatentRecordParser();
        private Text key = new Text();
        private Text value = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if(parser.isValid()){
                this.key.set(parser.getPatentId());
                this.value.set(parser.getRefPatentId());
                context.write(this.key,this.value);
            }
        }
    }

    public static class MyReducer extends Reducer<Text,Text,Text,Text>{
        private Text key = new Text();
        private Text value = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            this.key = key;
            List<Text> list = new ArrayList<>();
            for(Text t : values){
                list.add(t);
            }
            this.value.set(list.toString());
            context.write(this.key,this.value);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();





        Path input = new Path("hdfs://172.16.0.101:9000/data/patent/cite75_99.txt");
        Path output = new Path("hdfs://172.16.0.101:9000/temp/res_1");
//        Path input = new Path(conf.get("input"));
//        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(MyMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PatentReference_0011(),args));
    }
}
