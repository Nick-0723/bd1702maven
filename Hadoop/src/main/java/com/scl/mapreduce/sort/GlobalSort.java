package com.scl.mapreduce.sort;

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
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

public class GlobalSort extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inputPath = "hdfs://172.16.0.4:9000/user/nick/res_YearStationTemperature_seq";
        String outputPath = "hdfs://172.16.0.4:9000/user/nick/res_"+this.getClass().getSimpleName()+"_1";
        Path input = new Path(inputPath);
        Path output = new Path(outputPath);

        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

//        job.setMapperClass(DemoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

//        job.setCombinerClass(DemoCombi.class);

//        job.setReducerClass(DemoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        //重新设置分区规则
        job.setPartitionerClass(TotalOrderPartitioner.class);
        //构建采样器,从输入的键中采样,分析分布区间
        InputSampler.Sampler<Text,DoubleWritable> sampler = new InputSampler.RandomSampler<>(1,1000,5);
        //运行采样器,计算每个键对应的分布区间,并将计算结果保存
        InputSampler.writePartitionFile(job,sampler);
        //获得采样器分析的分区结果路径名
        URI uri = new URI(TotalOrderPartitioner.getPartitionFile(conf));

        //将分区结果分发到集群上的各个节点,Map任务
        job.addCacheFile(uri);



        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GlobalSort(),args));
    }
}
