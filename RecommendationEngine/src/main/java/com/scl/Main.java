package com.scl;

import com.scl.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class Main extends Configured implements Tool {

    private Configuration conf;
    @Override
    public int run(String[] args) throws Exception {
        Path source = new Path("hdfs://1.1.1.5:9000/user/nick/recommendation_engine/sourceData.txt");
         conf = getConf();

        Class<? extends Main> aClass = this.getClass();

        //配置作业1 用户向量UserVector
        Job job1 = Job.getInstance(conf, "job_UserVector");
        job1.setJarByClass(aClass);
        job1.setMapperClass(UserVector.DemoMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(UserVector.DemoReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job1, source);
        check(UserVector.output);
        TextOutputFormat.setOutputPath(job1, UserVector.output);

        //配置作业2 物品向量GoodsVector
        Job job2 = Job.getInstance(conf, "job_GoodsVector");
        job2.setJarByClass(aClass);
        job2.setMapperClass(GoodsVector.DemoMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(UserVector.DemoReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job2, source);
        check(GoodsVector.output);
        TextOutputFormat.setOutputPath(job2, GoodsVector.output);

        //配置作业3 共现次数Cooccurrence
        Job job3 = Job.getInstance(conf, "job_Cooccurrence");
        job3.setJarByClass(aClass);
        job3.setMapperClass(Cooccurrence.DemoMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setReducerClass(Cooccurrence.DemoReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job3, Cooccurrence.input);
        check(Cooccurrence.output);
        TextOutputFormat.setOutputPath(job3, Cooccurrence.output);

        //配置作业4 物品相似度矩阵GoodsSimilarityMatrix
        Job job4 = Job.getInstance(conf, "job_GoodsSimilarityMatrix");
        job4.setJarByClass(aClass);
        job4.setMapperClass(GoodsSimilarityMatrix.DemoMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setReducerClass(GoodsSimilarityMatrix.DemoReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job4, GoodsSimilarityMatrix.input);
        check(GoodsSimilarityMatrix.output);
        TextOutputFormat.setOutputPath(job4, GoodsSimilarityMatrix.output);

        //配置作业5 物品相似度矩阵乘物品向量GoodsSimilarityMatrixByGoodsVector
        Job job5 = Job.getInstance(conf, "job_GoodsSimilarityMatrixByGoodsVector");
        job5.setJarByClass(aClass);
        job5.setMapOutputKeyClass(IdTag.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setReducerClass(GoodsSimilarityMatrixByGoodsVector.DemoReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job5, GoodsSimilarityMatrixByGoodsVector.input1, TextInputFormat.class, GoodsSimilarityMatrixByGoodsVector.FMapper.class);
        MultipleInputs.addInputPath(job5, GoodsSimilarityMatrixByGoodsVector.input2, TextInputFormat.class, GoodsSimilarityMatrixByGoodsVector.SMapper.class);
        check(GoodsSimilarityMatrixByGoodsVector.output);
        TextOutputFormat.setOutputPath(job5, GoodsSimilarityMatrixByGoodsVector.output);
        job5.setPartitionerClass(MyPartitioner.class);
        job5.setGroupingComparatorClass(MyGroupComparator.class);
        job5.setSortComparatorClass(MySortComparator.class);

        //配置作业6 推荐向量分组求和VectorSum
        Job job6 = Job.getInstance(conf, "job_VectorSum");
        job6.setJarByClass(aClass);
        job6.setMapperClass(VectorSum.DemoMapper.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);
        job6.setReducerClass(VectorSum.DemoReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        job6.setInputFormatClass(TextInputFormat.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job6, VectorSum.input);
        check(VectorSum.output);
        TextOutputFormat.setOutputPath(job6, VectorSum.output);

        //配置作业6 物品相似度矩阵乘物品向量GoodsSimilarityMatrixByGoodsVector
        Job job7 = Job.getInstance(conf, "job_Distinct");
        job7.setJarByClass(aClass);
        job7.setMapOutputKeyClass(UserGoodsTag.class);
        job7.setMapOutputValueClass(Text.class);
        job7.setReducerClass(Distinct.DemoReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(IntWritable.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job7, source, TextInputFormat.class, Distinct.FMapper.class);
        MultipleInputs.addInputPath(job7, Distinct.input, TextInputFormat.class, Distinct.SMapper.class);
        check(Distinct.output);
        TextOutputFormat.setOutputPath(job7, Distinct.output);
        job7.setGroupingComparatorClass(UGT_GroupComparator.class);
        job7.setSortComparatorClass(UGT_SortComparator.class);


        Job[] jobs = new Job[]{job1, job2, job3, job4, job5, job6,job7};
        ControlledJob[] controlledJobs = new ControlledJob[7];
        for (int i = 0; i < 7; i++) {
            controlledJobs[i] = new ControlledJob(jobs[i].getConfiguration());
            controlledJobs[i].setJob(jobs[i]);
        }

        controlledJobs[2].addDependingJob(controlledJobs[0]);
        controlledJobs[3].addDependingJob(controlledJobs[2]);
        controlledJobs[4].addDependingJob(controlledJobs[1]);
        controlledJobs[4].addDependingJob(controlledJobs[3]);
        controlledJobs[5].addDependingJob(controlledJobs[4]);
        controlledJobs[6].addDependingJob(controlledJobs[5]);

        JobControl jobControl = new JobControl("RecommendationEngine");

        for (ControlledJob controlledJob : controlledJobs) {
            jobControl.addJob(controlledJob);
        }

        new Thread(jobControl).start();

        while (true) {
            for (ControlledJob cj : jobControl.getRunningJobList()) {
                cj.getJob().monitorAndPrintJob();
            }
            if (jobControl.allFinished()) break;
        }
        return 0;
    }

    private void check(Path path) throws Exception{
        String outputPath = path.toString();
        // 创建文件系统
        FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
        // 如果输出目录存在，我们就删除
        if (fileSystem.exists(new Path(outputPath))) {
            fileSystem.delete(new Path(outputPath), true);
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Main(), args));
    }
}
