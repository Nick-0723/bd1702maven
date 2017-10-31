package com.scl.mapreduce.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DB2HDFS extends Configured implements Tool {

    private static class DemoMapper extends Mapper<LongWritable,TemperatureDB,IntWritable,Text> {
        @Override
        protected void map(LongWritable key, TemperatureDB value, Context context) throws IOException, InterruptedException {
            context.write(value.getYear(),new Text(value.getStationId()+"\t"+value.getTempe()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outputPath = "hdfs://172.16.0.4:9000/user/nick/res_"+this.getClass().getSimpleName();
        Path output = new Path(outputPath);


        Job job = Job.getInstance(conf,this.getClass().getSimpleName()+"_nick");
        job.setJarByClass(this.getClass());

        job.setMapperClass(DemoMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String driver="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://172.16.0.100:3306/hadoop?useSSL=true";
        String user="hadoop";
        String password="hadoop";
        DBConfiguration.configureDB(job.getConfiguration(),driver,url,user,password);

        String tableName="station_tbl";
        String whereCondition="year<1995";
        String orderByField="temperature";
        String[] fieldNames={"year","station","temperature"};
        DBInputFormat.setInput(job,TemperatureDB.class,
                tableName,whereCondition,orderByField,fieldNames);

        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DB2HDFS(),args));
    }
}
