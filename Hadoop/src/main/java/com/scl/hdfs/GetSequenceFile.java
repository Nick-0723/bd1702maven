package com.scl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.hadoop.io.SequenceFile.Reader.*;

public class GetSequenceFile extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        Path p = new Path("hdfs://10.10.2.80:9000/user/hadoop/test1.seq");
        Option option = SequenceFile.Reader.file(p);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,option);
        Text key = (Text)reader.getKeyClass().newInstance();
        BytesWritable value = (BytesWritable)reader.getValueClass().newInstance();

        String outputPath = "G:/BD1702/bd1702maven/Hadoop/logs";
        while (reader.next(key,value)){

           if (reader.syncSeen() && key.toString().equals(new String(value.copyBytes())))
               Files.createDirectories(Paths.get(outputPath +key.toString()));
           else
               Files.write(Paths.get(outputPath +key.toString()),value.copyBytes());

        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GetSequenceFile(),args));
    }
}
