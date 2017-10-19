package com.briup.bd1702.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.apache.hadoop.io.SequenceFile.Writer.*;

public class PutSequenceFile extends Configured implements Tool {

    private FileSystem local;
    Configuration conf;
    private SequenceFile.Writer writer = null;
    private String source = "G:/BD1702/bd1702maven/Hadoop/src";


    @Override
    public int run(String[] strings) throws Exception {
        conf = getConf();
//        Path sourcePath = new Path(conf.get("sourcePath"));

        Path sourcePath = new Path(source);

//        Path outputPath = new Path(conf.get("outputPath"));
        Path outputPath = new Path("hdfs://10.10.2.80:9000/user/hadoop/test1.seq");
        Option option_file = file(outputPath);
        Option option_keyType = keyClass(Text.class);
        Option option_valueType = valueClass(BytesWritable.class);

        writer = SequenceFile.createWriter(conf,option_file,option_keyType,option_valueType);

        local = FileSystem.getLocal(conf);
        FileStatus[] status = local.listStatus(sourcePath);
        try {
            for(FileStatus statu : status){
                process(statu);
            }
        } finally {
            writer.close();
        }


        return 0;
    }

    private void seqWriter(String key, byte[] value,boolean sync) throws IOException {
        key = key.split(source)[1];
        if(sync) {
            value = key.getBytes();
            writer.sync();
        }
        writer.append(new Text(key),new BytesWritable(value));
    }

    private void process(FileStatus status) throws Exception {
        InputStream in = null;
        byte[] b = new byte[10240];
        int len;
        String path = status.getPath().toString();
        try {
            if(status.isFile()){
                in = local.open(status.getPath());
                int off = 0;
                while ((len = in.read(b,off,10240)) != -1){
                    b = Arrays.copyOf(b, b.length * 2);
                    off = len;
                }
                seqWriter(path,b,false);
            } else if (status.isDirectory()){
                seqWriter(path,b,true);
                FileStatus[] temp = local.listStatus(status.getPath());
                for (FileStatus f : temp){
                    process(f);
                }
            }
        } finally {
            if(in != null) in.close();
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PutSequenceFile(),args));
    }
}
