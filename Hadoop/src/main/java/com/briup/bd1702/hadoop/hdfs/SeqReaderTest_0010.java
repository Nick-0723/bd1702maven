package com.briup.bd1702.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SeqReaderTest_0010 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path input = new Path("hdfs://10.10.2.80:9000/user/hadoop/test1.seq");

        //创建读取器的选项 Option的对象
        SequenceFile.Reader.Option option_1 = SequenceFile.Reader.file(input);
        //创建读取器Reader对象
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, option_1);
        Object key = reader.getKeyClass().newInstance();
        Object value = reader.getValueClass().newInstance();
        //开始读取数据
        while (reader.next((Writable) key,(Writable)value)){
            // 判断当前的Record是否有同步标记 如果有打印* 如果没有打印空格
//            String syncStr = reader.syncSeen() ? "*" : " ";
//            System.out.println(syncStr+" : "+key);

            reader.sync(reader.getPosition());
            String syncStr = reader.syncSeen() ? "*" : " ";
            System.out.println(syncStr+" : "+key+" : "+ new String(((BytesWritable)value).copyBytes()));
            System.out.println(key.toString().equals(new String(((BytesWritable)value).copyBytes())));
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SeqReaderTest_0010(),args));
    }
}
