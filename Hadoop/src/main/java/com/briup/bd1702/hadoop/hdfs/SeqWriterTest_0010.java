package com.briup.bd1702.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SeqWriterTest_0010 extends Configured implements Tool {
    private static final String[] DATA={
      "One car com one car go",
      "Two car peng peng people die",
      "120 wu wa wu wa come"
    };

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path output = new Path(conf.get("output"));
        SequenceFile.Writer writer = null;

        try {
            //准备至少三个选项

            //1. 文件存储路径
            SequenceFile.Writer.Option option_1 = SequenceFile.Writer.file(output);
            //2. 指定键的数据类型
            SequenceFile.Writer.Option option_2 = SequenceFile.Writer.keyClass(IntWritable.class);
            //3. 指定值的数据类型
            SequenceFile.Writer.Option  option_3 = SequenceFile.Writer.valueClass(Text.class);

            //构建SequenceFile.Writer对象

            writer = SequenceFile.createWriter(conf, option_1, option_2, option_3);

            for (int i = 0; i < 100; i++) {
                writer.append(new IntWritable(i),new Text(DATA[i%DATA.length]));
            }
        } finally {
            if (writer != null) { writer.close(); }
        }



        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SeqWriterTest_0010(),args));
    }
}
