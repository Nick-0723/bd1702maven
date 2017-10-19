package com.briup.bd1702.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Exercise extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {
        InputStream in = null;
        BufferedReader br = null;
        PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream("Hadoop/res.txt")));
        try {
            Map<String,Long> map = new HashMap<>();
            Configuration conf = getConf();
            String url = "hdfs://1.1.1.5:9000/data/data.txt";
            FileSystem fileSystem = FileSystem.get(URI.create(url), conf, "nick");
            in = fileSystem.open(new Path(url));
            br = new BufferedReader(new InputStreamReader(in));
            System.out.println("start resolve data");
            String s;
            while((s = br.readLine())!=null) {
                String data = s.split("[,]")[1];
                if(map.containsKey(data)) map.put(data,map.get(data)+1);
                else map.put(data,1L);
            }
            System.out.println("resolved data, writing in file");
            map.forEach((k,v)-> out.println(k+" : "+v));
            out.flush();
        } finally {
            if (in != null) { in.close(); }
            if (br != null) { br.close(); }
            out.close();
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Exercise(),args));
    }
}
