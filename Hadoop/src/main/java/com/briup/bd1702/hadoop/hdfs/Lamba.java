package com.briup.bd1702.hadoop.hdfs;


import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Lamba {
    public static void main(String[] args) throws FileNotFoundException {

        String input = "hdfs://1.1.1.5:9000/data/data.txt";
        Map<String, Integer> vocList = createVocList(input);
        vocList.forEach((n, v) -> {
            System.out.println(n + " : " + v);
        });
    }

    public static Map<String, Integer> createVocList(String path) {
        HashMap<String, Integer> reduce = null;
        BufferedReader br = null;
        try {
            Configuration con = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(path), con, "njhserver");
            FSDataInputStream fis = fs.open(new Path(path));
            br = new BufferedReader(new InputStreamReader(fis));
            List<String> list = new ArrayList<>();
            String content;
            while ((content = br.readLine()) != null) {
                list.add(content);
            }
            reduce = list.stream().reduce(new HashMap<String, Integer
                    >(), (map, str) -> {
                String[] fileSpart = str.split(",");
                if (fileSpart.length == 2) {
                    Integer integer = map.get(fileSpart[1]);
                    if (integer == null) {
                        map.put(fileSpart[1].trim(), 1);
                    } else {
                        integer += 1;
                        map.put(fileSpart[1].trim(), integer);
                    }
                }
                return map;
            }, (left, right) -> {
                left.putAll(right);
                return left;
            });
        } catch (Exception e) {
            br.close();
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return reduce;
        }
    }
}
