package com.scl;

import com.scl.utils.IdTag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

class GoodsSimilarityMatrixByGoodsVector{
    static Path input1 = GoodsSimilarityMatrix.output;
    static Path input2 = GoodsVector.output;
    static Path output = new Path("hdfs://1.1.1.5:9000/user/nick/recommendation_engine/res_"+GoodsSimilarityMatrixByGoodsVector.class.getSimpleName());


    static class FMapper extends Mapper<LongWritable,Text,IdTag,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            context.write(new IdTag(Integer.parseInt(strs[0]),1),value);
        }
    }
    static class SMapper extends Mapper<LongWritable,Text,IdTag,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            context.write(new IdTag(Integer.parseInt(strs[0]),2),value);
        }
    }

    static class DemoReducer extends Reducer<IdTag,Text,Text,IntWritable> {
        @Override
        protected void reduce(IdTag key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            String goodsSimilarityMatrix = iterator.next().toString();
            String goodsVector = iterator.next().toString();
            String[] first = goodsSimilarityMatrix.split("\t")[1].split(",");
            String[] second = goodsVector.split("\t")[1].split(",");
            for (String aSecond : second) {
                String[] user_num = aSecond.split(":");
                int snum = Integer.parseInt(user_num[1]);
                for (String aFirst : first) {
                    String[] goods_num = aFirst.split(":");
                    int fnum = Integer.parseInt(goods_num[1]);
                    context.write(new Text(user_num[0] + "," + goods_num[0]), new IntWritable(fnum * snum));
                }
            }
        }
    }

}
