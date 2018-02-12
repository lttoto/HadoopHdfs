package com.lt.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by taoshiliu on 2018/2/11.
 * input file
 * xiaomi 200
 * huawei 300
 * xiaomi 100
 * huawei 200
 * iphone7 300
 * iphone7 500
 * nokia 20
 */
public class PartitionerApp {

    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            context.write(new Text(words[0]),new LongWritable(Long.parseLong(words[1])));

        }
    }

    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable value : values) {
                sum += value.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }

    public static class MyPartitioner extends Partitioner<Text,LongWritable> {

        @Override
        public int getPartition(Text text, LongWritable longWritable, int i) {

            if(text.toString().equals("xiaomi")) {
                return 0;
            }

            if(text.toString().equals("huawei")) {
                return 1;
            }

            if(text.toString().equals("iphone7")) {
                return 2;
            }

            return 3;
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath,true);
            System.out.println("output file exist,delete.");
        }

        Job job = Job.getInstance(configuration,"wordcount");
        job.setJarByClass(PartitionerApp.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //Partition指定Map输出的相同的Key归到同一个Reducer中执行
        job.setPartitionerClass(MyPartitioner.class);
        //setNumReduceTask表示将Reduce的结果放置在对应的file中
        job.setNumReduceTasks(4);

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);

        /*
        * Partitioner设置的结果是
        * 最终会有4个文件part-0000(xiaomi)，part-0001(huawei)，part-0002(iphone7)，part-0003(nokia)
        * */
    }

}
