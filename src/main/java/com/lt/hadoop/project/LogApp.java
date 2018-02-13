package com.lt.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by taoshiliu on 2018/2/13.
 */
public class LogApp {

    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

        LongWritable one = new LongWritable(1);

        private UserAgentParser userAgentParser;

        //避免在map中多次初始化userAgentParser对象
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser  = new UserAgentParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String source = line.substring(getCharacterPosition(line,"\"",7) + 1);
            UserAgent agent = userAgentParser.parse(source);
            String browser = agent.getBrowser();

            context.write(new Text(browser),one);

        }

        //map完成后直接让gc回收掉，避免内存的负载
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
        }
    }

    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable value : values) {
                sum += value.get();
            }
            context.write(key,new LongWritable(sum));
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
        job.setJarByClass(LogApp.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

    private static int getCharacterPosition(String value,String operator,int index) {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while(slashMatcher.find()) {
            mIdx++;

            if(mIdx == index) {
                break;
            }
        }
        return slashMatcher.start();
    }

}
