package com.demo.work01;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author affable
 * @description word count
 * @date 2020/4/15 15:44
 */
public class WordCount {

    /**
     * 用来存放每一个分割出的字符
     * 定义为全局变量，减少创建对象次数
     * 提高程序性能
     */
    private static Text word = new Text();

    /**
     * 分词后，从 map 端写出到环形缓冲区
     * 每个词都是 1
     */
    private static final IntWritable ONE = new IntWritable(1);

    /**
     * Mapper<Object, Text, Text, IntWritable>
     *     输入 key value | 输出 key value 类型
     *
     * 切分字符串，并写出
     *
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 根据默认分割符，切割每一行字符串
            StringTokenizer iterator = new StringTokenizer(value.toString());
            while (iterator.hasMoreTokens()) {
                word.set(iterator.nextToken());
                context.write(word, ONE);
            }

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    /**
     * 根据单词聚合
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * 最后计算得出的结果
         */
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Preconditions.checkArgument(args.length >= 2, "缺少参数!");

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word-count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        // 使用默认的 HashPartition
        // 设置 combiner 提高程序性能
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
