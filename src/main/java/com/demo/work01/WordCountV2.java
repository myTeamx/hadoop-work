package com.demo.work01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author affable
 * @description word count
 * @date 2020/4/15 15:44
 */
public class WordCountV2 {

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

    /**
     * 倒序比较器
     */
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        String inputPath = "src/main/resources/dev/input_demo";
        String tmpOutputPath = "src/main/resources/dev/tmp_output_demo";
        String tmpSortOutputPath = "src/main/resources/dev/tmp_sort_output_demo";
        String outputPath = "src/main/resources/dev/output_demo";

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word-count");
        job.setJarByClass(WordCountV2.class);
        job.setMapperClass(TokenizerMapper.class);
        // 使用默认的 HashPartition
        // 设置 combiner 提高程序性能
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 下一个排序任务以临时目录为输入目录
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));

        FileOutputFormat.setOutputPath(job, new Path(tmpOutputPath));

        if (job.waitForCompletion(true)) {
            // 第一个 mr 完成之后，继续执行下一个倒序排列的 mr
            Job sortJob = Job.getInstance(conf, "word-sort");
            sortJob.setJarByClass(WordCountV2.class);

            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            // hadoop 提供的 kv 交换的 mapper
            sortJob.setMapperClass(InverseMapper.class);
            // 倒序比较器
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            sortJob.setNumReduceTasks(1);

            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);

            // 下一个排序任务以临时目录为输入目录
            sortJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.setInputPaths(sortJob, new Path(tmpOutputPath));
            FileOutputFormat.setOutputPath(sortJob, new Path(tmpSortOutputPath));

            if (sortJob.waitForCompletion(true)) {
                // 排序完成以后，对 kv 反转
                Job reverseJob = Job.getInstance(conf, "word-reverse");
                reverseJob.setJarByClass(WordCountV2.class);

                reverseJob.setInputFormatClass(SequenceFileInputFormat.class);
                // hadoop 提供的 kv 交换的 mapper
                reverseJob.setMapperClass(InverseMapper.class);
                reverseJob.setNumReduceTasks(0);

                reverseJob.setOutputKeyClass(Text.class);
                reverseJob.setOutputValueClass(IntWritable.class);

                FileInputFormat.setInputPaths(reverseJob, new Path(tmpSortOutputPath));
                FileOutputFormat.setOutputPath(reverseJob, new Path(outputPath));

                System.exit(reverseJob.waitForCompletion(true) ? 0 : 1);
            }

        }

    }


}
