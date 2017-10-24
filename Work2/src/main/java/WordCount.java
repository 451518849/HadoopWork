import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        // 类似java中的基本数据封装
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            //相当于split函数，默认的分隔符是空格、制表符、换行符、回车符
            StringTokenizer itr = new StringTokenizer(value.toString());

            System.out.println("==============="+value.toString());

            while (itr.hasMoreTokens()) {

                //copy a text
                word.set(itr.nextToken());

                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        //(key,values),values已经对相同的key，进行了次数累加
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;

            System.out.println("+++++++"+values.toString());

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/Users/wangxiaofa/Desktop/Hadoop/src/input/data"));
        FileInputFormat.addInputPath(job, new Path("/Users/wangxiaofa/Desktop/Hadoop/src/input/data1"));

        FileOutputFormat.setOutputPath(job, new Path("/Users/wangxiaofa/Desktop/Hadoop/src/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
