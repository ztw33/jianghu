package cn.nju.st13.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class WordCoOcc_pair {

    public static class WordCoOccMapper extends Mapper<Object, Text, WordPair, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            String[] inputList = input.split(" ");

            /* 跳过只有一个人名的段落 */
            if (inputList.length < 2) {
                return;
            }

            /* 去掉重复出现的名字 */
            ArrayList<String> nameList = new ArrayList<>();
            for (String name : inputList) {
                if (!nameList.contains(name)) {
                    nameList.add(name);
                }
            }

            for (int i = 0; i < nameList.size(); i++) {
                for (int j = i+1; j < nameList.size(); j++) {
                    context.write(new WordPair(nameList.get(i), nameList.get(j)), one);
                }
            }
        }
    }

    public static class WordCoOccCombiner extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class WordCoOccReducer extends Reducer<WordPair, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(new Text(key.toString()), result);
        }
    }

    static void Task2Main(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2019st13 Word Co-Occurrence Job");
        job.setMapperClass(WordCoOcc_pair.WordCoOccMapper.class);
        job.setCombinerClass(WordCoOcc_pair.WordCoOccCombiner.class);
        job.setReducerClass(WordCoOcc_pair.WordCoOccReducer.class);
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJarByClass(WordCoOcc_pair.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }
}
