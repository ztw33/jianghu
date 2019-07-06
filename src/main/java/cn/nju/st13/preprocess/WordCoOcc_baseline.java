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

public class WordCoOcc_baseline {
    public static class WordCoOccMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

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

            /*
             * baseline:会出现颠倒，采用逗号分隔的字符串作为key
             */
            for (int i = 0; i < nameList.size(); i++) {
                for (int j = 0; j < nameList.size(); j++) {
                    if (i != j){
                        word.set(nameList.get(i)+","+nameList.get(j));
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class WordCoOccReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    static void Task2Main(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2019st13 Word Co-Occurrence_baseline Job");
        job.setMapperClass(WordCoOcc_baseline.WordCoOccMapper.class);
        job.setCombinerClass(WordCoOcc_baseline.WordCoOccReducer.class);
        job.setReducerClass(WordCoOcc_baseline.WordCoOccReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJarByClass(WordCoOcc_baseline.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }
}
