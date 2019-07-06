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
import java.util.Collections;

public class WordCoOcc {
    public static class WordCoOccMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

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
                for (int j = 0; j < nameList.size(); j++) {
                    if (i != j){
                        k.set(nameList.get(i));
                        v.set(nameList.get(j)+":1");
                        context.write(k, v);
                    }
                }
            }
        }
    }

    public static class WordCoOccCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> names = new ArrayList<>();
            ArrayList<Integer> nums = new ArrayList<>();
            for (Text val : values) {
                String[] p = val.toString().split(":");
                String name = p[0];
                Integer num = Integer.valueOf(p[1]);
                if (names.contains(name)) {
                    int index = names.indexOf(name);
                    nums.set(index, nums.get(index)+num);
                } else {
                    names.add(name);
                    nums.add(num);
                }
            }
            StringBuilder value = new StringBuilder();
            for (int i=0; i<names.size(); i++) {
                if (i > 0) {
                    value.append(",");
                }
                value.append(names.get(i)).append(":").append(nums.get(i));
            }
            context.write(key, new Text(value.toString()));
        }
    }

    public static class WordCoOccReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> names = new ArrayList<>();
            ArrayList<Integer> nums = new ArrayList<>();
            for (Text val : values) {
                String[] pairs = val.toString().split(",");
                for (String pair : pairs) {
                    String[] p = pair.split(":");
                    String name = p[0];
                    Integer num = Integer.valueOf(p[1]);
                    if (names.contains(name)) {
                        int index = names.indexOf(name);
                        nums.set(index, nums.get(index)+num);
                    } else {
                        names.add(name);
                        nums.add(num);
                    }
                }
            }
            for (int i=0; i<names.size(); i++) {
                result.set(nums.get(i));
                context.write(new Text(key.toString()+","+names.get(i)), result);
            }
        }
    }

    static void Task2Main(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2019st13 Word Co-Occurrence Job");
        job.setMapperClass(WordCoOcc.WordCoOccMapper.class);
        job.setCombinerClass(WordCoOcc.WordCoOccCombiner.class);
        job.setReducerClass(WordCoOcc.WordCoOccReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(WordCoOcc.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }
}
