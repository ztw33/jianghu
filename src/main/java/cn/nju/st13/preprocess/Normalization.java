package cn.nju.st13.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class Normalization {
    public static class NormMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            String names = input.split("\t")[0];
            String freq = input.split("\t")[1];
            String name1 = names.split(",")[0];
            String name2 = names.split(",")[1];

            // 不会出现颠倒的情况
            k.set(name1);
            v.set(name2+":"+freq);
            context.write(k, v);
            k.set(name2);
            v.set(name1+":"+freq);
            context.write(k, v);
        }
    }

    public static class NormReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            /*
             * 特别注意：Iterable变量不能循环访问两遍啊。。
             */
            ArrayList<String> valuesList = new ArrayList<>();
            for (Text val : values) {
                sum += Integer.parseInt(val.toString().split(":")[1]);
                valuesList.add(val.toString());
            }
            ArrayList<String> resultList = new ArrayList<>();
            for (String val : valuesList) {
                String[] temp = val.split(":");
                String name = temp[0];
                double weight = Double.valueOf(temp[1]) / sum;
                resultList.add(name + "," + String.format("%.6f", weight));
            }
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < resultList.size(); i++) {
                if (i > 0) {
                    result.append("|");
                }
                result.append(resultList.get(i));
            }
            Text v = new Text();
            v.set(result.toString());
            context.write(key, v);
        }
    }

    public static void Task3Main(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2019st13 Normalization Job");
        job.setMapperClass(Normalization.NormMapper.class);
        job.setReducerClass(Normalization.NormReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(Normalization.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }
}
