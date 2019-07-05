package cn.nju.st13.preprocess;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordSeg {
    public static class WordSegMapper extends Mapper<Object, Text, Text, NullWritable> {

        private MultipleOutputs mos;
        private List<String> nameList;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs(context);
            // 获得名字列表
            String nameListStr = context.getConfiguration().get("NameList");
            nameList = Arrays.asList(nameListStr.split(" "));
        }

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 分词
            String str = value.toString();
            Result result = ToAnalysis.parse(str);
            List<Term> terms = result.getTerms();

            // 获得列表中的名字
            ArrayList<String> resultName = new ArrayList<>();
            for (Term term : terms) {
                if (term.getNatureStr().equals("nr") && nameList.contains(term.getName())) {
                    resultName.add(term.getName());
                }
            }

            // 除去无人名的段落
            if (resultName.size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (String s : resultName)
                {
                    sb.append(s);
                    sb.append(" ");
                }
                word.set(sb.toString());
                //context.write(NullWritable.get(), word);
                FileSplit fileSplit = (FileSplit)context.getInputSplit();
                String fileName = fileSplit.getPath().getName();
                mos.write(word, NullWritable.get(), fileName+".segment");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }
    }

    static void Task1Main(Path inputPath, Path nameListPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();

        // 读入人名列表，加入自定义字典
        ArrayList<String> nameList = new ArrayList<>();
        FileSystem fs= nameListPath.getFileSystem(conf);
        FSDataInputStream fin = fs.open(nameListPath);
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(fin, StandardCharsets.UTF_8));
            while((line = br.readLine()) != null) {
                DicLibrary.insert(DicLibrary.DEFAULT, line, "nr", 1000);
                nameList.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                br.close();
            }
        }

        StringBuilder nameStr = new StringBuilder();
        for (String s : nameList)
        {
            nameStr.append(s);
            nameStr.append(" ");
        }

        // 共享名字列表变量
        conf.set("NameList", nameStr.toString());

        Job job = Job.getInstance(conf, "2019st13 Word Segmentation Job");
        job.setJarByClass(WordSeg.class);
        job.setMapperClass(WordSegMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.waitForCompletion(true);
    }
}
