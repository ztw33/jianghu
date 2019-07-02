package cn.nju.st13;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordSeg {
    public static class WordSegMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获得名字列表
            String nameListStr = context.getConfiguration().get("NameList");
            List<String> nameList = Arrays.asList(nameListStr.split(" "));

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

            // 除去无人名以及只有一个人名的段落
            if (resultName.size() > 1) {
                StringBuilder sb = new StringBuilder();
                for (String s : resultName)
                {
                    sb.append(s);
                    sb.append(" ");
                }
                word.set(sb.toString());
                context.write(word, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 3){
            System.err.println("必须输入读取文件路径、人名列表和输出路径");
            System.exit(2);
        }

        // 读入人名列表，加入自定义字典
        ArrayList<String> nameList = new ArrayList<>();
        FileSystem fs= FileSystem.get(conf);
        FSDataInputStream fin = fs.open(new Path(args[1]));
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
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
