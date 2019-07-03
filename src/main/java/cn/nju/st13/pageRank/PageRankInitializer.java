package cn.nju.st13.pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 * 对任务三输出进行预处理
 * 初始化所有节点的PR值为1/n
 * 测试正常
 * */

public class PageRankInitializer {

	public static class PageRankInitializerMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("[\t]");
			String nodeName = tokens[0];
			String content = tokens[1];
			Double pr =1 / Double.parseDouble(context.getConfiguration().get("num") );
			content = content + "|PR,"+pr;
			context.write(new Text(nodeName), new Text(content));
		}
	}

	public static class PageRankInitializerReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text text : values) {
				context.write(key, text);
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();
		configuration.set("num", args[2]);
		Job job = Job.getInstance(configuration,"2019st13 PRInitialize");
		job.setJarByClass(PageRankInitializer.class);
		job.setMapperClass(PageRankInitializerMapper.class);
		job.setReducerClass(PageRankInitializerReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("Initialization finished");
	}
}
