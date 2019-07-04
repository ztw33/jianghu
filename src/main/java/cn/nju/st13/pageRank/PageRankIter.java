package cn.nju.st13.pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PageRankIter {
	public static class PageRankIterMapper extends Mapper<Object, Text, Text, PageRankElement> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] keyValue = value.toString().split("[\t]");
			String nodeName = keyValue[0];
			String content = keyValue[1];
			String[] tokens = content.split("[|]");
			int length = tokens.length;
			StringBuilder neighbours = new StringBuilder("");
			for (int i = 0; i < length - 1; i++) {
				neighbours.append(tokens[i]);
				neighbours.append('|');
			}
			//传递图结构
			context.write(new Text(nodeName), new PageRankElement(neighbours.toString()));
			//分发PR值
			double prValue = Double.parseDouble(tokens[length - 1].split("[,]")[1]);
			for (int i = 0; i < length - 1; i++) {
				String[] nameAndWeight = tokens[i].split("[,]");
				String neighbourName = nameAndWeight[0];
				double weight = Double.parseDouble(nameAndWeight[1]);
				context.write(new Text(neighbourName), new PageRankElement(weight * prValue));
			}

		}
	}

	public static class PageRankIterReducer extends Reducer<Text, PageRankElement, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<PageRankElement> values, Context context) throws IOException, InterruptedException{
			String neighbours = null;
			double newPrValue = 0.0;
			double d = Double.parseDouble(context.getConfiguration().get("d"));
			double num = Double.parseDouble(context.getConfiguration().get("num"));
			for (PageRankElement element : values) {
				if (element.isNode()) {
					neighbours = element.getContent();
				} else {
					newPrValue += d * element.getPassValue();
				}
			}
			newPrValue += (1 - d) * (1 / num);
			context.write(key, new Text(neighbours + "PR," + Double.toString(newPrValue)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("d", "0.85");
		configuration.set("num", args[2]);
		Job job = Job.getInstance(configuration, "2019st13 PRIter");
		job.setJarByClass(PageRankIter.class);
		job.setMapperClass(PageRankIterMapper.class);
		job.setReducerClass(PageRankIterReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PageRankElement.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("Iterating...");

	}
}
