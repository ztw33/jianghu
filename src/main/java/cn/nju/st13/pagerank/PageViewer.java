package cn.nju.st13.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

class DecDoubleWritable extends DoubleWritable {
	@Override
	public int compareTo(DoubleWritable o) {
		return -super.compareTo(o);
	}

	public DecDoubleWritable(double val) {
		super(val);
	}

	public DecDoubleWritable() {
		super();
	}


	@Override
	public String toString() {
		return super.toString();
	}
}

public class PageViewer {
	public static class PageViewerMapper extends Mapper<Object, Text, DecDoubleWritable,Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] temp = value.toString().split("[\t]");
			String name = temp[0];
			String[] tokens = temp[1].split("[|]");
			double pr = Double.parseDouble(tokens[tokens.length - 1].split("[,]")[1]);
			context.write(new DecDoubleWritable(pr),new Text(name));
		}
	}

	public static class PageViewerReducer extends Reducer<DecDoubleWritable, Text, Text, Text> {
		@Override
		public void reduce(DecDoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val : values) {
				context.write(val, new Text(key.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "2019st13 PR viewer");
		job.setJarByClass(PageViewer.class);
		job.setMapperClass(PageViewerMapper.class);
		job.setReducerClass(PageViewerReducer.class);
		job.setMapOutputKeyClass(DecDoubleWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("PR viewer done");
	}
}
