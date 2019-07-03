package cn.nju.st13.pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


enum Attribute {
	numOfLines
}

//使用MapReduce任务计算一共有多少个人物
class LineCounter {
	public static class CounterMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) {
			context.getCounter(Attribute.numOfLines).increment(1);
		}
	}
}

public class PageRankDirver {
	public static void main(String[] args) throws Exception {
		//计算一共有多少个人物
		Configuration configuration1 = new Configuration();
		Job countJob = Job.getInstance(configuration1, "2019st13 PR count");
		countJob.setJarByClass(PageRankDirver.class);
		countJob.setMapperClass(LineCounter.CounterMapper.class);
		countJob.setInputFormatClass(TextInputFormat.class);
		countJob.setOutputFormatClass(NullOutputFormat.class);
		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(countJob, new Path(args[0]));
		//FileOutputFormat.setOutputPath(countJob, new Path(args[1] + "_temp"));
		countJob.waitForCompletion(true);
		Counter counter = countJob.getCounters().findCounter(Attribute.numOfLines);
		System.out.println("Num of Lines: " + counter.getValue());

		//初始化图结构，初始化PR值
		String[] args2 = new String[3];
		args2[0] = args[0];
		args2[1] = args[1] + "_temp";
		args2[2] = Long.toString(counter.getValue());
		PageRankInitializer.main(args2);

		//循环
		String[] args3 = new String[3];
		args3[0] = args2[1];
		args3[1] = args[1] + "_temp2";
		args3[2] = args2[2];
		PageRankIter.main(args3);

	}
}
