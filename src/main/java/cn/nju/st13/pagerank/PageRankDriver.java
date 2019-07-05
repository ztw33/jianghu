package cn.nju.st13.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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


public class PageRankDriver {
	//添加两个可选参数
	//-max_iter=n(n为整数,默认为5)
	//-retain_process=true/false(是否保留中间结果，默认为false, 开启时，中间结果用于可视化分析)
	public static void main(String[] args) throws Exception {
		int MAX_ITERATION_TIMES = 5;
		boolean RETAIN_PROCESS = false;
		for(int i = 2;i < args.length;i++) {
			String parameter = args[i];
			String[] tokens = parameter.split("[=]");
			if(tokens.length==2) {
				if(tokens[0].equals("-max_iter")) {
					MAX_ITERATION_TIMES = Integer.parseInt(tokens[1]);
				}

				if(tokens[0].equals("-retain_process")) {
					if(tokens[1].equals("true")) {
						RETAIN_PROCESS = true;
					}
					if(tokens[1].equals("false")) {
						RETAIN_PROCESS = false;
					}
				}
			}
		}

		System.out.println("Set max_iter = "+MAX_ITERATION_TIMES);
		System.out.println("Set retain_process = " + RETAIN_PROCESS);

		//计算一共有多少个人物
		Configuration configuration1 = new Configuration();
		Job countJob = Job.getInstance(configuration1, "2019st13 PR count");
		countJob.setJarByClass(PageRankDriver.class);
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
		args2[1] = args[1] + "_iter0";
		args2[2] = Long.toString(counter.getValue());
		PageRankInitializer.main(args2);

		//循环
		//不保留中间结果
		if(!RETAIN_PROCESS) {
			String[] args3 = new String[3];
			args3[0] = args2[1];
			args3[1] = args[1] + "_iter1";
			args3[2] = args2[2];
			for(int i = 0;i < MAX_ITERATION_TIMES;i++) {
				PageRankIter.main(args3);
				FileSystem fs = new Path(args2[1]).getFileSystem(configuration1);
				if (fs.exists(new Path(args2[1]))) {
					fs.delete(new Path(args2[1]));
					fs.rename(new Path(args3[1]), new Path(args2[1]));
				}
			}
		}
		//保留每次的中间结果
		else {
			for(int i = 0;i < MAX_ITERATION_TIMES;i++) {
				String[] args3 = new String[3];
				args3[0] = args[1] + "_iter" + i;
				args3[1] = args[1] + "_iter" + (i + 1);
				args3[2] = args2[2];
				PageRankIter.main(args3);
			}
		}

		//整理结果，进行排序
		if(!RETAIN_PROCESS) {
			String [] args4 = new String[2];
			args4[0] = args2[1];
			args4[1] = args[1];
			PageViewer.main(args4);
		}
		else {
			String[] args4 = new String[2];
			args4[0] = args[1] + "_iter" + MAX_ITERATION_TIMES;
			args4[1] = args[1];
			PageViewer.main(args4);
		}


		//删除中间文件
		if(!RETAIN_PROCESS) {
			FileSystem fs = new Path(args2[1]).getFileSystem(configuration1);
			if(fs.exists(new Path(args2[1]))) {
				fs.delete(new Path(args2[1]), true);
			}
		}
	}
}
