package cn.nju.st13;

import cn.nju.st13.labelprop.LabelProp;
import cn.nju.st13.pagerank.PageRankDriver;
import cn.nju.st13.preprocess.Preprocess;

public class Main {
	public static void main(String[] args) throws Exception {
		String novelsPath = args[0];
		String peopleNamePath = args[1];
		String outputPath = args[2];

		//任务1-3
		String[] args1 = {novelsPath, peopleNamePath, outputPath, "-1"};
		Preprocess.main(args1);

		//任务4
		String[] args2 = {outputPath + "/output3", outputPath + "/output4", "-max_iter=15", "-retain_process=false"};
		PageRankDriver.main(args2);

		//任务5
		String[] args3 = {outputPath + "/output3", outputPath + "/output5"};
		LabelProp.main(args3);
	}
}
