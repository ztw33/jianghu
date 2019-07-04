package cn.nju.st13;

import java.io.IOException;
import java.sql.Time;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class LabelProp {
    static class InitMapper extends Mapper <Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split(" |\t", 1);
            String node = content[0];
            String label = String.valueOf(node.hashCode());

            //emit node's label & edgeList
            context.write(new Text(node), new Text(label + " " + content[1]));
        }
    }

    static class InitReducer extends Reducer <Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text val : values) {
                //output to file
				context.write(key, val);
			}
		}
    }

    static class IterMapper extends Mapper <Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split(" |\t", 2);
            String node = content[0];
            String label = content[1];
            String[] edgeList = content[2].split(" ");

            //emit node's label to its neighbors
            for (String edge : edgeList) {
                String neighbor = edge.split(",")[0];
                context.write(new Text(neighbor), new Text("label " + node + "," + label));
            }

            //emit node's edgeList
            context.write(new Text(node), new Text("edgeList " + content[2]));
        }
    }

    static class IterReducer extends Reducer <Text, Text, Text, Text> {
        Random randGen = new Random();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> labelMap = new HashMap<String, Integer>(); //name -> label
            HashMap<Integer, Double> weightMap = new HashMap<Integer, Double>(); //label -> weight
            String[] edgeList = null;
            String edgeListStr = null;

            //resolve data, build labelMap & get edgeList
			for (Text val : values) {
                String[] content = val.toString().split(" ", 1);
				if (content[0].equals("label")) {
                    String neighbor = content[1].split(",")[0];
                    Integer neighborLabel = new Integer(content[1].split(",")[1]);
                    labelMap.put(neighbor, neighborLabel);
                }
                else if (content[0].equals("edgeList")) {
                    edgeList = content[1].split(" ");
                    edgeListStr = content[1];
                }
                else {
                    System.err.println("Undefined prefix: " + content[0]);
                }
            }
            
            assert(edgeList != null);

            //build weightMap
            int edgeNum = edgeList.length;
            int counter = 0;
            int brokenTie = randGen.nextInt(edgeNum);
            for (String edge : edgeList) {
                if (counter == brokenTie) continue;

                String neighbor = edge.split(",")[0];
                Double weight = new Double(edge.split(",")[1]);
                Integer neigborLabel = labelMap.get(neighbor);

                if (weightMap.containsKey(neigborLabel)) { // add
                    weightMap.put(neigborLabel, weightMap.get(neigborLabel) + weight);
                }
                else { // init
                    weightMap.put(neigborLabel, weight);
                }

                counter++;
            }

            //find maximum weight label
            Integer maxLabel = new Integer(-1);
            Double maxValue = new Double(-1);
            for (HashMap.Entry<Integer, Double> entry : weightMap.entrySet()) {
                if (entry.getValue() > maxValue) {
                    maxValue = entry.getValue();
                    maxLabel = entry.getKey();
                }
            }

            context.write(key, new Text(maxLabel.toString() + " " + edgeListStr));
		}
    }

    static void initialize(String inputPath, String outputPath) {
        Configuration configuration = new Configuration();

        Job initJob = null;
        try {
            initJob = Job.getInstance(configuration, "2019St13 LabelProp Initialization Job");
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        initJob.setJarByClass(LabelProp.class);
        initJob.setMapperClass(InitMapper.class);
        initJob.setReducerClass(InitReducer.class);
        initJob.setInputFormatClass(TextInputFormat.class);
        // initJob.setOutputKeyClass(Text.class);
        // initJob.setOutputValueClass(IntWritable.class);
        try {
            FileInputFormat.addInputPath(initJob, new Path(inputPath));
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        FileOutputFormat.setOutputPath(initJob, new Path(outputPath));
    }

    static void iterate(String inputPath, String outputPath) {
        Configuration configuration = new Configuration();

        Job iterJob = null;
        try {
            iterJob = Job.getInstance(configuration, "2019St13 LabelProp Iteration Job");
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        iterJob.setJarByClass(LabelProp.class);
        iterJob.setMapperClass(InitMapper.class);
        iterJob.setReducerClass(InitReducer.class);
        iterJob.setInputFormatClass(TextInputFormat.class);
        // iterJob.setOutputKeyClass(Text.class);
        // iterJob.setOutputValueClass(IntWritable.class);
        try {
            FileInputFormat.addInputPath(iterJob, new Path(inputPath));
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        FileOutputFormat.setOutputPath(iterJob, new Path(outputPath));
    }
    
    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];
        int epoch = 100;

        initialize(inputPath, outputPath + ".tempout");

        for (int i = 0; i < epoch; ++i) {
            iterate(outputPath + ".tempout", outputPath + ".tempout");
        }
    }
}