package cn.nju.st13;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

    static class ClusterMapper extends Mapper <Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split(" |\t", 2);
            String node = content[0];
            String label = content[1];

            context.write(new Text(label), new Text(node));
        }
    }

    static class ClusterReducer extends Reducer <Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
                context.write(val, key);
            }
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
        initJob.setNumReduceTasks(5);
        // initJob.setOutputKeyClass(Text.class);
        // initJob.setOutputValueClass(IntWritable.class);
        try {
            FileInputFormat.addInputPath(initJob, new Path(inputPath));
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        FileOutputFormat.setOutputPath(initJob, new Path(outputPath));

        try {
            initJob.waitForCompletion(true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
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
        iterJob.setMapperClass(IterMapper.class);
        iterJob.setReducerClass(IterReducer.class);
        iterJob.setInputFormatClass(TextInputFormat.class);
        iterJob.setNumReduceTasks(5);
        // iterJob.setOutputKeyClass(Text.class);
        // iterJob.setOutputValueClass(IntWritable.class);
        try {
            FileInputFormat.addInputPath(iterJob, new Path(inputPath));
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        FileOutputFormat.setOutputPath(iterJob, new Path(outputPath));

        try {
            iterJob.waitForCompletion(true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void cluster(String inputPath, String outputPath) {
        Configuration configuration = new Configuration();

        Job clusterJob = null;
        try {
            clusterJob = Job.getInstance(configuration, "2019St13 LabelProp Clustering Job");
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        clusterJob.setJarByClass(LabelProp.class);
        clusterJob.setMapperClass(InitMapper.class);
        clusterJob.setReducerClass(InitReducer.class);
        clusterJob.setInputFormatClass(TextInputFormat.class);
        // clusterJob.setOutputKeyClass(Text.class);
        // clusterJob.setOutputValueClass(IntWritable.class);
        try {
            FileInputFormat.addInputPath(clusterJob, new Path(inputPath));
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        FileOutputFormat.setOutputPath(clusterJob, new Path(outputPath));

        try {
            clusterJob.waitForCompletion(true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];
        int epoch = 100;

        initialize(inputPath, outputPath + ".tempout");

        for (int i = 0; i < epoch; ++i) {
            iterate(outputPath + ".tempout", outputPath + ".tempout");
        }

        cluster(outputPath + ".tempout", outputPath);

        try {
            FileSystem fs = new Path(args[1] + ".tempout").getFileSystem(new Configuration());
            if(fs.exists(new Path(args[1] + ".tempout"))){
                    fs.delete(new Path(args[1] + ".tempout"), true); 
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}