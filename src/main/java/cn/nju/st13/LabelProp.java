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

public class LabelProp {
    static class InitMapper extends Mapper <Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split(" |\\t");
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
            String[] content = value.toString().split(" |\\t");
            String node = content[0];
            String label = content[1];
            String[] edgeList = content[2].split("\\|");

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
                String[] content = val.toString().split(" ");
				if (content[0].equals("label")) {
                    String neighbor = content[1].split(",")[0];
                    Integer neighborLabel = new Integer(content[1].split(",")[1]);
                    labelMap.put(neighbor, neighborLabel);
                }
                else if (content[0].equals("edgeList")) {
                    edgeList = content[1].split("\\|");
                    edgeListStr = content[1];
                }
                else {
                    throw new IOException("Undefined prefix: " + content[0]);
                }
            }
            
            if (edgeList.length == 0) throw new IOException("edgeList is empty");

            //build weightMap
            int edgeNum = edgeList.length;
            int counter = 0;
            int brokenTie = randGen.nextInt(edgeNum);
            if (edgeNum == 1) brokenTie = -1;
            for (String edge : edgeList) {
                //randomly break tie
                if (counter != brokenTie) {
                    String neighbor = edge.split(",")[0];
                    Double weight = new Double(edge.split(",")[1]);
                    Integer neigborLabel = labelMap.get(neighbor);
                    if (neigborLabel == null) throw new IOException("neighborLabel is null");

                    if (weightMap.containsKey(neigborLabel)) { // add
                        weightMap.put(neigborLabel, weightMap.get(neigborLabel) + weight);
                    }
                    else { // init
                        weightMap.put(neigborLabel, weight);
                    }
                }

                counter++;
            }

            //find maximum weight label
            Integer maxLabel = new Integer(-1);
            Double maxValue = new Double(-1.0);
            for (HashMap.Entry<Integer, Double> entry : weightMap.entrySet()) {
                if (entry.getValue() > maxValue) {
                    maxValue = entry.getValue();
                    maxLabel = entry.getKey();
                }
            }

            if (maxLabel == -1) throw new IOException("maxLabel cannot be found with map size " + weightMap.size() + " counter " + counter + " length " + edgeList.length);

            context.write(key, new Text(maxLabel.toString() + " " + edgeListStr));
		}
    }

    static class ClusterMapper extends Mapper <Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split(" |\\t", 3);
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

    static void initialize(Path inputPath, Path outputPath) {
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
        initJob.setOutputKeyClass(Text.class);
        initJob.setOutputValueClass(Text.class);
        try {
            FileInputFormat.addInputPath(initJob, inputPath);
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        FileOutputFormat.setOutputPath(initJob, outputPath);

        try {
            initJob.waitForCompletion(true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void iterate(Path inputPath, Path outputPath, int epochId) {
        Configuration configuration = new Configuration();

        Job iterJob = null;
        try {
            iterJob = Job.getInstance(configuration, "2019St13 LabelProp Iteration Job epoch " + String.valueOf(epochId));
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        iterJob.setJarByClass(LabelProp.class);
        iterJob.setMapperClass(IterMapper.class);
        iterJob.setReducerClass(IterReducer.class);
        iterJob.setInputFormatClass(TextInputFormat.class);
        iterJob.setNumReduceTasks(5);
        iterJob.setOutputKeyClass(Text.class);
        iterJob.setOutputValueClass(Text.class);
        try {
            FileInputFormat.addInputPath(iterJob, inputPath);
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        FileOutputFormat.setOutputPath(iterJob, outputPath);

        try {
            iterJob.waitForCompletion(true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void cluster(Path inputPath, Path outputPath) {
        Configuration configuration = new Configuration();

        Job clusterJob = null;
        try {
            clusterJob = Job.getInstance(configuration, "2019St13 LabelProp Clustering Job");
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        clusterJob.setJarByClass(LabelProp.class);
        clusterJob.setMapperClass(ClusterMapper.class);
        clusterJob.setReducerClass(ClusterReducer.class);
        clusterJob.setInputFormatClass(TextInputFormat.class);
        clusterJob.setOutputKeyClass(Text.class);
        clusterJob.setOutputValueClass(Text.class);
        try {
            FileInputFormat.addInputPath(clusterJob, inputPath);
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        FileOutputFormat.setOutputPath(clusterJob, outputPath);

        try {
            clusterJob.waitForCompletion(true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int epoch = 2;
        try {
            Configuration configuration = new Configuration();
            Path tempin = new Path(outputPath + ".tempin");
            Path tempout = new Path(outputPath + ".tempout");

            initialize(inputPath, tempout);

            for (int i = 0; i < epoch; ++i) {
                FileSystem fs = tempout.getFileSystem(configuration);
                if (fs.exists(tempout)) {
                    fs.rename(tempout, tempin);
                }
                iterate(tempin, tempout, i + 1);
                if (fs.exists(tempin)) {
                    fs.delete(tempin, true); 
                }
            }

            cluster(tempout, outputPath);

            //delete immediate files
            FileSystem fs = tempout.getFileSystem(configuration);
            if (fs.exists(tempin)) {
                fs.delete(tempin, true); 
            }
            if (fs.exists(tempout)) {
                fs.delete(tempout, true); 
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}