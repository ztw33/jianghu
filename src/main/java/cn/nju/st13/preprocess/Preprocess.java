package cn.nju.st13.preprocess;

import org.apache.hadoop.fs.Path;

public class Preprocess {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("error!");
            System.exit(1);
        }
        WordSeg.Task1Main(new Path(args[0]), new Path(args[1]), new Path(args[2] + "output1"));
        switch (args[3]) {
            case "-1":
                WordCoOcc_baseline.Task2Main(new Path(args[2] + "output1"), new Path(args[2] + "output2"));
                Normalization_inv.Task3Main(new Path(args[2] + "output2"), new Path(args[2] + "output3"));
                break;
            case "-2":
                WordCoOcc_pair.Task2Main(new Path(args[2] + "output1"), new Path(args[2] + "output2"));
                Normalization.Task3Main(new Path(args[2] + "output2"), new Path(args[2] + "output3"));
                break;
            case "-3":
                WordCoOcc.Task2Main(new Path(args[2] + "output1"), new Path(args[2] + "output2"));
                Normalization_inv.Task3Main(new Path(args[2] + "output2"), new Path(args[2] + "output3"));
                break;
        }
    }
}
