import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiply {

  public static class CellMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int m = Integer.parseInt(conf.get("m"));
      int p = Integer.parseInt(conf.get("p"));
      String line = value.toString();
      String[] allVals = line.split(",");
      Text outputKey = new Text();
      Text outputVal = new Text();
      if(allVals[0].equals("0")) {
	      outputVal.set(allVals[2]+","+allVals[3]);
        for (int k = 0; k < p; k++) {
          outputKey.set(allVals[1]+","+k);
          context.write(outputKey, outputVal);
        }
      } else if(allVals[0].equals("1")) {
	      outputVal.set(allVals[1]+","+allVals[3]);
        for (int k = 0; k < m; k++) {
          outputKey.set(k+","+allVals[2]);
          context.write(outputKey, outputVal);
        }
      }
    }
  }

  public static class VecMultReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
      String[] vals;
      int n = Integer.parseInt(context.getConfiguration().get("n"));
      float[] partialVals = new float[n];
      Arrays.fill(partialVals, 1);
      for (Text val : values) {
	      vals = val.toString().split(",");
        partialVals[Integer.parseInt(vals[0])] *= Float.parseFloat(vals[1]);
      }
      for (int i = 0; i < n; i++) {
	      sum += partialVals[i];
      }
      context.write(null, new Text(key.toString()+","+Float.toString(sum)));
    }
  }

  public static void main(String[] args) throws Exception {
    if(args.length != 5) {
      System.err.println("Usage: MatrixMultiply <in_dir> <out_dir> <M> <N> <P> (Where an MxN matrix is being multiplied by an NxP matrix)");
      System.exit(2);
    }
    Configuration conf = new Configuration();
    conf.set("m", args[2]);
    conf.set("n", args[3]);
    conf.set("p", args[4]);
    conf.set("mapred.max.split.size","2097152");
    Job job = Job.getInstance(conf, "Matrix Multiply");
    job.setJarByClass(MatrixMultiply.class);
    job.setMapperClass(CellMapper.class);
    //job.setCombinerClass(VecMultReducer.class);
    job.setReducerClass(VecMultReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(3);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
