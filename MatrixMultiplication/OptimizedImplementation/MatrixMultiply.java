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
      int K = Integer.parseInt(conf.get("k"));
      int L = Integer.parseInt(conf.get("l"));
      String line = value.toString();
      String[] allVals = line.split(",");
      Text outputKey = new Text();
      Text outputVal = new Text();
      int rowGroupSize = ((m-1)/K+1);
      int colGroupSize = ((p-1)/L+1);
      if(allVals[0].equals("0")) {
        int rowGroup = Integer.parseInt(allVals[1]) / rowGroupSize;
        int rowGroupIdx = Integer.parseInt(allVals[1]) - rowGroup * rowGroupSize;
        outputVal.set("0,"+rowGroupIdx+","+allVals[2]+","+allVals[3]);
	      for (int l = 0; l < L; l++) {
	        outputKey.set(rowGroup+","+l);
	        context.write(outputKey, outputVal);
	      }
      } else if(allVals[0].equals("1")) {
        int colGroup = Integer.parseInt(allVals[2]) / colGroupSize;
        int colGroupIdx = Integer.parseInt(allVals[2]) - colGroup * colGroupSize;
        outputVal.set("1,"+colGroupIdx+","+allVals[1]+","+allVals[3]);          
        for (int k = 0; k < K; k++) {
          outputKey.set(k+","+colGroup);
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
      
      String[] vals;
      String[] keys;
      Configuration conf = context.getConfiguration();
      int M = Integer.parseInt(conf.get("m"));
      int N = Integer.parseInt(conf.get("n"));
      int P = Integer.parseInt(conf.get("p"));
      int K = Integer.parseInt(conf.get("k"));
      int L = Integer.parseInt(conf.get("l"));
      int rowGroupSize = ((M-1)/K+1);
      int colGroupSize = ((P-1)/L+1);
      float[][][] partialVals = new float[rowGroupSize][colGroupSize][N];
      float[][] sums = new float[rowGroupSize][colGroupSize];
      for (float[][] partialMat: partialVals)
        for (float[] partialRow: partialMat)
          Arrays.fill(partialRow, 1.0f);
      
      keys = key.toString().split(",");
      for (Text val : values) {
	      vals = val.toString().split(",");
        int matId = Integer.parseInt(vals[0]);
        int partialIdx = Integer.parseInt(vals[2]);
        float value = Float.parseFloat(vals[3]);
        if(matId == 0) {
          int rowIdx = Integer.parseInt(vals[1]);
          for (int i = 0; i < colGroupSize; i++) {
            partialVals[rowIdx][i][partialIdx] *= value;
          }
        } else if(matId == 1) {
          int colIdx = Integer.parseInt(vals[1]);
          for (int i = 0; i < rowGroupSize; i++) {
            partialVals[i][colIdx][partialIdx] *= value;
          }
        }
      }
      for (int r = 0; r < rowGroupSize; r++) {
        for (int c = 0; c < colGroupSize; c++) {
          for (int n = 0; n < N; n++) {
	          sums[r][c] += partialVals[r][c][n];
          }
          context.write(null, new Text((Integer.parseInt(keys[0])*rowGroupSize+r)+","+(Integer.parseInt(keys[1])*colGroupSize+c)+","+Float.toString(sums[r][c])));
        }
      }
      
      
    }
  }

  public static void main(String[] args) throws Exception {
    if(args.length != 7) {
      System.err.println("Usage: MatrixMultiply <in_dir> <out_dir> <M> <N> <P> <K> <L> (Where an MxN matrix is being multiplied by an NxP matrix) with a total of KxL reducers.");
      System.exit(2);
    }
    Configuration conf = new Configuration();
    conf.set("m", args[2]);
    conf.set("n", args[3]);
    conf.set("p", args[4]);
    conf.set("k", args[5]);
    conf.set("l", args[6]);
    conf.set("mapred.max.split.size","15000000");
    Job job = Job.getInstance(conf, "Matrix Multiply");
    job.setJarByClass(MatrixMultiply.class);
    job.setMapperClass(CellMapper.class);
    job.setReducerClass(VecMultReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(8);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
