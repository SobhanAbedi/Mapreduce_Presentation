import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumCombiner
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class LongSumReducer
       extends Reducer<Text,IntWritable,LongWritable,Text> {
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      long sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(result, key);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path out = new Path(args[1]);

    Job job1 = Job.getInstance(conf, "word count");
    job1.setJarByClass(WordCount.class);
    job1.setMapperClass(TokenizerMapper.class);
    job1.setCombinerClass(IntSumCombiner.class);
    job1.setReducerClass(LongSumReducer.class);
    job1.setNumReduceTasks(3);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(LongWritable.class);
    job1.setOutputValueClass(Text.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
    if (!job1.waitForCompletion(true)) {
      System.exit(1);
    }
    Job job2 = Job.getInstance(conf, "sort by frequency");
    job2.setJarByClass(WordCount.class);
    //job2.setMapperClass(KeyValueSwappingMapper.class);
    job2.setNumReduceTasks(3);
    job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
    job2.setOutputKeyClass(LongWritable.class);
    job2.setOutputValueClass(Text.class);
    job2.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job2, new Path(out, "out1"));
    FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
    if (!job2.waitForCompletion(true)) {
      System.exit(1);
    }
  }
}
