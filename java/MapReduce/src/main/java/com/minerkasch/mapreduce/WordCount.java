package com.minerkasch.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool {

   /**
    * Tokenize the value and output each word as a key and 1 as the value
    * 
    * @author Zachary Radtka
    *
    */
   public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

      // The count for a single word, 1
      private final static LongWritable one = new LongWritable(1);

      // The word to pass to the combiner/reducer
      private Text word = new Text();

      public void map(LongWritable keyIn, Text valueIn, Context context) throws IOException,
            InterruptedException {
         // Split the current line into words
         StringTokenizer tokenizer = new StringTokenizer(valueIn.toString());

         // Create an output key/value pair for each word: <word,1>
         while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().toLowerCase());
            context.write(word, one);
         }
      }
   }


   /**
    * Sum up the values for each unique key
    * 
    * @author Zachary Radtka
    *
    */
   public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

      // The total count for a unique word
      private LongWritable sum = new LongWritable();

      public void reduce(Text keyIn, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

         // Initialize the current word's sum
         long partialSum = 0;

         // Calculate the number of occurrences for the current word
         for (LongWritable value : values) {
            partialSum += value.get();
         }

         // Set the sum for the current word and output it's total
         sum.set(partialSum);
         context.write(keyIn, sum);
      }
   }


   @Override
   public int run(String[] args) throws Exception {

      // Configuration processed by ToolRunner
      Configuration conf = getConf();

      // Create a Job using the processed conf
      Job job = Job.getInstance(conf, "WordCount");
      job.setJarByClass(WordCount.class);

      // Set the Mapper, Combiner, and Reducer class
      // It is important to note that when using the reducer as a
      // combiner, the reducer's input key/value types much match
      // it's output key/value types
      job.setMapperClass(WordCountMapper.class);
      job.setCombinerClass(WordCountReducer.class);
      job.setReducerClass(WordCountReducer.class);

      // Set the input and output for the job
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      // Set the input and output paths
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      // Submit the job and return it's status
      return job.waitForCompletion(true) ? 0 : 1;
   }


   /**
    * A simple driver that creates, configures and runs a MapReduce job
    * 
    * @param args
    * @throws Exception
    */
   public static void main(String[] args) throws Exception {

      // Ensure an input and output path were specified
      if (args.length != 2) {
         System.err.println("<inputDirectory> <ouputDirectory>");

         System.exit(1);
      }

      // Set up and run the job
      int result = ToolRunner.run(new Configuration(), new WordCount(), args);

      System.exit(result);
   }

}
