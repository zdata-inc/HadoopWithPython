package com.minerkasch.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.minerkasch.mapreduce.WordCount.WordCountMapper;
import com.minerkasch.mapreduce.WordCount.WordCountReducer;

public class WordCountTest {
   MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
   ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
   MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

   @Before
   public void setUp() {
      WordCountMapper mapper = new WordCountMapper();
      WordCountReducer reducer = new WordCountReducer();

      mapDriver = MapDriver.newMapDriver(mapper);
      reduceDriver = ReduceDriver.newReduceDriver(reducer);
      mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
   }

   /**
    * A simple test to ensure the mapper is correctly tokenizing a string and returning the words as
    * keys with the value 1
    * 
    * @throws IOException
    */
   @Test
   public void testMapper() throws IOException {
      // Create the input key/value and output key/value
      Text valueIn = new Text("Hello world hello");
      Text keyOut0 = new Text("hello");
      Text keyOut1 = new Text("world");
      LongWritable valueOut = new LongWritable(1);

      // Set the mapper's input and expected output
      mapDriver.withInput(new LongWritable(), valueIn);
      mapDriver.withOutput(keyOut0, valueOut);
      mapDriver.withOutput(keyOut1, valueOut);
      mapDriver.withOutput(keyOut0, valueOut);

      // Run the test
      mapDriver.runTest();
   }

   /**
    * A simple test to ensure that the reducer is correctly adding values
    */
   @Test
   public void testReducer() {
      // Create the key 'hello' with two values: 1, 1
      Text keyIn = new Text("hello");
      ArrayList<LongWritable> valuesIn = new ArrayList<LongWritable>();
      valuesIn.add(new LongWritable(1));
      valuesIn.add(new LongWritable(1));

      // Set the reducer's input and expected output
      reduceDriver.withInput(keyIn, valuesIn);
      reduceDriver.withOutput(keyIn, new LongWritable(2));

      // Run the test
      reduceDriver.runTest();
   }

}
