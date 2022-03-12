package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Phone {

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
   private Text phone = new Text();
   private IntWritable downlink = new IntWritable(0);
   private IntWritable uplink = new IntWritable(0);
   private IntWritable total = new IntWritable(0);

     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       String line = value.toString();
       String[] tokens = line.split("\\s+");
       phone.set(tokens[1]);
       downlink.set(Integer.parseInt(tokens[7]));
       uplink.set(Integer.parseInt(tokens[8]));
       total.set(Integer.parseInt(tokens[7]) + Integer.parseInt(tokens[8]));
       output.collect(phone, total);
     }
   }

   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       int sumDownlink = 0;
       int sumUplink = 0;
       int sumTotal = 0;
       while (values.hasNext()) {
         sumTotal += values.next().get();
       }
       output.collect(key, new IntWritable(sumTotal));
     }
   }

   public static void main(String[] args) throws Exception {
     JobConf conf = new JobConf(Phone.class);
     conf.setJobName("phone");

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(IntWritable.class);

     conf.setMapperClass(Map.class);
     conf.setCombinerClass(Reduce.class);
     conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     JobClient.runJob(conf);
   }
}
