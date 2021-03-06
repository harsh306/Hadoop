package com.elixir.hadoop.Chromo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.elixir.hadoop.Chromo.SecondrySort.IntPair;


public  class IntSumReducer 
     extends Reducer<IntPair,IntWritable,Text,IntWritable> {
	
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
