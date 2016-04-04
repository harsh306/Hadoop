package com.elixir.hadoop.Chromo;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;

import com.elixir.hadoop.Chromo.SecondrySort.*;

public class ChromoPartitioner extends Partitioner<IntPair,IntWritable>{

	public int getPartition(IntPair arg0, IntWritable arg1, int numOfReducers) {
		// TODO Auto-generated method stub
		return arg0.getPrimaryKey()%numOfReducers;
	}

	
}
