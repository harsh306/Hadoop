package com.elixir.hadoop.Chromo;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CoverageMapper 
extends Mapper<Object, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);
private Text position = new Text();

public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
String []temp=(value.toString().split(" "));
int ChrNum=Integer.parseInt(temp[0]);
int startPos=Integer.parseInt(temp[1]);
int endPos=Integer.parseInt(temp[2]);
for(int i=startPos;i<=endPos;i++)
   {
	    position.set( String.valueOf(ChrNum)+" "+ String.valueOf(i));
     context.write(position, one);
   }
}
}
