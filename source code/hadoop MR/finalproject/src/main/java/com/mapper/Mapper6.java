package com.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper6 extends Mapper<LongWritable,Text,Text,IntWritable>{
	private boolean flag = true;
	
	public void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException
	{	
		if (flag) 
		{
			String[] tokens = value.toString().split("\t");
			context.write(new Text(tokens[0]), new IntWritable(1));
			flag = false;
		}
	}

}
