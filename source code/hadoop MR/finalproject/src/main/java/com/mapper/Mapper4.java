package com.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper4 extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{	
		String[] tokens = value.toString().split(",");
		if(tokens[0].equals("Div"))
			return;
		context.write(new Text(tokens[2]), new Text("B"));
		context.write(new Text(tokens[3]), new Text("B"));
	}
}
