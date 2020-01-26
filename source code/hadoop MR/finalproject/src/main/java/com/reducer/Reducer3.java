package com.reducer;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text, Text, Text, Text>{
	private HashSet<String> setA = new HashSet<String>();
	private HashSet<String> setB = new HashSet<String>();
	
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException,InterruptedException
	{
		setA.clear();
		setB.clear();
		
		for(Text t:values)
		{
			if(t.toString().charAt(0) == 'A')
				setA.add(key.toString());
			if(t.toString().charAt(0) == 'B')
				setB.add(key.toString());
		}
		
		if(setA.isEmpty() ^ setB.isEmpty())
		{
			for(String a : setA)
			{
				context.write(new Text(a), new Text("is downgraded team."));
			}
			
			for(String b : setB)
			{
				context.write(new Text(b), new Text("is upgraded team."));
			}
			
		}
	}
	

}
