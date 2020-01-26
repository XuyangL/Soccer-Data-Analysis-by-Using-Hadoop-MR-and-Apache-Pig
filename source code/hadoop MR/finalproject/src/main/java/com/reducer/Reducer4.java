package com.reducer;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.utils.LoveTeamComparator;
import com.utils.LoveTeamWritable;


public class Reducer4 extends Reducer<Text, LoveTeamWritable, Text, LoveTeamWritable>{

private static TreeMap<LoveTeamWritable, String> map = new TreeMap<LoveTeamWritable, String>(new LoveTeamComparator());
	
	public void reduce(Text key, Iterable<LoveTeamWritable> values, Context context)throws IOException,InterruptedException
	{
		double goals = 0;
		int numberOfMatch = 0;
		
		for(LoveTeamWritable v : values)
		{
			goals += v.getGoals() * v.getNumberOfMatches();
			numberOfMatch += v.getNumberOfMatches();
		}
			
		LoveTeamWritable s = new LoveTeamWritable(goals/numberOfMatch, numberOfMatch);
		map.put(s, key.toString());
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		int i = 0;
		for(LoveTeamWritable v : map.keySet())
		{
			context.write(new Text(map.get(v)), v);
			i++;
			if(i >= 10)
			  break;	
		}	
	}
}
