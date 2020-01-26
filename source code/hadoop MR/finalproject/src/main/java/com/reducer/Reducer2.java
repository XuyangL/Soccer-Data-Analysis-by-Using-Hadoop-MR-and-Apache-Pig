package com.reducer;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.utils.ToughTeamComparator;
import com.utils.ToughTeamWritable;

public class Reducer2 extends Reducer<Text, ToughTeamWritable, Text, ToughTeamWritable>{
	
	private static TreeMap<ToughTeamWritable, String> map = new TreeMap<ToughTeamWritable, String>(new ToughTeamComparator());
	
	public void reduce(Text key, Iterable<ToughTeamWritable> values, Context context)throws IOException,InterruptedException
	{
		double points = 0;
		int numberOfMatch = 0;
		
		for(ToughTeamWritable v : values)
		{
			points += v.getPoints() * v.getNumberOfMatches();
			numberOfMatch += v.getNumberOfMatches();
		}
			
		ToughTeamWritable s = new ToughTeamWritable(points/numberOfMatch, numberOfMatch);
		map.put(s, key.toString());
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		int i = 0;
		for(ToughTeamWritable v : map.keySet())
		{
			context.write(new Text(map.get(v)), v);
			i++;
			if(i >= 3)
			  break;	
		}	
	}
}
