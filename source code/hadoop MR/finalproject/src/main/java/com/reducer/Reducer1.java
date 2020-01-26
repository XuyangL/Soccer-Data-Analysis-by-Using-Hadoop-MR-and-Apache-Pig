package com.reducer;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.utils.StandingComparator;
import com.utils.StandingWritable;


public class Reducer1 extends Reducer<Text, StandingWritable, Text, StandingWritable>{
	
	private static TreeMap<StandingWritable, String> map = new TreeMap<StandingWritable, String>(new StandingComparator());
	
	public void reduce(Text key, Iterable<StandingWritable> values, Context context)throws IOException,InterruptedException
	{
		int sumWin = 0;
		int sumDraw = 0;
		int sumLose = 0;
		int sumGoalin = 0;
		int sumGoallose = 0;
		int sumGoaldiff = 0;
		int sumPoint = 0;
		
		for(StandingWritable v : values)
		{
			sumWin += v.getWin();
			sumDraw += v.getDraw();
			sumLose += v.getLose();
			sumGoalin += v.getGoalin();
			sumGoallose += v.getGoallost();
			sumGoaldiff += v.getGoaldiff();
			sumPoint += v.getPoints();
					
		}
		StandingWritable s = new StandingWritable(sumWin,sumDraw,sumLose,sumGoalin,sumGoallose,sumGoaldiff,sumPoint);
		
		map.put(s, key.toString());
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		for(StandingWritable s : map.keySet())
		{
			context.write(new Text(map.get(s)), s);
			
		}
	}
}
