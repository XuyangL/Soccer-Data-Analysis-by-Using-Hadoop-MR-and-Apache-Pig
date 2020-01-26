package com.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.utils.LoveTeamWritable;


public class Mapper5 extends Mapper<LongWritable, Text, Text, LoveTeamWritable>{
	
	private String team = null;
	public void setup(Context context)
	{
		team = context.getConfiguration().get("targetTeam");	
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{	
		String[] tokens = value.toString().split(",");
		if(tokens[0].equals("Div"))
			return;
		
		int homeGoal = Integer.valueOf(tokens[4]);
		int awayGoal = Integer.valueOf(tokens[5]);
	
		if(team.equals(tokens[2]))
		{	
			context.write(new Text(tokens[3]), new LoveTeamWritable(homeGoal - awayGoal,1));
		}
		
		if(team.equals(tokens[3]))
		{	
			context.write(new Text(tokens[2]), new LoveTeamWritable(awayGoal - homeGoal,1));
		}
	}
}
