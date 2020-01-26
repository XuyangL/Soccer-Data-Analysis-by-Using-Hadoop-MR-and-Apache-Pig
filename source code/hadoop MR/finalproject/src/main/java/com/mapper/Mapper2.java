package com.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.utils.ToughTeamWritable;

public class Mapper2 extends Mapper<LongWritable, Text, Text, ToughTeamWritable>{
	
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
	
		if(team.equals(tokens[2]))
		{	
			switch(tokens[6])
			{
				case "H":
					context.write(new Text(tokens[3]), new ToughTeamWritable(3,1));
					break;
					
				case "D":
					context.write(new Text(tokens[3]), new ToughTeamWritable(1,1));
					break;
					
				case "A":
					context.write(new Text(tokens[3]), new ToughTeamWritable(0,1));
					break;
			
			}		
		}
		
		if(team.equals(tokens[3]))
		{	
			switch(tokens[6])
			{
				case "H":
					context.write(new Text(tokens[2]), new ToughTeamWritable(0,1));
					break;
					
				case "D":
					context.write(new Text(tokens[2]), new ToughTeamWritable(1,1));
					break;
					
				case "A":
					context.write(new Text(tokens[2]), new ToughTeamWritable(3,1));
					break;
			
			}		
		}
	}

}
