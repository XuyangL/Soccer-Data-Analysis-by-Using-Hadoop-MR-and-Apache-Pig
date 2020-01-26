package com.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.utils.StandingWritable;

public class Mapper1 extends Mapper<LongWritable,Text,Text,StandingWritable>{

	public void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException
	{		
		String[] tokens = value.toString().split(",");
		if(tokens[0].equals("Div"))
			return;
		
		Text homeTeam = new Text(tokens[2]);
		Text awayTeam = new Text(tokens[3]);
		int homeGoal = Integer.valueOf(tokens[4]);
		int awayGoal = Integer.valueOf(tokens[5]);
		
		switch(tokens[6])
		{
			case "H":
				StandingWritable s1 = new StandingWritable(1, 0, 0, homeGoal, awayGoal, homeGoal - awayGoal, 3);
				StandingWritable s2 = new StandingWritable( 0, 0, 1, awayGoal, homeGoal, awayGoal - homeGoal, 0);
				context.write(homeTeam, s1);
				context.write(awayTeam, s2);
				break;
			
			case "D":
				StandingWritable s3 = new StandingWritable(0, 1, 0, homeGoal, awayGoal, 0, 1);
				StandingWritable s4 = new StandingWritable( 0, 1, 0, awayGoal, homeGoal, 0, 1);
				context.write(homeTeam, s3);
				context.write(awayTeam, s4);
				break;
				
			case "A":
				StandingWritable s5 = new StandingWritable(0, 0, 1, homeGoal, awayGoal, homeGoal - awayGoal, 0);
				StandingWritable s6 = new StandingWritable(1, 0, 0, awayGoal, homeGoal, awayGoal - homeGoal, 3);
				context.write(homeTeam, s5);
				context.write(awayTeam, s6);
				break;
		}	
	}
}
