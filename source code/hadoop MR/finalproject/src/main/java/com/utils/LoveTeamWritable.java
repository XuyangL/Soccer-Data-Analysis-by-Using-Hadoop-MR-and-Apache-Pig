package com.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LoveTeamWritable implements Writable{

	private double goals;
	private int numberOfMatches;
	
	public LoveTeamWritable() {
		super();
	}

	public LoveTeamWritable(double goals, int numberOfMatches) {
		super();
		this.goals = goals;
		this.numberOfMatches = numberOfMatches;
	}

	public double getGoals() {
		return goals;
	}

	public void setGoals(double goals) {
		this.goals = goals;
	}

	public int getNumberOfMatches() {
		return numberOfMatches;
	}

	public void setNumberOfMatches(int numberOfMatches) {
		this.numberOfMatches = numberOfMatches;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.goals = arg0.readDouble();
		this.numberOfMatches = arg0.readInt();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeDouble(this.goals);
		arg0.writeInt(this.numberOfMatches);	
	}
	
	@Override
	public String toString()
	{
		return this.goals + "," + this.numberOfMatches;
	}

}
