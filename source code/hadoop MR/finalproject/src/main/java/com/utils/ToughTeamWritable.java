package com.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ToughTeamWritable implements Writable{
	
	private double points;
	private int numberOfMatches;
	
	
	public ToughTeamWritable() {
		super();
	}
	
	public ToughTeamWritable(double points, int numberOfMatches) {
		super();
		this.points = points;
		this.numberOfMatches = numberOfMatches;
	}
	
	
	public double getPoints() {
		return points;
	}
	public void setPoints(double points) {
		this.points = points;
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
		this.points = arg0.readDouble();
		this.numberOfMatches = arg0.readInt();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeDouble(this.points);
		arg0.writeInt(this.numberOfMatches);
		
	}
	
	@Override
	public String toString()
	{
		return this.points + "," + this.numberOfMatches;
	}

}
