package com.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StandingWritable implements Writable{
	private int win;
	private int draw;
	private int lose;
	private int goalin;
	private int goallost;
	private int goaldiff;
	private int points;

	
	public StandingWritable() {
		super();
	}

	public StandingWritable(int win, int draw, int lose, int goalin, int goallost, int goaldiff,
			int points) {
		super();
		this.win = win;
		this.draw = draw;
		this.lose = lose;
		this.goalin = goalin;
		this.goallost = goallost;
		this.goaldiff = goaldiff;
		this.points = points;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.win = arg0.readInt();
		this.draw = arg0.readInt();
		this.lose = arg0.readInt();
		this.goalin = arg0.readInt();
		this.goallost = arg0.readInt();
		this.goaldiff = arg0.readInt();
		this.points = arg0.readInt();
		
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(this.win);
		arg0.writeInt(this.draw);
		arg0.writeInt(this.lose);
		arg0.writeInt(this.goalin);
		arg0.writeInt(this.goallost);
		arg0.writeInt(this.goaldiff);
		arg0.writeInt(this.points);		
	}

	public int getWin() {
		return win;
	}

	public void setWin(int win) {
		this.win = win;
	}

	public int getDraw() {
		return draw;
	}

	public void setDraw(int draw) {
		this.draw = draw;
	}

	public int getLose() {
		return lose;
	}

	public void setLose(int lose) {
		this.lose = lose;
	}

	public int getGoalin() {
		return goalin;
	}

	public void setGoalin(int goalin) {
		this.goalin = goalin;
	}

	public int getGoallost() {
		return goallost;
	}

	public void setGoallost(int goallost) {
		this.goallost = goallost;
	}

	public int getGoaldiff() {
		return goaldiff;
	}

	public void setGoaldiff(int goaldiff) {
		this.goaldiff = goaldiff;
	}

	public int getPoints() {
		return points;
	}

	public void setPoints(int points) {
		this.points = points;
	}
	
	@Override
	public String toString()
	{
		return this.win + "," + this.draw + "," + this.lose + "," + this.goalin + "," + this.goallost + "," + this.goaldiff + "," + this.points; 
	}

}
