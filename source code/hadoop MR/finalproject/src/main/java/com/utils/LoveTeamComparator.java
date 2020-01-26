package com.utils;

import java.util.Comparator;

public class LoveTeamComparator implements Comparator<LoveTeamWritable>{

	@Override
	public int compare(LoveTeamWritable o1, LoveTeamWritable o2) {
		// TODO Auto-generated method stub
		int result = 0;
		
		if(o1.getGoals() > o2.getGoals())
			result = -1;
		else if (o1.getGoals() < o2.getGoals())
				result = 1;
			 else result = 0;
		 
		return result;	
	}

}
