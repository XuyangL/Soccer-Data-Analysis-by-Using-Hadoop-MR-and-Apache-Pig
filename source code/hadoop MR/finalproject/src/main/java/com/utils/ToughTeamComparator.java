package com.utils;

import java.util.Comparator;

public class ToughTeamComparator implements Comparator<ToughTeamWritable>{

	@Override
	public int compare(ToughTeamWritable o1, ToughTeamWritable o2) {
		// TODO Auto-generated method stub
		int result = 0;
		
		if(o1.getPoints() > o2.getPoints())
			result = 1;
		else if (o1.getPoints() < o2.getPoints())
				result = -1;
			 else result = 0;
		 
		return result;	
	}

}
