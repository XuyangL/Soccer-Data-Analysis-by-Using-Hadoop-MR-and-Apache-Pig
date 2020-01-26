package com.utils;

import java.util.Comparator;

public class StandingComparator implements Comparator<StandingWritable>{

	@Override
	public int compare(StandingWritable o1, StandingWritable o2) {
		// TODO Auto-generated method stub
		int result = 0;
		if(o1.getPoints() > o2.getPoints())
			result = -1;
		else if (o1.getPoints() < o2.getPoints())
				result = 1;
			 else
			 {
				  if(o1.getGoaldiff() > o2.getGoaldiff())
					result = -1;
				  else if(o1.getGoaldiff() < o2.getGoaldiff())
					  	result = 1;
					 
				 	   else result = 0;
			 }
		return result;
	}

}
