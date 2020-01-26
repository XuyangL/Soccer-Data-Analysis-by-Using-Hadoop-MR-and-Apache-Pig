package com.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.mapper.Mapper1;
import com.mapper.Mapper6;
import com.reducer.Reducer1;
import com.reducer.Reducer5;
import com.utils.StandingWritable;

public class App5 {

	private static String[] files = {
			"season-0910_csv.csv",
			"season-1011_csv.csv",
			"season-1112_csv.csv",
			"season-1213_csv.csv",
			"season-1314_csv.csv",
			"season-1415_csv.csv",
			"season-1516_csv.csv",
			"season-1617_csv.csv",
			"season-1718_csv.csv",
			"season-1819_csv.csv"
	};
	
	public static void main(String[] args) {
		for (int i = 0; i < 10; i++) 
		{
			try
			{
				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf, "demo4_" + i);
	        	job.setJarByClass(App5.class);
	        	
	        	job.setMapperClass(Mapper1.class);
	        	job.setReducerClass(Reducer1.class);    
	        	
	        	job.setMapOutputKeyClass(Text.class);
	        	job.setMapOutputValueClass(StandingWritable.class);
	        	
	        	job.setInputFormatClass(TextInputFormat.class);
	        	job.setOutputFormatClass(TextOutputFormat.class);
	        	
	        	job.setOutputKeyClass(Text.class);
	        	job.setOutputValueClass(StandingWritable.class);
	        	
	        	FileInputFormat.addInputPath(job, new Path(args[0] + "/" + files[i]));
	        	FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp" + i));
	        	
	        	try {
					job.waitForCompletion(true);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
			
	        catch(IOException e)
	        {
	        	e.printStackTrace();
	        }	
		}
		
		
	    try 
	    {
	    	Configuration conf = new Configuration();
	    	Job job = Job.getInstance(conf, "demo4_final");
	    	job.setJarByClass(App3.class);
	    	
	    	for(int i = 0 ; i  < 10 ; i++)
	    		MultipleInputs.addInputPath(job, new Path(args[1] + "/temp" + i + "/part-r-00000"), TextInputFormat.class, Mapper6.class);
	    	
	    	job.setReducerClass(Reducer5.class); 
	    	
	    	job.setOutputFormatClass(TextOutputFormat.class);
	    	FileOutputFormat.setOutputPath(job, new Path(args[1] + "/final"));
	    	
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(IntWritable.class);
	    		    	
	    	try {
				System.exit(job.waitForCompletion(true) ? 0 :1);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    catch(IOException e)
	    {
	    	e.printStackTrace();
	    }
		
		
	}
}
