package com.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.mapper.Mapper3;
import com.mapper.Mapper4;
import com.reducer.Reducer3;


public class App3 {

	public static void main( String[] args )
    {
		Configuration conf = new Configuration();
	    try 
	    {
	    	Job job = Job.getInstance(conf, "demo1");
	    	job.setJarByClass(App3.class);
	    	
	    	MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper3.class);
	    	MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper4.class);
	    	job.setReducerClass(Reducer3.class); 
	    	
	    	job.setOutputFormatClass(TextOutputFormat.class);
	    	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    	
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    		    	
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
