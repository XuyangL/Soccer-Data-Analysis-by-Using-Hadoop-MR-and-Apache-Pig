package com.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.mapper.Mapper5;
import com.reducer.Reducer4;
import com.utils.LoveTeamWritable;


public class App4 {
	
	public static void main( String[] args )
    {
    	Configuration conf = new Configuration();
        try 
        {
        	Job job = Job.getInstance(conf, "demo5");
        	job.setJarByClass(App2.class);
        	
        	job.getConfiguration().set("targetTeam", args[0]);
        	
        	job.setMapperClass(Mapper5.class);
        	job.setReducerClass(Reducer4.class);    
        	
        	job.setMapOutputKeyClass(Text.class);
        	job.setMapOutputValueClass(LoveTeamWritable.class);
        	
        	job.setInputFormatClass(TextInputFormat.class);
        	job.setOutputFormatClass(TextOutputFormat.class);
        	
        	job.setOutputKeyClass(Text.class);
        	job.setOutputValueClass(LoveTeamWritable.class);
        	
        	FileInputFormat.addInputPath(job, new Path(args[1]));
        	FileOutputFormat.setOutputPath(job, new Path(args[2]));
        	
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
