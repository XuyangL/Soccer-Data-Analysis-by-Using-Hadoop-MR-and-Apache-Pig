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

import com.mapper.Mapper1;
import com.reducer.Reducer1;
import com.utils.StandingWritable;

/**
 * Hello world!
 *
 */
public class App1 
{
    public static void main( String[] args )
    {
    	Configuration conf = new Configuration();
        try 
        {
        	Job job = Job.getInstance(conf, "demo3");
        	job.setJarByClass(App1.class);
        	
        	job.setMapperClass(Mapper1.class);
        	job.setReducerClass(Reducer1.class);    
        	
        	job.setMapOutputKeyClass(Text.class);
        	job.setMapOutputValueClass(StandingWritable.class);
        	
        	job.setInputFormatClass(TextInputFormat.class);
        	job.setOutputFormatClass(TextOutputFormat.class);
        	
        	job.setOutputKeyClass(Text.class);
        	job.setOutputValueClass(StandingWritable.class);
        	
        	FileInputFormat.addInputPath(job, new Path(args[0]));
        	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        	
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
