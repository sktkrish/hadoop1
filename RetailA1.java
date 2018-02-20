import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class RetailA1 {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");
	            String mykey = "common"; 
	            String prodid = str[5];
	            String dt = str[0];
	            String custid = str[1].trim();
	            String sales = str[8];
	            String myval = dt + "," + custid + "," + sales;
	            //double vol = Double.parseDouble(str[5]);
	            context.write(new Text(mykey),new Text(myval));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	 public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
	   {
		    private DoubleWritable result = new DoubleWritable();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      //double sum = 0;
		      int sales = 0;
		      int maxsales = 0;
		      String dt = "";
		      String custid = "";
		      
				
		         for (Text val : values)
		         {       	
		        	 String[] token = val.toString().split(",");
		        	 sales = Integer.parseInt(token[2]);
		        	// dt = token[0];
		        	 //custid = token[1];
		        	 if(sales > maxsales)
		        	 {
		        		 maxsales = sales;
		        		 dt = token[0];
			        	 custid = token[1];
		        	 }
		         }
		      
		         
		      String myValue = dt + "," + custid + "," + String.format("%d", maxsales);      
		      context.write(NullWritable.get(), new Text(myValue));
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "A1 Quest");
		    job.setJarByClass(RetailA1.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
