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
public class RetailA21 {
	public static class MapClass extends Mapper<LongWritable,Text,LongWritable,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");
	            Long prodid = Long.parseLong(str[5]);
	            Long sales = Long.parseLong(str[8]);
	            Long cost = Long.parseLong(str[7]);
	            Long profit = sales - cost;
	            //double vol = Double.parseDouble(str[5]);
	            context.write(new LongWritable(prodid),new LongWritable(profit));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	 public static class ReduceClass extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>
	   {
		    private LongWritable result = new LongWritable();
		    
		    public void reduce(LongWritable key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      //double sum = 0;
		      int sales = 0;
		      int maxsales = 0;
		      String dt = "";
		      String custid = "";
		      Long gross = (long) 0;
		      
				
		         for (LongWritable val : values)
		         {       	
		        	 gross += val.get(); 
		        	
		         }
		      
		         
		      result.set(gross);      
		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "A1 Quest");
		    job.setJarByClass(RetailA21.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(LongWritable.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
