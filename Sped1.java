

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Sped1 {
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");
	            int speed = Integer.parseInt(str[1]);
	            context.write(new Text(str[0]),new IntWritable(speed));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,Text>
	   {
		    private Text result = new Text();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      //double sum = 0;
		      int offence = 0;
		      int total = 0;
		      int per = 0;
		      String custid = "";
		      Long gross = (long) 0;
		      
				
		         for (IntWritable val : values)
		         {       	
		        	 if(val.get() > 65)
		        	 {
		        		 offence++;
		        	 }
		        	 total++;
		        	
		         }
		         per = ((offence * 100)/total);
		         String percent = String.format("%d", per);
		         String fin = percent + "%";
		      
		         
		      result.set(fin);      
		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
	    Job job = Job.getInstance(conf, "speed");
	    job.setJarByClass(Sped1.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(0);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
