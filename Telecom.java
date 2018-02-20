import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



import java.text.*;
import java.util.*;
public class Telecom {
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		  Text phoneNumber = new Text();
		  IntWritable dur = new IntWritable();
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	
	            if(str[4].equals("1"))
	            {
	             phoneNumber.set(str[0]);
	             String end = str[3];
	             String start = str[2];
	            long duration = toMillis(end) - toMillis(start);
	            dur.set((int) duration/ (1000*60));
	            context.write(phoneNumber,dur);
	            }
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	private static long toMillis(String date) {
   	 
        SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss");
        Date dateFrm = null;
        try {
            dateFrm = format.parse(date);

        } catch (ParseException e) {

            e.printStackTrace();
       }
        return dateFrm.getTime();
    }
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		    private IntWritable result = new IntWritable();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
				
		         for (IntWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
		      if(sum > 60)
		      {
		      result.set(sum);		      
		      context.write(key, result);
		      }
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    conf.set("mapreduce.output.textoutputformat.separator", ",");
	    Job job = Job.getInstance(conf, "Telecom");
	    job.setJarByClass(Telecom.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
