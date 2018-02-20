



import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class VarianceHigh {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,DoubleWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            double high = Double.parseDouble(str[4]);
	            double low = Double.parseDouble(str[5]);
	            double var = high - low;
	            double var_ratio = var/low;
	            double var_perc = var_ratio * 100;
	            DoubleWritable var_fin = new DoubleWritable(var_perc); 
	            context.write(new Text(str[1]),var_fin);
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	 public static class ReduceClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	   {
		    private DoubleWritable result = new DoubleWritable();
		    
		    public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		      //double sum = 0;
		      double max = 0;
		      double temp =0;
		      
				
		         for (DoubleWritable val : values)
		         {       	
		        	 temp = val.get();
		        	if(temp > max)
		        	{
		        		max = temp;
		        	}
		         }
		      
		         
		      result.set(max);		      
		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(VarianceHigh.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}



