import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
​
public class IMDBStudent20191003 {
​
	public static class IMDBMapper extends Mapper<Object, Text, Text, IntWritable>{
		private String filename;
		private Text genreText = new Text();
​
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{​
            		setup(context);

            		String[] line =  value.toString().split("::");
			int len = line.length;
            		String[] genres = line[len - 1].split("|");
         
            		for (String genre : genres) {
               			genreText.set(genre);
                		context.write(genreText, 1);
            		}    

		}

		protected void setup( Context contex) throws IOException, InterruptedException 
		{
			filename = ((FileSplit) contex.getInputSplit()).getPath().getName();
		}
	}
​
	public static class IMDBReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable sumWritable = new IntWritable();
​
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
		{
            		long sum = 0;
            		for (IntWritable val : values) {
                		sum += val.get();
            		}
            		sumWritable.set(sum);
            		context.write(key, sumWritable);
		}
	}
​
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "IMDBStudent");
​
		job.setJarByClass(IMDBStudent20191003.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
​
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
​
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
​
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
        
		job.waitForCompletion(true);
	}
}
​
