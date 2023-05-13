import java.io.IOException;
import java.util.*;
​
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.module.Configuration;
​
public class IMDBStudent20191003 {
​
	public static class IMDBMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		private String filename;
		private Text genreText = new Text();
​
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
​
            setup(context);

            while (itr.hasMoreTokens()) {
                String[] line =  itr.nextToken().split("::");
                String[] genres = line[2].split("|");

                for (String genre : genres) {
                    genreText.set(genre);
                    context.write(genreText, 1);
                }

            }

		}
		protected void setup( Context contex) throws IOException, InterruptedException {
			filename = ((FileSplit) contex.getInputSplit()).getPath().getName();
		}
	}
​
	public static class IMDBReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private LongWritable sumWritable = new LongWritable();
​
		public void reduce(Text key, Iterable<LongWritable> values, Context context ) throws IOException, InterruptedException
		{
            long sum = 0;
            for (LongWritable val : values) {
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
		job.setOutputValueClass(LongWritable.class);
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
