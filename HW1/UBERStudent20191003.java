import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.text.ParseException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20191003
{

	public static class UBERMapper20191003 extends Mapper<Object, Text, Text, Text>
	{
		private Text keyWord = new Text();
		private Text valueWord = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			while (itr.hasMoreTokens()) {
				String bNumber = itr.nextToken();
				String inputDate = itr.nextToken();	
				
				DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
				Date date = new Date();
				try{
					date = df.parse(inputDate);
				} 
				catch (ParseException e) {
					System.err.println("error");
				}
				
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				
				String wNumber = Integer.toString(cal.get(Calendar.DAY_OF_WEEK) -1);						
				String activeVehicles = itr.nextToken().trim();
				String trips = itr.nextToken().trim();				
						
				keyWord.set(bNumber + "," + wNumber);
				value_word.set(trips + "," + activeVehicles);
				
				context.write(keyWord, valueWord);				
			}
		}
	}

	public static class UBERReducer20191003 extends Reducer<Text, Text, Text, Text> 
	{
		private String [] weeks = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
		private Text newKey = new Text();
		private Text resultText = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int sumTrips = 0;
			int sumVehicles = 0;
			
			StringTokenizer itrKey = new StringTokenizer(key.toString(), ",");
			while (itrKey.hasMoreTokens()) {
				String bNumber = itrKey.nextToken();
				int wNumber = Integer.parseInt(itrKey.nextToken().trim());
			
				String week = weeks[wNumber];
				newKey.set(bNumber + "," + week);
			}
			
			for (Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				while (itr.hasMoreTokens()) {
					int trips = Integer.parseInt(itr.nextToken().trim());
					int activeVehicles = Integer.parseInt(itr.nextToken().trim());
					
					sumTrips += trips;
					sumVehicles += activeVehicles;			
				}				
			}
			
			String sum = Integer.toString(sumTrips) + "," 
				+ Integer.toString(sumVehicles);
						
			resultText.set(sum);
			context.write(newKey, resultText);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "uberstudent20191003");	
		
		job.setJarByClass(UBERStudent20191003.class);
		job.setMapperClass(UBERMapper20191003.class);
		job.setReducerClass(UBERReducer20191003.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}
