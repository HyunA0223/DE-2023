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

import java.time.DayOfWeek;
import java.time.LocalDate;

public class UBERStudent20191003 {

    public static class UBERMapper extends Mapper<Object, Text, Text, Text>
    {
		private Text keyText = new Text();
                private String[] dayList = {"", "MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"};
                private Text key = new Text();
                private Text resultText = new Text();


                public void map(Object key, Text value, Context context) throws IOException, InterruptedException
                {

                    String[] line =  value.toString().split(",");
                    String bNum = line[0];

                    String[] datePart = line[1].split("/");
                    int year = Integer.parseInt(datePart[2]);
                    int month = Integer.parseInt(datePart[0]);
                    int day = Integer.parseInt(datePart[1]);
                    LocalDate date = LocalDate.of(year, month, day);
                    DayOfWeek dayOfWeek = date.getDayOfWeek();

                    String bNumAndDay = bNum + "," + dayList[dayOfWeek.getValue()];
                    keyText.set(bNumAndDay);

                    int vehicles = Integer.parseInt(line[2]);
                    int trips = Integer.parseInt(line[3]);

                    String tripAndVehicle = trips + "," + vehicles;
                    resultText.set(tripAndVehicle);

                    context.write(keyText, resultText);
                }

    }
​
    public static class UBERReducer extends Reducer<Text, Text, Text, Text>
    {
                private Text resultText = new Text();
​
                public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
                {
                    int vehicles = 0;
                    int trips = 0;
                    for (Text val : values) {
			String[] tav = val.toString().split(",");		
			trips += Integer.parseInt(tav[0]);
                     	vehicles += Integer.parseInt(tav[1]);
                     }
                    String result = trips + "," + vehicles;
                    resultText.set(result);
                    context.write(key, resultText);
	}
    }

    public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "UBERStudent20191003");
​
		job.setJarByClass(UBERStudent20191003.class);
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
​
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
