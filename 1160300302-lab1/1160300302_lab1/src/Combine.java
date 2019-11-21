import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Combine {

	private static double rating_down;
	private static double rating_up;
	
	public static class SampleMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split("\\|");
			String rating = words[6];
			String career = words[10];
			if(!rating.equals("?")) {
				context.write(new Text(career), new DoubleWritable(Double.parseDouble(rating)));
			}
		}
	}
	
	public static class SampleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sampling_ratio = 0.01;
			int sampling_distance = (int)(1 / sampling_ratio);
			int first_sampling = new Random().nextInt(sampling_distance - 1) + 1;
			int count = 0;
			boolean flag = false;

			for(DoubleWritable t:values) {
				count++;
				flag = ((count - first_sampling) % sampling_distance == 0);
				if(flag) {
					context.write(key, t);
				}
			}
		}
	}
	
	public static class CriticalValueMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		private static IntWritable no = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
			String[] words = value.toString().split("\t");
			String rating = words[1];
			context.write(no, new DoubleWritable(Double.parseDouble(rating)));
		}
	}
	
	public static class CriticalValueReducer extends Reducer<IntWritable, DoubleWritable, DoubleWritable, Text> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			List<Double> list = new ArrayList<Double>();
			for (DoubleWritable d : values) {
				list.add(Double.parseDouble(d.toString()));
			}
			Collections.sort(list);
			rating_down = list.get((int) (0.01 * list.size()));
			rating_up = list.get((int) (0.99 * list.size()));
		}
	}
	 
	public static class DataFilterAndNormal_Mapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat df2 = new SimpleDateFormat("MMMM dd,yyyy");
			SimpleDateFormat df3 = new SimpleDateFormat("yyyy/MM/dd");
			DecimalFormat df4 =new DecimalFormat("#0.00");
			
			String pattern1 = "\\d{4}-\\d{1,2}-\\d{1,2}";
			String pattern2 = "[a-zA-Z]+ \\d{1,2},\\d{4}";
			String pattern3 = "(^(-?\\d+)(\\.\\d+)?)℉";
			Pattern p1 = Pattern.compile(pattern1);
			Pattern p2 = Pattern.compile(pattern2);
			Pattern p3 = Pattern.compile(pattern3);
			Matcher m1, m2, m3;
			
			String[] words = value.toString().split("\\|");
			double longitude = Double.parseDouble(words[1]);
			double latitude = Double.parseDouble(words[2]);
			String review_date = words[4];
			String user_birthday = words[8];
			String temperature = words[5];
			String rating = words[6];
			
			String new_review_date = review_date;
			String new_user_birthday = user_birthday;
			String new_temperature = temperature;
			
			try {
				m1 = p1.matcher(review_date);
				m2 = p2.matcher(review_date);
				if(m1.find()) {
					new_review_date = df3.format(df1.parse(review_date));
				} 
				if(m2.find()) {
					new_review_date = df3.format(df2.parse(review_date));
				}
				m1 = p1.matcher(user_birthday);
				m2 = p2.matcher(user_birthday);
				if(m1.find()) {
					new_user_birthday = df3.format(df1.parse(user_birthday));
				} 
				if(m2.find()) {
					new_user_birthday = df3.format(df2.parse(user_birthday));
				}
				
				m3 = p3.matcher(temperature);
				if(m3.find()) {
					double temp = Double.parseDouble(m3.group(1));
					temp = (temp - 32) / 1.8;
					new_temperature = df4.format(temp) + "℃";
				}
				
				String text = value.toString().replace(review_date, new_review_date);
				text = text.replace(user_birthday, new_user_birthday);
				text = text.replace(temperature, new_temperature);
				
				Configuration conf = context.getConfiguration();
				if (longitude > conf.getDouble("longitude_down", 0) && longitude < conf.getDouble("longitude_up", 180)
						&& latitude > conf.getDouble("latitude_down", 0) && latitude < conf.getDouble("latitude_up", 90)) {
					context.write(new Text(rating), new Text(text));
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class DataFilterAndNormal_Reducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			DecimalFormat df =new DecimalFormat("#0.00");
			String rating = key.toString();
			if(rating.equals("?")) {
				for(Text value:values) {
					context.write(new Text(value), null);
				}
			} else {
				double rating_double = Double.parseDouble(rating);
				if(rating_double < rating_up && rating_double > rating_down) {
					
					double new_rating = (rating_double - rating_down) / (rating_up - rating_down);
					for(Text value:values) {
						String text = value.toString();
						text = text.replace(rating, String.valueOf(df.format(new_rating)));
						context.write(new Text(text), null);
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		conf.setDouble("longitude_down", 8.1461259);
		conf.setDouble("longitude_up", 11.1993265);
		conf.setDouble("latitude_down", 56.5824856);
		conf.setDouble("latitude_up", 57.750511);

		Job job1 = Job.getInstance(conf, "Sample");
		job1.setJarByClass(Combine.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapperClass(Combine.SampleMapper.class);
		job1.setReducerClass(Combine.SampleReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);

		Path outputPath = new Path(otherArgs[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

		job1.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "Critical value");
		job2.setJarByClass(Combine.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(Combine.CriticalValueMapper.class);
		job2.setReducerClass(Combine.CriticalValueReducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		outputPath = new Path(otherArgs[2]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		
		job2.waitForCompletion(true);
		outputPath = new Path(otherArgs[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		Job job3 = Job.getInstance(conf, "Data Filter and Normal");
		job3.setJarByClass(Combine.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setMapperClass(Combine.DataFilterAndNormal_Mapper.class);
		job3.setReducerClass(Combine.DataFilterAndNormal_Reducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		outputPath = new Path(otherArgs[2]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job3, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[2]));
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}

}
