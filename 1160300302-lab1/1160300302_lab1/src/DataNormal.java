import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DataNormal {

	private static double min;
	private static double max;
	
	public static class NormalMapper extends Mapper<Object, Text, Text, Text> {
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
			Matcher m1, m2;
			
			String[] words = value.toString().split("\\|");
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
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			Matcher m3 = Pattern.compile(pattern3).matcher(temperature);
			if(m3.find()) {
				double temp = Double.parseDouble(m3.group(1));
				temp = (temp - 32) / 1.8;
				new_temperature = df4.format(temp) + "℃";
			}
			
			if(!rating.equals("?")) {
				double d = Double.parseDouble(rating);
				if(d > max) {
					max = d;
				}
				if(d < min) {
					min = d;
				}
			}
			String text = value.toString().replace(review_date, new_review_date);
			text = text.replace(user_birthday, new_user_birthday);
			text = text.replace(temperature, new_temperature);
			context.write(new Text(rating), new Text(text));
		}
	}
	
	public static class NormalReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			DecimalFormat df =new DecimalFormat("#0.00");
			String rating = key.toString();
			
			if(!rating.equals("?")) {
				double new_rating = (Double.parseDouble(rating) - min) / (max - min);
				for(Text value:values) {
					String text = value.toString();
					text = text.replace(rating, String.valueOf(df.format(new_rating)));
					context.write(new Text(text), null);
				}
			} else {
				for(Text value:values) {
					context.write(new Text(value), null);
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
		
		min = 100;
        max = 0;
		Job job = Job.getInstance(conf, "DateNormal");
		job.setJarByClass(DataNormal.class);
		job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(DataNormal.NormalMapper.class);
        job.setReducerClass(DataNormal.NormalReducer.class);
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);
        
        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
	}

}
