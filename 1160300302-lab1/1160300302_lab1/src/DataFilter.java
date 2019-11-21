import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

public class DataFilter {

	private static double rating_down;
	private static double rating_up;

	public static class DataFilterMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split("\\|");
			double longitude = Double.parseDouble(words[1]);
			double latitude = Double.parseDouble(words[2]);
			String rating = words[6];

			Configuration conf = context.getConfiguration();
			if (longitude > conf.getDouble("longitude_down", 0) && longitude < conf.getDouble("longitude_up", 180)
					&& latitude > conf.getDouble("latitude_down", 0) && latitude < conf.getDouble("latitude_up", 90)) {
				context.write(new Text(rating), value);
			}
		}
	}

	public static class DataFilterReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (key.toString().equals("?")) {
				for (Text value : values) {
					context.write(value, null);
				}
			} else {
				for (Text value : values) {
					String rating = value.toString().split("\\|")[6];
					if (Double.parseDouble(rating) > rating_down && Double.parseDouble(rating) < rating_up) {
						context.write(value, null);
					}
				}
			}
		}
	}

	public static class StatisticMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		private static IntWritable no = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split("\\|");
			String rating = words[6];
			if (!rating.equals("?")) {
				context.write(no, new DoubleWritable(Double.parseDouble(rating)));
			}
		}
	}

	public static class StatisticReducer extends Reducer<IntWritable, DoubleWritable, DoubleWritable, Text> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Reducer<IntWritable, DoubleWritable, DoubleWritable, Text>.Context context)
				throws IOException, InterruptedException {
			List<Double> list = new ArrayList<Double>();
			for (DoubleWritable d : values) {
				list.add(Double.parseDouble(d.toString()));
			}
			Collections.sort(list);
			for (Double d : list) {
				context.write(new DoubleWritable(d), new Text(""));
			}
			rating_down = list.get((int) (0.01 * list.size()));
			rating_up = list.get((int) (0.99 * list.size()));
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

		Job job1 = Job.getInstance(conf, "statistic rating");
		job1.setJarByClass(DataFilter.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapperClass(DataFilter.StatisticMapper.class);
		job1.setReducerClass(DataFilter.StatisticReducer.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);

		Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 1]));

		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "data filter");
		job2.setJarByClass(DataFilter.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(DataFilter.DataFilterMapper.class);
		job2.setReducerClass(DataFilter.DataFilterReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		outputPath = new Path(otherArgs[otherArgs.length - 1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

}
