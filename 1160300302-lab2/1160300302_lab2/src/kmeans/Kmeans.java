package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class Kmeans {
	
	public static class selectCenterMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new IntWritable(1), value);
		}
	}
	
	public static class selectCenterReducer extends Reducer<IntWritable, Text, Text, Text> {
		int k = 6;
		Random rand = new Random();
		
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			List<List<Integer>> list = new ArrayList<List<Integer>>();
			Iterator<Text> it = value.iterator();
			while(it.hasNext()) {
				String[] fields = it.next().toString().split(",");
				List<Integer> tmp_list = new ArrayList<Integer>();
				for(int i=0; i<fields.length; i++) {
					tmp_list.add(Integer.valueOf(fields[i]));
				}
				list.add(tmp_list);
			}
			int size = list.size();
			int loc;
			List<Integer> tmp_list;
			for(int i=0; i < k; i++) {
				loc = rand.nextInt(size);
				tmp_list = list.get(loc);
				System.out.println(loc);
				System.out.println(tmp_list);
				String tmp_str = tmp_list.subList(1, tmp_list.size()).toString();
				String new_str = tmp_str.substring(1, tmp_str.length() - 1).replace(", ", ",");
				System.out.println(new_str);
				context.write(new Text(new_str), null);
			}
		}
	}

	public static class TrainMap extends Mapper<Object, Text, IntWritable, Text> {
		List<List<Double>> centers;
		int k;

		public void setup(Context context) throws IOException, InterruptedException {
			centers = getCenters(context.getConfiguration().get("centersPath"));
			k = centers.size();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<Double> field_list = new ArrayList<Double>();
			String[] fields = value.toString().split(",");
			for (int i = 0; i < fields.length; i++) {
				field_list.add(Double.parseDouble(fields[i]));
			}
			
			double min = Double.MAX_VALUE;
			int center_loc = 0;

			for (int i = 0; i < k; i++) {
				double tmp_distance = 0;
				for (int j = 1; j < field_list.size(); j++) {
					double tmp_center = Math.abs(centers.get(i).get(j - 1));
					double tmp_filed = Math.abs(field_list.get(j));
					tmp_distance += Math.pow(tmp_center - tmp_filed, 2);
				}
				
				if (tmp_distance < min) {
					min = tmp_distance;
					center_loc = i;
				}
			}
			context.write(new IntWritable(center_loc + 1), value);
		}
	}

	public static class TrainReduce extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			Iterator<Text> it = value.iterator();
			int count = 0;
			
			List<Double> sum= new ArrayList<Double>();
			while(it.hasNext()) {
				String[] fields = it.next().toString().split(",");
				for(int i=1; i<fields.length; i++) {
					if((i-1) >= sum.size()) {
						sum.add(Double.valueOf(fields[i]));
					} else {
						double tmp_sum = sum.get(i - 1);
						sum.set(i - 1, tmp_sum + Double.valueOf(fields[i]));
					}
				}
				count++;
			}
			
			int sum_size = sum.size();
			StringBuilder str = new StringBuilder();
			for(int i=0; i<sum_size - 1; i++) {
				sum.set(i, sum.get(i) / count);
				str.append(sum.get(i) + ",");
			}
			
			sum.set(sum_size - 1, sum.get(sum_size - 1) / count);
			str.append(sum.get(sum_size - 1));

			context.write(new Text(str.toString()), null);
		}

	}
	
	public static void updateCenters(String oldPath, String newPath) throws IOException {
		Configuration conf = new Configuration();
		Path outPath = new Path(oldPath);
		FileSystem fileSystem = outPath.getFileSystem(conf);

		FSDataOutputStream overWrite = fileSystem.create(outPath, true);
		overWrite.writeChars("");
		overWrite.close();

		Path inPath = new Path(newPath);
		FileStatus[] listFiles = fileSystem.listStatus(inPath);
		for (int i = 0; i < listFiles.length; i++) {
			FSDataOutputStream out = fileSystem.create(outPath);
			FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
			IOUtils.copyBytes(in, out, 4096, true);
		}
	}
	
	public static List<List<Double>> getCenters(String filePath) throws IOException {
		List<List<Double>> list = new ArrayList<List<Double>>();
		Path path = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);

		FSDataInputStream fsis = fileSystem.open(path);
		LineReader lineReader = new LineReader(fsis, conf);

		Text line = new Text();
		while (lineReader.readLine(line) > 0) {
			List<Double> tmp_list = new ArrayList<Double>();
			String[] fields = line.toString().split(",");
			for(int i=0; i<fields.length; i++) {
				tmp_list.add(Double.parseDouble(fields[i]));
			}
			list.add(tmp_list);
		}
		lineReader.close();
		return list;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Kmeans <in> [<in>...] <out>");
			System.exit(2);
		}
	
		conf.set("centersPath", otherArgs[1]);
		
		Job first_job = Job.getInstance(conf, "initial centers");
		first_job.setJarByClass(Kmeans.class);
		first_job.setMapperClass(selectCenterMapper.class);
		first_job.setReducerClass(selectCenterReducer.class);
		first_job.setMapOutputKeyClass(IntWritable.class);
		first_job.setMapOutputValueClass(Text.class);

		Path outputPath = new Path(otherArgs[2]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(first_job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(first_job, new Path(otherArgs[2]));
		first_job.waitForCompletion(true);
		
		updateCenters(args[1], args[2]);
		
		int count = 0;
		while(count < 10) {
			count++;
			Job job = Job.getInstance(conf, "Kmeans");
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(TrainMap.class);
			job.setReducerClass(TrainReduce.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			outputPath = new Path(otherArgs[2]);
			outputPath.getFileSystem(conf).delete(outputPath, true);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			job.waitForCompletion(true);
		}
		
		Job job = Job.getInstance(conf, "Kmeans map stage");
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(TrainMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		outputPath = new Path(otherArgs[2]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.waitForCompletion(true);

	}
}
