package logistic;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

public class Logistic {

	public static class TrainWeightMapper extends Mapper<Object, Text, IntWritable, Text> {
		Map<Integer, Double> weights;
		
		public void setup(Context context) throws IOException, InterruptedException {
			weights = getWeights(context.getConfiguration().get("weightPath"));
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String y = value.toString().split("\t")[0];
			String[] fields = value.toString().split("\t")[1].split(",");
			double tmp = weights.get(0);
			
			for(int i=0; i<fields.length; i++) {
				tmp += weights.get(i + 1) * Double.parseDouble(fields[i]);
			}
			double sigmoid = Math.exp(tmp) / (1 + Math.exp(tmp));
			context.write(new IntWritable(0), new Text(y + "," + 1.0 + "," + sigmoid));
			for(int i=0; i<fields.length; i++) {
				context.write(new IntWritable(i + 1), new Text(y + "," + fields[i] + "," + sigmoid));
			}
		}
	}

	public static class TrainWeightReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
		Map<Integer, Double> weights;
		double previous_weight;
		double alpha = 0.000001;
		
		public void setup(Context context) throws IOException, InterruptedException {
			weights = getWeights(context.getConfiguration().get("weightPath"));
		}
		
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			previous_weight = weights.get(Integer.parseInt(key.toString()));
			double gradient = 0;
			for(Text t:value) {
				String[] text = t.toString().toString().split(",");
				gradient += (Double.parseDouble(text[0]) - Double.parseDouble(text[2])) * Double.parseDouble(text[1]);
			}
			double distance = gradient * alpha;
			context.write(new Text(key + "," + (distance + previous_weight)), null);
		}
	}
	
	public static Map<Integer, Double> getWeights(String filePath) throws IOException {
		Map<Integer, Double> map = new HashMap<Integer, Double>();
		Path path = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);

		FSDataInputStream fsis = fileSystem.open(path);
		LineReader lineReader = new LineReader(fsis, conf);

		Text line = new Text();
		while (lineReader.readLine(line) > 0) {
			String[] fields = line.toString().split(",");
			int y = Integer.parseInt(fields[0]);
			double weight = Double.parseDouble(fields[1]);
			map.put(y, weight);
		}
		lineReader.close();
		return map;
	}
	
	public static void updateFile(String oldPath, String newPath) throws IOException {
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
	
	public static class LikeHoodFunctionMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		Map<Integer, Double> weights_pre;
		Map<Integer, Double> weights_new;
		IntWritable key_old = new IntWritable(0);
		IntWritable key_new = new IntWritable(1);
		
		public void setup(Context context) throws IOException, InterruptedException {
			weights_new = getWeights(context.getConfiguration().get("weightPath"));
			weights_pre = getWeights(context.getConfiguration().get("weightPrePath"));
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			double tmp_pre = weights_pre.get(0);
			double tmp_new = weights_pre.get(0);
			String y = value.toString().split("\t")[0];
			String[] fields = value.toString().split("\t")[1].split(",");
			
			for(int i=0; i<fields.length; i++) {
				tmp_pre += weights_pre.get(i + 1) * Double.parseDouble(fields[i]);
				tmp_new += weights_new.get(i + 1) * Double.parseDouble(fields[i]);
			}
			
			double lw_pre = Double.parseDouble(y) * tmp_pre - Math.log(1 + Math.exp(tmp_pre));
			double lw_new = Double.parseDouble(y) * tmp_new - Math.log(1 + Math.exp(tmp_new));
			context.write(key_new, new DoubleWritable(lw_new));
			context.write(key_old, new DoubleWritable(lw_pre));
		}
	}
	
	public static class LikeHoodFunctionReducer extends Reducer<IntWritable, DoubleWritable, Text, DoubleWritable> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
			double lw = 0;
			for(DoubleWritable d:value) {
				lw += Double.parseDouble(d.toString());
			}
			context.write(new Text(key + "," + lw), null);
		}
	}
	
	public static boolean idEnd(String filePath) throws IOException {
		double seita = 1000;
		
		Map<Integer, Double> map = new HashMap<Integer, Double>();
		Path path = new Path(filePath);

		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);
		FileStatus[] listFile = fileSystem.listStatus(path);
		for (int i = 0; i < listFile.length; i++) {
			path = new Path(listFile[i].getPath().toString());
			conf = new Configuration();
			fileSystem = path.getFileSystem(conf);
			
			FSDataInputStream fsis = fileSystem.open(path);
			LineReader lineReader = new LineReader(fsis, conf);

			Text line = new Text();
			while (lineReader.readLine(line) > 0) {
				String[] fields = line.toString().split(",");
				int y = Integer.parseInt(fields[0]);
				double lw = Double.parseDouble(fields[1]);
				map.put(y, lw);
			}
			lineReader.close();
		}
		System.out.println(map);
		if(Math.abs(map.get(0) - map.get(1)) < seita) {
			return true;
		} else {
			return false;
		}
	}
	
	public static class TestMapper extends Mapper<Object, Text, Text, Text> {
		Map<Integer, Double> weights;
		
		public void setup(Context context) throws IOException, InterruptedException {
			weights = getWeights(context.getConfiguration().get("weightPath"));
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String real_y = value.toString().split("\t")[0];
			String classify_y;
			String[] fields = value.toString().split("\t")[1].split(",");
			double tmp = weights.get(0);
			for(int i=0; i<fields.length; i++) {
				tmp += weights.get(i + 1) * Double.parseDouble(fields[i]);
			}
			
			double p = Math.exp(tmp) / (1 + Math.exp(tmp));
			if(p > 0.5) {
				classify_y = "1";
			} else {
				classify_y = "0";
			}
			context.write(new Text(real_y), new Text(classify_y));
			
		}	
	}
	
	public static double correct_rate(String filePath) throws IOException {
		double count = 0;
		double correct_count = 0;
		
		Path path = new Path(filePath);

		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);
		FileStatus[] listFile = fileSystem.listStatus(path);
		for (int i = 0; i < listFile.length; i++) {
			path = new Path(listFile[i].getPath().toString());
			conf = new Configuration();
			fileSystem = path.getFileSystem(conf);
			
			FSDataInputStream fsis = fileSystem.open(path);
			LineReader lineReader = new LineReader(fsis, conf);

			Text line = new Text();
			while (lineReader.readLine(line) > 0) {
				String[] fields = line.toString().split("\t");
				count++;
				if(fields[0].equals(fields[1])) {
					correct_count++;
				}
			}
			lineReader.close();
		}
		return correct_count / count;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Logistic <in> [<in>...] <out>");
			System.exit(2);
		}
		
		conf.set("weightPath", otherArgs[2]);
		conf.set("weightPrePath", otherArgs[3]);
		
		int count = 0;
		while(true) {
			count++;
			Job job1 = Job.getInstance(conf, "Logistic Train Weights");
			job1.setJarByClass(Logistic.class);
			job1.setMapperClass(TrainWeightMapper.class);
			job1.setReducerClass(TrainWeightReducer.class);
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(DoubleWritable.class);
	
			Path outputPath = new Path(otherArgs[4]);
			outputPath.getFileSystem(conf).delete(outputPath, true);
			
			FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[4]));
			job1.waitForCompletion(true);
			
			updateFile(otherArgs[2], otherArgs[4]);
			
			Job job2 = Job.getInstance(conf, "LikeHood Function");
			job2.setJarByClass(Logistic.class);
			job2.setMapperClass(LikeHoodFunctionMapper.class);
			job2.setReducerClass(LikeHoodFunctionReducer.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(DoubleWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);
	
			outputPath = new Path(otherArgs[4]);
			outputPath.getFileSystem(conf).delete(outputPath, true);
			
			FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
			job2.waitForCompletion(true);
			
			System.out.println(count);
			if(idEnd(otherArgs[4]) || count >= 1) {
				break;
			} else {
				updateFile(otherArgs[3], otherArgs[2]);
			}
		}
		
		Job job3 = Job.getInstance(conf, "Test");
		job3.setJarByClass(Logistic.class);
		job3.setMapperClass(TestMapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		Path outputPath = new Path(otherArgs[4]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job3, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
		job3.waitForCompletion(true);
		
		System.out.println("correct rate: " + correct_rate(otherArgs[4]));

	}
	
}
