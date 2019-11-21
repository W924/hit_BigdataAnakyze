package naiveBayes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class NaiveBayes {

	public static class TrainPriorMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		IntWritable y0 = new IntWritable(0);
		IntWritable y1 = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String y = value.toString().split("\t")[0];
			if(y.equals("0")) {
				context.write(y0, y1);
			}
			if(y.equals("1")) {
				context.write(y1, y1);
			}
		}
	}
	
	public static class TrainPriorReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
		public void reduce(IntWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			int count = 0;
			for(@SuppressWarnings("unused") IntWritable i:value) {
				count++;
			}
			context.write(new Text(key + "," + count), null);
		}
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
	
	public static class TrainGaussianMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String y = value.toString().split("\t")[0];
			String[] fields = value.toString().split("\t")[1].split(",");
			for(int i=0; i<fields.length; i++) {
				context.write(new Text(y + "," + i), new Text(fields[i]));
			}
		}
	}
	
	public static class TrainGaussianReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			int y = Integer.parseInt(key.toString().split(",")[0]);
			int field = Integer.parseInt(key.toString().split(",")[1]);
			int count = 0;
			
			double mean = 0;
			double variance = 0;
			List<Double> list = new ArrayList<Double>();
			
			Iterator<Text> it = value.iterator();
			while(it.hasNext()) {
				count++;
				double t = Double.parseDouble(it.next().toString());
				mean += t;
				list.add(t);
			}
			mean /= count;
			for(int i=0; i<list.size(); i++) {
				variance += Math.pow(list.get(i) - mean, 2);
			}
			variance /= count;
			context.write(new Text(y + "," + field + "," + mean + "," + variance), null);
		}
	}
	
	public static class TestMapper extends Mapper<Object, Text, Text, Text> {
		double y_0;
		double y_1;
		Map<Integer, Map<Integer, List<Double>>> parameters;
		
		public void setup(Context context) throws IOException, InterruptedException {
			Map<Integer, Integer> map = getPrior(context.getConfiguration().get("priorProbabilityPath"));
			double tmp_y0 = map.get(0);
			double tmp_y1 = map.get(1);
			y_0 = tmp_y0 / (tmp_y0 + tmp_y1);
			y_1 = tmp_y1 / (tmp_y0 + tmp_y1);
			parameters = getGaussianParameter(context.getConfiguration().get("GaussianParameterPath"));
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String real_y = value.toString().split("\t")[0];
			String classify_y;
			String[] fields = value.toString().split("\t")[1].split(",");
			double classify_y0 = posterior_probability(y_0, parameters.get(0), fields);
			double classify_y1 = posterior_probability(y_1, parameters.get(1), fields);
			if(classify_y0 >= classify_y1) {
				classify_y = "0";
			} else {
				classify_y = "1";
			}
			context.write(new Text(real_y), new Text(classify_y));
			
			
		}
	}
	
	public static double posterior_probability(double y_pro, Map<Integer, List<Double>> parameters, String[] fields) {
		double result = 1;
		for(Entry<Integer, List<Double>> entry:parameters.entrySet()) {
			int field = entry.getKey();
			List<Double> parameter = entry.getValue();
			result *= normal_probability(Double.parseDouble(fields[field]), parameter.get(0), parameter.get(1));
		}
		return result * y_pro;
	}
	
	public static double normal_probability(double x, double mean, double variance) {
		return (1 / (Math.sqrt(variance * 2 * Math.PI))) * Math.exp(-( Math.pow(x - mean, 2)/ (2 * variance)));
	}
	
	public static Map<Integer, Integer> getPrior(String filePath) throws IOException {
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		Path path = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);

		FSDataInputStream fsis = fileSystem.open(path);
		LineReader lineReader = new LineReader(fsis, conf);

		Text line = new Text();
		while (lineReader.readLine(line) > 0) {
			String[] fields = line.toString().split(",");
			int y = Integer.parseInt(fields[0]);
			int number = Integer.parseInt(fields[1]);
			map.put(y, number);
		}
		lineReader.close();
		return map;
	}
	
	public static Map<Integer, Map<Integer, List<Double>>> getGaussianParameter(String filePath) throws IOException {
		Map<Integer, Map<Integer, List<Double>>> map = new HashMap<Integer, Map<Integer, List<Double>>>();
		Path path = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);

		FSDataInputStream fsis = fileSystem.open(path);
		LineReader lineReader = new LineReader(fsis, conf);
		Text line = new Text();
		while (lineReader.readLine(line) > 0) {
			String[] fields = line.toString().split(",");
			int y = Integer.parseInt(fields[0]);
			int field = Integer.parseInt(fields[1]);
			List<Double> parameter_list = new ArrayList<Double>();
			parameter_list.add(Double.parseDouble(fields[2]));
			parameter_list.add(Double.parseDouble(fields[3]));
			Map<Integer, List<Double>> tmp_map;
			if(map.get(y) != null) {
				tmp_map = map.get(y);
			} else {
				tmp_map = new HashMap<Integer, List<Double>>();
			}
			tmp_map.put(field, parameter_list);
			map.put(y, tmp_map);
		}
		lineReader.close();
		return map;
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
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: NaiveBayes <in> [<in>...] <out>");
			System.exit(2);
		}
		
		conf.set("priorProbabilityPath", otherArgs[2]);
		conf.set("GaussianParameterPath", otherArgs[3]);
		
		Job job1 = Job.getInstance(conf, "Navie Bayes Train Prior");
		job1.setJarByClass(NaiveBayes.class);
		job1.setMapperClass(TrainPriorMapper.class);
		job1.setReducerClass(TrainPriorReducer.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		Path outputPath = new Path(otherArgs[4]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[4]));
		job1.waitForCompletion(true);
		
		updateFile(otherArgs[2], otherArgs[4]);
		
		Job job2 = Job.getInstance(conf, "Navie Bayes Train Gaussian");
		job2.setJarByClass(NaiveBayes.class);
		job2.setMapperClass(TrainGaussianMapper.class);
		job2.setReducerClass(TrainGaussianReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		outputPath = new Path(otherArgs[4]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
		job2.waitForCompletion(true);
		
		updateFile(otherArgs[3], otherArgs[4]);
		
		Job job3 = Job.getInstance(conf, "Navie Bayes Test");
		job3.setJarByClass(NaiveBayes.class);
		job3.setMapperClass(TestMapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		outputPath = new Path(otherArgs[4]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job3, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
		job3.waitForCompletion(true);
		
		System.out.println("correct rate: " + correct_rate(otherArgs[4]));
		
	}
}

