package gmm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
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

public class GMM {

	public static class PostProbabilityMapper extends Mapper<Object, Text, IntWritable, Text> {
		Map<Integer, BlockRealMatrix> means = new HashMap<Integer, BlockRealMatrix>();
		Map<Integer, BlockRealMatrix> covariances = new HashMap<Integer, BlockRealMatrix>();
		Map<Integer, Double> alphas = new HashMap<Integer, Double>();
		int k;
		
		public void setup(Context context) throws IOException, InterruptedException {
			Map<Integer, List<String>> tmp_means = getMeans(context.getConfiguration().get("meansPath"));
			for(Entry<Integer, List<String>> entry:tmp_means.entrySet()) {
				int i = entry.getKey();
				List<String> tmp_list = entry.getValue();
				String[] tmp_fields = tmp_list.get(1).split(",");
				
				double[][] matrix = new double[68][1];
				for(int j=0; j < tmp_fields.length; j++) {
					matrix[j] = new double[]{Double.parseDouble(tmp_fields[j])};
				}
				alphas.put(i, Double.parseDouble(tmp_list.get(0)));
				means.put(i, new BlockRealMatrix(matrix));
			}
			
			Map<Integer, String> tmp_covariances = getCovariances(context.getConfiguration().get("covariancesPath"));
			for(Entry<Integer, String> entry:tmp_covariances.entrySet()) {
				int i = entry.getKey();
				String[] tmp_fields = entry.getValue().split("\\|");
				
				double[][] matrix = new double[68][68];
				for(int j=0; j < tmp_fields.length; j++) {
					double[] line = new double[68];
					String[] tmp_array = tmp_fields[j].split(",");
					for(int m=0; m<tmp_array.length; m++) {
						line[m] = Double.parseDouble(tmp_array[m]);
					}
					matrix[j] = line;
				}
				covariances.put(i, new BlockRealMatrix(matrix));
			}
			k = alphas.size();
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			double[][] x = new double[68][1];
			
			for(int i=1; i<fields.length; i++) {
				x[i - 1] = new double[]{Double.parseDouble(fields[i])};
			}
			BlockRealMatrix x_matrix = new BlockRealMatrix(x);
			Map<Integer, Double> probabilitys = new HashMap<Integer, Double>();
			
			double sum = 0;
			for(int i=0; i<k; i++) {
				BlockRealMatrix tmp_matrix = x_matrix.subtract(means.get(i));
				double p = Math.exp(-0.5 * (tmp_matrix.transpose().multiply(new LUDecomposition(covariances.get(i)).getSolver().getInverse()).multiply(tmp_matrix)).getEntry(0, 0));
				double tmp = Math.pow(2 * Math.PI, 34) * Math.pow(new LUDecomposition(covariances.get(i)).getDeterminant(), 0.5);
				p /= tmp;
				probabilitys.put(i, p);
				sum += alphas.get(i) * p;
			}
			
			for(Entry<Integer, Double> entry:probabilitys.entrySet()) {
				int i = entry.getKey();
				context.write(new IntWritable(i), new Text((entry.getValue() * alphas.get(i)) / sum + "\t" + value.toString()));
			}
		}
	}
	
	public static class TrainMeanAndAlphaReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			int count = 0;
			double sum = 0;
			List<Double> mean_list = new ArrayList<Double>();
			
			for(Text t:value) {
				String[] fields = t.toString().split("\t");
				double post_probability = Double.parseDouble(fields[0]);
				sum += post_probability;
				count++;
				
				String[] words = fields[1].split(",");
				for(int i=1; i<words.length; i++) {
					if(mean_list.size() <= i - 1) {
						mean_list.add(post_probability * Double.parseDouble(words[i]));
					} else {
						mean_list.set(i - 1, mean_list.get(i - 1) + post_probability * Double.parseDouble(words[i]));
					}
				}
			}
			StringBuilder str = new StringBuilder();
			for(int i = 0; i < mean_list.size() - 1; i++) {
				str.append(mean_list.get(i) / sum + ",");
			}
			str.append(mean_list.get(mean_list.size() - 1) / sum);
			context.write(key, new Text(sum / count + "\t" + str));
		}
	}
	
	
	public static Map<Integer, List<String>> getMeans(String filePath) throws IOException {
		Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
		Path path = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);

		FSDataInputStream fsis = fileSystem.open(path);
		LineReader lineReader = new LineReader(fsis, conf);

		Text line = new Text();
		while (lineReader.readLine(line) > 0) {
			List<String> tmp_list = new ArrayList<String>();
			String[] fields = line.toString().split("\t");
			int y = Integer.parseInt(fields[0]);
			tmp_list.add(fields[1]);
			tmp_list.add(fields[2]);
			map.put(y, tmp_list);
		}
		lineReader.close();
		return map;
	}
	
	public static Map<Integer, String> getCovariances(String filePath) throws IOException {
		Map<Integer, String> map = new HashMap<Integer, String>();
		Path path = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fileSystem = path.getFileSystem(conf);

		FSDataInputStream fsis = fileSystem.open(path);
		LineReader lineReader = new LineReader(fsis, conf);

		Text line = new Text();
		while (lineReader.readLine(line) > 0) {
			String[] fields = line.toString().split("\t");
			map.put(Integer.parseInt(fields[0]), fields[1]);
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
	
	public static class TrainCovarianceReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		Map<Integer, BlockRealMatrix> means = new HashMap<Integer, BlockRealMatrix>();
		
		public void setup(Context context) throws IOException, InterruptedException {
			Map<Integer, List<String>> tmp_means = getMeans(context.getConfiguration().get("meansPath"));
			for(Entry<Integer, List<String>> entry:tmp_means.entrySet()) {
				int i = entry.getKey();
				List<String> tmp_list = entry.getValue();
				String[] tmp_fields = tmp_list.get(1).split(",");
				
				double[][] matrix = new double[68][1];
				for(int j=0; j < tmp_fields.length; j++) {
					matrix[j] = new double[]{Double.parseDouble(tmp_fields[j])};
				}
				means.put(i, new BlockRealMatrix(matrix));
			}
		}
		
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			int k = Integer.parseInt(key.toString());
			double sum = 0;
			
			RealMatrix covariance_matrix = null;
			
			for(Text t:value) {
				String[] fields = t.toString().split("\t");
				double post_probability = Double.parseDouble(fields[0]);
				sum += post_probability;
				
				String[] x_array = fields[1].split(",");
				double[][] x = new double[68][1];
				for(int i=1; i<x_array.length; i++) {
					x[i - 1] = new double[]{Double.parseDouble(x_array[i])};
				}
				
				RealMatrix x_matrix = new BlockRealMatrix(x);
				RealMatrix tmp_matrix = x_matrix.subtract(means.get(k));
				tmp_matrix = tmp_matrix.multiply(tmp_matrix.transpose());
				tmp_matrix = tmp_matrix.scalarMultiply(post_probability);
				
				if(covariance_matrix == null) {
					covariance_matrix = tmp_matrix;
				} else {
					covariance_matrix = covariance_matrix.add(tmp_matrix);
				}
				
			}
			
			covariance_matrix = covariance_matrix.scalarMultiply(1.0/sum);
			
			if((new LUDecomposition(covariance_matrix)).getDeterminant() == 0) {
				System.out.println("Singular!");
				for(int i=0; i<covariance_matrix.getRowDimension(); i++) {
					if(covariance_matrix.getEntry(i, i) < 0) {
						covariance_matrix.addToEntry(i, i, -0.0001);
					} else {
						covariance_matrix.addToEntry(i, i, 0.0001);
					}
				}
			}
			
			StringBuilder str = new StringBuilder();
			for(int i=0; i<covariance_matrix.getRowDimension() - 1; i++) {
				for(int j=0; j<covariance_matrix.getRowDimension() - 1; j++) {
					str.append(covariance_matrix.getEntry(i, j) + ",");
				}
				str.append(covariance_matrix.getEntry(i, covariance_matrix.getRowDimension() - 1) + "|");
			}
			for(int i=0; i<covariance_matrix.getRowDimension() - 1; i++) {
				str.append(covariance_matrix.getEntry(covariance_matrix.getRowDimension() - 1, i) + ",");
			}
			str.append(covariance_matrix.getEntry(covariance_matrix.getRowDimension() - 1, covariance_matrix.getRowDimension() - 1));
			context.write(key, new Text(str.toString()));
		}
	}
	
	public static class TestMapper extends Mapper<Object, Text, IntWritable, Text> {
		Map<Integer, BlockRealMatrix> means = new HashMap<Integer, BlockRealMatrix>();
		Map<Integer, BlockRealMatrix> covariances = new HashMap<Integer, BlockRealMatrix>();
		Map<Integer, Double> alphas = new HashMap<Integer, Double>();
		int k;
		
		public void setup(Context context) throws IOException, InterruptedException {
			Map<Integer, List<String>> tmp_means = getMeans(context.getConfiguration().get("meansPath"));
			for(Entry<Integer, List<String>> entry:tmp_means.entrySet()) {
				int i = entry.getKey();
				List<String> tmp_list = entry.getValue();
				String[] tmp_fields = tmp_list.get(1).split(",");
				
				double[][] matrix = new double[68][1];
				for(int j=0; j < tmp_fields.length; j++) {
					matrix[j] = new double[]{Double.parseDouble(tmp_fields[j])};
				}
				alphas.put(i, Double.parseDouble(tmp_list.get(0)));
				means.put(i, new BlockRealMatrix(matrix));
			}
			
			Map<Integer, String> tmp_covariances = getCovariances(context.getConfiguration().get("covariancesPath"));
			for(Entry<Integer, String> entry:tmp_covariances.entrySet()) {
				int i = entry.getKey();
				String[] tmp_fields = entry.getValue().split("\\|");
				
				double[][] matrix = new double[68][68];
				for(int j=0; j < tmp_fields.length; j++) {
					double[] line = new double[68];
					String[] tmp_array = tmp_fields[j].split(",");
					for(int m=0; m<tmp_array.length; m++) {
						line[m] = Double.parseDouble(tmp_array[m]);
					}
					matrix[j] = line;
				}
				covariances.put(i, new BlockRealMatrix(matrix));
			}
			k = alphas.size();
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			double[][] x = new double[68][1];
			
			for(int i=1; i<fields.length; i++) {
				x[i - 1] = new double[]{Double.parseDouble(fields[i])};
			}
			BlockRealMatrix x_matrix = new BlockRealMatrix(x);
			Map<Integer, Double> probabilitys = new HashMap<Integer, Double>();
			
			for(int i=0; i<k; i++) {
				BlockRealMatrix tmp_matrix = x_matrix.subtract(means.get(i));
				double p = Math.exp(-0.5 * (tmp_matrix.transpose().multiply(new LUDecomposition(covariances.get(i)).getSolver().getInverse()).multiply(tmp_matrix)).getEntry(0, 0));
				double tmp = Math.pow(2 * Math.PI, 34) * Math.pow(new LUDecomposition(covariances.get(i)).getDeterminant(), 0.5);
				p /= tmp;
				probabilitys.put(i, p);
			}
			
			int loc = 0;
			double max = 0;
			for(Entry<Integer, Double> entry:probabilitys.entrySet()) {
				int i = entry.getKey();
				double p = entry.getValue();
				if(p > max) {
					max = p;
					loc = i;
				}
			}
			context.write(new IntWritable(loc), value);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: GMM <in> [<in>...] <out>");
			System.exit(2);
		}
		
		conf.set("meansPath", otherArgs[1]);
		conf.set("covariancesPath", otherArgs[2]);
		
		int count = 0;
		while(count < 1) {
			Job job1 = Job.getInstance(conf, "compute means");
			job1.setJarByClass(GMM.class);
			job1.setMapperClass(PostProbabilityMapper.class);
			job1.setReducerClass(TrainMeanAndAlphaReducer.class);
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(IntWritable.class);
			job1.setOutputValueClass(Text.class);
	
			Path outputPath = new Path(otherArgs[3]);
			outputPath.getFileSystem(conf).delete(outputPath, true);
			
			FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));
			
			job1.waitForCompletion(true);
			updateFile(otherArgs[1], otherArgs[3]);
			
			Job job2 = Job.getInstance(conf, "compute covariance");
			job2.setJarByClass(GMM.class);
			job2.setMapperClass(PostProbabilityMapper.class);
			job2.setReducerClass(TrainCovarianceReducer.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);
	
			outputPath = new Path(otherArgs[3]);
			outputPath.getFileSystem(conf).delete(outputPath, true);
			
			FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
			
			job2.waitForCompletion(true);
			updateFile(otherArgs[2], otherArgs[3]);
			
			count++;
		}
		
		Job job = Job.getInstance(conf, "Test");
		job.setJarByClass(GMM.class);
		job.setMapperClass(TestMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		Path outputPath = new Path(otherArgs[3]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		
		job.waitForCompletion(true);
		
	}
}
