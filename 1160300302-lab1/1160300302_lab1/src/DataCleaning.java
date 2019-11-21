import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

public class DataCleaning {

	public static class FillIncomeMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split("\\|");
			String nation = words[9];
			String career = words[10];
			context.write(new Text(nation + career), value);
		}
	}
	
	public static class FillIncomeReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double income_fill = 0;
			
			boolean flag = false;
			for(Text val:values) {
				String income = val.toString().split("\\|")[11];
				if(flag == false && !income.equals("?")) {
					flag = true;
					income_fill = Double.parseDouble(income);
				}
				if(!income.equals("?")) {
					income_fill = 0.95 * income_fill + 0.05 * Double.parseDouble(income);
					context.write(val, null);
				} else {
					String[] words = val.toString().split("\\|");
					StringBuilder text = new StringBuilder();
					for(int j=0; j<words.length - 1; j++) {
						text.append(words[j] + "|");
					}
					text.append((int)income_fill);
					context.write(new Text(text.toString()), null);
				}
			}
		}
	}
	
	public static class FillRatingMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split("\\|");
			String longitude = words[1];
			String latitude = words[2];
			String altitude = words[3];
			context.write(new Text(longitude + latitude + altitude), value);
		}
	}
	
	public static class FillRatingReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<Text> missing_list = new ArrayList<Text>();
			Map<Double, Double> map = new TreeMap<Double, Double>();
			
			DecimalFormat df =new DecimalFormat("#0.00");
			
			int K = 3;
			
			for(Text value:values) {
				String[] words = value.toString().split("\\|");
				String rating = words[6];
				if(!rating.equals("?")) {
					map.put(Double.valueOf(words[11]), Double.valueOf(rating));
					context.write(value, null);
				} else {
					missing_list.add(value);
				}
			}
			
			for(int i = 0; i<missing_list.size(); i++) {
				String val = missing_list.get(i).toString();
				if(map.size() == 0) {
					val = val.replace("?", "0.50");
					context.write(new Text(val), null);
				} else {
					double income = Double.parseDouble(val.split("\\|")[11]);
					Map<Double, Double> dValues = new TreeMap<Double, Double>();
					for (Map.Entry<Double, Double> entry : map.entrySet()) {
						dValues.put(Math.abs(entry.getKey() - income), entry.getValue());
					}
					Map<Double, Double> resultMap = sortMapByKey(map);
					 
					int count = 0;
					double sum = 0;
					for (Map.Entry<Double, Double> entry : resultMap.entrySet()) {
						count++;
						sum += entry.getValue();
						if(count == K) {
							break;
						}
					}
					
					val = missing_list.get(i).toString();
					val = val.replace("?", String.valueOf(df.format(sum / count)));

					context.write(new Text(val), null);
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
		
		Job job1 = Job.getInstance(conf, "Fill Income");
		job1.setJarByClass(DataCleaning.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapperClass(DataCleaning.FillIncomeMapper.class);
		job1.setReducerClass(DataCleaning.FillIncomeReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		Path outputPath = new Path(otherArgs[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

		job1.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "Fill Rating");
		job2.setJarByClass(DataCleaning.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(DataCleaning.FillRatingMapper.class);
		job2.setReducerClass(DataCleaning.FillRatingReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		outputPath = new Path(otherArgs[2]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		
		job2.waitForCompletion(true);
		outputPath = new Path(otherArgs[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
	}
	
	public static Map<Double, Double> sortMapByKey(Map<Double, Double> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<Double, Double> sortMap = new TreeMap<Double, Double>(
                new MapKeyComparator());

        sortMap.putAll(map);

        return sortMap;
    }
	
	public static class MapKeyComparator implements Comparator<Double>{
	    @Override
	    public int compare(Double d1, Double d2) {
	        return d1.compareTo(d2);
	    }
	}
}



