import java.io.IOException;
import java.util.Random;

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

public class GroupSampling {

	public static class GroupSamplingMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split("\\|");
			String career = words[10];
			context.write(new Text(career), value);
		}
	}

	public static class GroupSamplingReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			double sampling_ratio = 0.01;
			int sampling_distance = (int)(1 / sampling_ratio);
			int first_sampling = new Random().nextInt(sampling_distance - 1) + 1;
			int count = 0;
			boolean flag = false;

			for(Text t:values) {
				count++;
				flag = ((count - first_sampling) % sampling_distance == 0);
				if(flag) {
					context.write(t, new Text(""));
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if(otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "GroupSampling");
        job.setJarByClass(GroupSampling.class);
        job.setMapperClass(GroupSampling.GroupSamplingMapper.class);
        job.setReducerClass(GroupSampling.GroupSamplingReducer.class);
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        
        
        /* 删除输出目录 */
        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        for(int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
	}

}
