package mapreduce.clean;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanData {

	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("path error!!!");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(CleanData.class);
		job.setJobName("CleanData");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true)? 0:1);

	}

}
