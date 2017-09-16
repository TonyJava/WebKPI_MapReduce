package mapreduce.clean;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text,IntWritable,Text,NullWritable>{
	public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
