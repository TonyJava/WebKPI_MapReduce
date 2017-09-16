package mapreduce.clean;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		LogParser logparser = new LogParser();
		String [] info = logparser.parse(value.toString());
		if(info[2].endsWith(" HTTP/1.1")) {
			info[2] = info[2].substring(0, info[2].length()-" HTTP/1.1".length());
		}
		value.set(info[0]+'\t'+info[1]+'\t'+info[2]);
		
		context.write(value, new IntWritable(1));
	}
}
