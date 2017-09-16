package web.kpi;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class DataCleaner {
    
    static class LogMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        LogParser logParser = new LogParser();
        Text v2 = new Text();
        protected void map(LongWritable k1, Text v1, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
            try {
                String[] parse = logParser.parse(v1.toString());
                //过滤空白行
                if(parse != null) {
                    //过滤结尾的特定格式字符串
                    if(parse[2].endsWith(" HTTP/1.1")){
                        parse[2] = parse[2].substring(0, parse[2].length()-" HTTP/1.1".length());
                    }
                    
                    v2.set(parse[0]+'\t'+parse[1]+'\t'+parse[2]);
                }
            } catch (Exception e) {
                System.out.println("当前行处理出错："+v1.toString());
            }
            context.write(v2, new LongWritable(1));
        };
    }
    
    static class LogReduce extends Reducer<Text, LongWritable, Text, NullWritable>{
        protected void reduce(Text k2, java.lang.Iterable<LongWritable> v2s, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,NullWritable>.Context context) throws java.io.IOException ,InterruptedException {
            context.write(k2, NullWritable.get());
        };
    }
    
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:/desktop/hadoop-2.6.0");
        Configuration conf = new Configuration();
        conf.setStrings("dfs.nameservices", "cluster1");
        conf.setStrings("dfs.ha.namenodes.cluster1", "hadoop0,hadoop1");
        conf.setStrings("dfs.namenode.rpc-address.cluster1.hadoop0", "hadoop0:9000");
        conf.setStrings("dfs.namenode.rpc-address.cluster1.hadoop1", "hadoop1:9000");
        //必须配置，可以通过该类获取当前处于active状态的namenode
        conf.setStrings("dfs.client.failover.proxy.provider.cluster1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        
        
        Job job = Job.getInstance(conf, "LogDataCleaner");
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
          System.err.println("Usage: wordcount <in> [<in>...] <out>");
          System.exit(2);
        }
        
            
        // 删除已存在的输出目录
        String FILE_OUT_PATH = otherArgs[otherArgs.length - 1];
        //String FILE_OUT_PATH = "hdfs://cluster1/hmbbs_cleaned/2013_05_30";
         FileSystem fileSystem = FileSystem.get(new URI(FILE_OUT_PATH), conf);
         if (fileSystem.exists(new Path(FILE_OUT_PATH))) {
             fileSystem.delete(new Path(FILE_OUT_PATH), true);
         }
        
        job.setJarByClass(DataCleaner.class);
        //1.1 设置分片函数
        job.setInputFormatClass(TextInputFormat.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
          FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //FileInputFormat.addInputPath(job, new Path("hdfs://cluster1/hmbbs_logs/access_2013_05_30.log"));
        //1.2 设置map
        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //1.3 设置分区函数
        job.setPartitionerClass(HashPartitioner.class);
        //job.setNumReduceTasks(3);
        //1.4 分组排序
        //1.5 规约
        
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(FILE_OUT_PATH));
        
        //2.2 设置Reduce
        job.setReducerClass(LogReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.waitForCompletion(true);
    }
    
    static class LogParser {

        public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        public static final SimpleDateFormat dateformat1=new SimpleDateFormat("yyyyMMddHHmmss");
//        public static void main(String[] args) throws ParseException {
//            final String S1 = "27.19.74.143 - - [30/May/2013:17:38:20 +0800] \"GET /static/image/common/faq.gif HTTP/1.1\" 200 1127";
//            LogParser parser = new LogParser();
//            final String[] array = parser.parse(S1);
//            System.out.println("样例数据： "+S1);
//            System.out.format("解析结果：  ip=%s, time=%s, url=%s, status=%s, traffic=%s", array[0], array[1], array[2], array[3], array[4]);
//        }
        /**
         * 解析英文时间字符串
         * @param string
         * @return
         * @throws ParseException
         */
        private Date parseDateFormat(String string){
            Date parse = null;
            try {
                parse = FORMAT.parse(string);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return parse;
        }
        /**
         * 解析日志的行记录
         * @param line
         * @return 数组含有5个元素，分别是ip、时间、url、状态、流量
         */
        public String[] parse(String line){
            if(line.trim() == "") {
                return null;
            }
            String ip = parseIP(line);
            String time = parseTime(line);
            String url = parseURL(line);
            String status = parseStatus(line);
            String traffic = parseTraffic(line);
            
            return new String[]{ip, time ,url, status, traffic};
        }
        
        private String parseTraffic(String line) {
            final String trim = line.substring(line.lastIndexOf("\"")+1).trim();
            String traffic = trim.split(" ")[1];
            return traffic;
        }
        private String parseStatus(String line) {
            final String trim = line.substring(line.lastIndexOf("\"")+1).trim();
            String status = trim.split(" ")[0];
            return status;
        }
        private String parseURL(String line) {
            final int first = line.indexOf("\"");
            final int last = line.lastIndexOf("\"");
            String url = line.substring(first+1, last);
            return url;
        }
        private String parseTime(String line) {
            final int first = line.indexOf("[");
            final int last = line.indexOf("+0800]");
            String time = line.substring(first+1,last).trim();
            Date date = parseDateFormat(time);
            return dateformat1.format(date);
        }
        private String parseIP(String line) {
            String ip = line.split("- -")[0].trim();
            return ip;
        }
        
    }
}
