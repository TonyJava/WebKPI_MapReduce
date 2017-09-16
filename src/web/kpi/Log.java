package web.kpi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Log {
	static class LogParser {

        public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        public static final SimpleDateFormat dateformat1=new SimpleDateFormat("yyyyMMddHHmmss");


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
        	final int first = line.indexOf("\"");
            final int second = line.indexOf("\"", first+1);
            final int third = line.indexOf("\"", second+1);
            String traffic = line.substring(second+2, third).split(" ")[1];
            return traffic;
        }
        private String parseStatus(String line) {
            final int first = line.indexOf("\"");
            final int second = line.indexOf("\"", first+1);
            final int third = line.indexOf("\"", second+1);
            String status = line.substring(second+2, third).split(" ")[0];
            return status;
        }
        private String parseURL(String line) {
            final int first = line.indexOf("\"");
            final int next = line.indexOf("\"", first+1);
            String url = line.substring(first+1, next);
            return url;
        }
        private String parseTime(String line) {
            final int first = line.indexOf("[");
            final int last = line.indexOf("+0800]");
            String time = line.substring(first+1,last).trim();
            Date date = parseDateFormat(time);
            return dateformat1.format(date);
        }
        //以ip后面的两个横线划分出ip地址
        private String parseIP(String line) {
            String ip = line.split("- -")[0].trim();
            return ip;
        }
        
    }
	public static void main(String[] args) throws IOException {
		LogParser parser = new LogParser();
		File file = new File("E:\\大数据学习\\数据\\网站KPI\\part.txt");//Text文件
		BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
		String s = null;
		while((s = br.readLine())!=null){//使用readLine方法，一次读一行
			final String[] array = parser.parse(s);
		   System.out.println("样例数据： "+s);
		   System.out.format("解析结果：  ip=%s, time=%s, url=%s, status=%s, traffic=%s", array[0], array[1], array[2], array[3], array[4]);
		}
		br.close();
	
	}
}
