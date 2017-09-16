package mapreduce.clean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class LogParser {
	public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    public static final SimpleDateFormat dateformat=new SimpleDateFormat("yyyyMMddHHmmss");


    private Date parseDateFormat(String string){
        Date parse = null;
        try {
            parse = FORMAT.parse(string);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return parse;
    }
    
    
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
        return dateformat.format(date);
    }
    //以ip后面的两个横线划分出ip地址
    private String parseIP(String line) {
        String ip = line.split("- -")[0].trim();
        return ip;
    }
}
