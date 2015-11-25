package com.bi.newlold.homeserver.mergedhs.middle;

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.paramparse.AbstractCommandParamParse;
import com.bi.newlold.homeserver.mergedhs.middle.HistoryMonthUserMR.HistoryMonthMapper;
import com.bi.newlold.homeserver.mergedhs.middle.HistoryMonthUserMR.HistoryMonthReducer;

public class HistoryMonthUser extends Configured implements Tool {
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        ParamParse paramParse = new ParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new HistoryMonthUser(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        String currentMonthInputPath = args[0];
        conf.set("current", getFirstDayOfMonth(currentMonthInputPath));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parseDate(getFirstDayOfMonth(currentMonthInputPath)));
        calendar.setTime(parseDate(getFirstDayOfMonth(currentMonthInputPath)));
        calendar.add(Calendar.MONTH, 1);// 加上一个月，变为下月的1号
        conf.set("next", dateFormat.format(calendar.getTime()));
        calendar.add(Calendar.MONTH, -2);// 减2个月，变为上月的1号
        conf.set("last", dateFormat.format(calendar.getTime()));
        calendar.add(Calendar.MONTH, -1);// 减1个月，变为上月的1号
        conf.set("previous", dateFormat.format(calendar.getTime()));
        Job job = new Job(conf, "HistoryMonthUser-newolduser");
        job.setJarByClass(HistoryMonthUser.class);
        for (String path : getPreThreeMonthInputPaths(currentMonthInputPath)) {
            if(fileExist(path)){
                FileInputFormat.addInputPath(job, new Path(path));
            }
         
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(HistoryMonthMapper.class);
        job.setReducerClass(HistoryMonthReducer.class);
        job.setNumReduceTasks(15);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    private String[] getPreThreeMonthInputPaths(String currentMonthInputPath) {
        List<String> paths = new LinkedList<String>();
        paths.add(currentMonthInputPath);
        int length = currentMonthInputPath.length();
        String pathPrefix = currentMonthInputPath.substring(0, length - 8);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parseDate(getFirstDayOfMonth(currentMonthInputPath)));
        SimpleDateFormat dateFormat = new SimpleDateFormat("/yyyy/MM");
        calendar.add(Calendar.MONTH, 1);// 加上一个月，变为下月的1号
        paths.add(pathPrefix + dateFormat.format(calendar.getTime()));
        calendar.add(Calendar.MONTH, -2);// 减2个月，变为上月的1号
        paths.add(pathPrefix + dateFormat.format(calendar.getTime()));
        calendar.add(Calendar.MONTH, -1);// 减1个月，变为上月的1号
        paths.add(pathPrefix + dateFormat.format(calendar.getTime()));
        return paths.toArray(new String[paths.size()]);

    }

    private String getFirstDayOfMonth(String filePath) {
        int endIndex = filePath.length();
        String month = filePath.substring(endIndex - 2, endIndex);
        String year = filePath.substring(endIndex - 7, endIndex - 3);
        String day = "01";
        return year + month + day;

    }

    private Date parseDate(String dateString) {
        Date date = null;
        try {
            date = dateFormat.parse(dateString);
        }
        catch(ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return date;
    }
    
    private boolean fileExist(String path) 
    {
        String uri = path;
        Configuration conf = new  Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(URI.create(uri),conf);
            return fileSystem.getFileStatus(new Path(uri))!=null;
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        return false;
       
    }

    static class ParamParse extends AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            String functionDescrtiption = "process the near three month log";
            return "Function :  \n    " + functionDescrtiption + "\n";
        }

        @Override
        public String getFunctionUsage() {
            // TODO Auto-generated method stub
            String functionUsage = "hadoop jar historymonthuser.jar -i {input_path} -o {output_path} ";
            return "Usage :  \n    " + functionUsage + "\n";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            return new Option[0];
        }

    }

}
