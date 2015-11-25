package com.bi.client.fact.week;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WeekIndexMR extends Configured implements Tool {

	public static class HistoryMacMapper extends
	    Mapper<LongWritable, Text, Text, Text> {
		private Text newValue = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    newValue.set("o");  // "o" represent history mac
		    context.write(value, newValue);
		}
	}
		
	public static class LoginMapper extends
       Mapper<LongWritable, Text, Text, Text> {
       private Text newValue = new Text();
       private Text newKey   = new Text();
       public void map(LongWritable key, Text value, Context context)
               throws IOException, InterruptedException {           
           String[] fields = value.toString().split("\t");
           String date = fields[0];
           String ver  = fields[1];
           String area = fields[2];
           String mac  = fields[3];
           String isp  = fields[4];
           long  onlineTime= Long.parseLong(fields[14]) - Long.parseLong(fields[13]);
           //String downLoadFlux =  Integer.parseInt(fields[19]) >= 20 ? "1" : "0";
           String downLoadFlux = fields[19];
           newKey.set(mac);
           newValue.set("hs:" + date + "," + ver + "," + area + "," 
                       + isp + "," + onlineTime + "," + downLoadFlux);  
           context.write(newKey, newValue);
       }
	}
	
	public static class BootMapper extends
       Mapper<LongWritable, Text, Text, Text> {
       private Text newValue = new Text();
       private Text newKey   = new Text();
       public void map(LongWritable key, Text value, Context context)
               throws IOException, InterruptedException {           
           String[] fields = value.toString().split("\t");
           String date = fields[0];
           String ver  = fields[1];
           String area = fields[2];
           String mac  = fields[3];
           String isp  = fields[4];
           String startType = fields[5];
           newKey.set(mac);
           newValue.set("bt:"  + date + "," + ver + "," + area + ","  + isp + "," + startType);  
           context.write(newKey, newValue);
       }
	}    
	
	public static class InstallMapper extends
       Mapper<LongWritable, Text, Text, Text> {
       private Text newValue = new Text();
       private Text newKey   = new Text();
       public void map(LongWritable key, Text value, Context context)
               throws IOException, InterruptedException {           
           String[] fields = value.toString().split("\t");
           String date = fields[0];
           String ver  = fields[1];
           String area = fields[2];
           String mac  = fields[3];
           String isp  = fields[4];
           newKey.set(mac);
           newValue.set("in:" + date + "," +  ver + "," + area + ","  + isp );  
           context.write(newKey, newValue);
       }
   }
    
	public static class UninstallMapper extends
	    Mapper<LongWritable, Text, Text, Text> {
	    private Text newValue = new Text();
	    private Text newKey   = new Text();
	    public void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {           
	        String[] fields = value.toString().split("\t");
	        String ver  = fields[1];
	        String area = fields[2];
	        String mac  = fields[3];
	        String isp  = fields[4];
	        newKey.set(mac);
	        newValue.set("un:" + ver + "," + area + ","  + isp );  
	        context.write(newKey, newValue);
       }
   }
  
    public static class taskStatMapper extends
        Mapper<LongWritable, Text, Text, Text> {
        private Text newValue = new Text();
        private Text newKey   = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {           
            String[] fields = value.toString().split("\t");
            String ver  = fields[1];
            String area = fields[2];
            String mac  = fields[3];
            String isp  = fields[4];
            String pvs  = fields[9];  // working model
            if(pvs.equals("2")){
                newKey.set(mac);
                newValue.set("ts:" + ver + "," + area + ","  + isp );  
                context.write(newKey, newValue);
            }
        }
}
    
    public static class WtbhMapper extends
        Mapper<LongWritable, Text, Text, Text> {
        private Text newValue = new Text();
        private Text newKey   = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {           
            String[] fields = value.toString().split("\t");
            String ver  = fields[1];
            String area = fields[2];
            String mac  = fields[3];
            String isp  = fields[4];
            int  watchTime = Integer.parseInt(fields[5]) - Integer.parseInt(fields[15]);
            int  adjustWatchTime =  0;
             if (watchTime < 0){
                 adjustWatchTime = 0;
             }else if (watchTime < 86400 ){
                 adjustWatchTime = watchTime;
             }else{
                 adjustWatchTime = 86400;
             }
             
            newKey.set(mac);
            newValue.set("wt:" + ver + "," + area + ","  + isp + "," + adjustWatchTime);  
            context.write(newKey, newValue);
       }
    }
    	
    public static class stageCombiner extends
        Reducer <Text, Text, Text, Text> {
        public void reduce(Text Key, Iterable <Text> values, Context context) 
            throws IOException,InterruptedException{
            String str = "";
            for (Text val : values){
                if(str.length() > 0)
                    str += "\t";
                str += val.toString();
            }
            context.write(Key, new Text(str));         
        }           
    }
		
	public static class partResultReducer extends 
	    Reducer <Text, Text, Text, Text> {
	    private WeekIndexContainer weekIndex = new WeekIndexContainer();
	    private HashMap<String, WeekIndexContainer> areaWeekIndex = new HashMap<String, WeekIndexContainer>();
	    private HashMap<String, WeekIndexContainer> ispWeekIndex = new HashMap<String, WeekIndexContainer>();
	    private HashMap<String, WeekIndexContainer> versionWeekIndex = new HashMap<String, WeekIndexContainer>();
	    
	    private HashMap<String, ArrayList<String>> hashMap = new HashMap<String, ArrayList<String>>();
	    
	    private HashMap<String, Integer> flux = new HashMap<String, Integer>(7); 
        private HashMap<String, HashMap<String, Integer>> areaFlux = new HashMap<String, HashMap<String, Integer>>(7);
        private HashMap<String, HashMap<String, Integer>> ispFlux = new HashMap<String, HashMap<String, Integer>>(7);
        private HashMap<String, HashMap<String, Integer>> versionFlux = new HashMap<String, HashMap<String, Integer>>(7);
        
	    private HashSet<String> dateSet = new HashSet<String>(7); 
        private HashMap<String, HashSet<String>> areaDateSet = new HashMap<String, HashSet<String>>(7);
        private HashMap<String, HashSet<String>> ispDateSet = new HashMap<String, HashSet<String>>(7);
        private HashMap<String, HashSet<String>> versionDateSet = new HashMap<String, HashSet<String>>(7);
        
        private HashSet<String> areaSet = new HashSet<String>();
        private HashSet<String> ispSet = new HashSet<String>();
        private HashSet<String> versionSet = new HashSet<String>();
        private boolean oldUser = false;
        
        private void parseLoginData(){
            flux.clear();
            areaFlux.clear();
            ispFlux.clear();
            versionFlux.clear();
            areaSet.clear();
            ispSet.clear();
            versionSet.clear();
            if( ! hashMap.containsKey("hs"))
                return;
            for(String detail : hashMap.get("hs")){
                String[] fields = detail.split(",");
                String date = fields[0];
                String ver  = fields[1];
                String area = fields[2];
                String isp  = fields[3];
                long onlineTime = Long.parseLong(fields[4]);
                int downLoadFlux = Integer.parseInt(fields[5]);
                
                if(!flux.containsKey(date))
                    flux.put(date, 0);
                flux.put(date, flux.get(date) + downLoadFlux);
                   
                if(!areaFlux.containsKey(area)){
                    HashMap<String, Integer> tmpFlux = new HashMap<String, Integer>(7);
                    areaFlux.put(area, tmpFlux);
                }
                if(! areaFlux.get(area).containsKey(date))
                    areaFlux.get(area).put(date, 0);
                areaFlux.get(area).put(date, areaFlux.get(area).get(date) + downLoadFlux);
                
                if(!ispFlux.containsKey(isp)){
                    HashMap<String, Integer> tmpFlux = new HashMap<String, Integer>(7);
                    ispFlux.put(isp, tmpFlux);
                }
                if(! ispFlux.get(isp).containsKey(date))
                    ispFlux.get(isp).put(date, 0);
                ispFlux.get(isp).put(date, ispFlux.get(isp).get(date) + downLoadFlux);
                
                if(!versionFlux.containsKey(ver)){
                    HashMap<String, Integer> tmpFlux = new HashMap<String, Integer>(7);
                    versionFlux.put(ver, tmpFlux);
                }
                if(! versionFlux.get(ver).containsKey(date))
                    versionFlux.get(ver).put(date, 0);
                versionFlux.get(ver).put(date, versionFlux.get(ver).get(date) + downLoadFlux);
           
                
                areaSet.add(area);
                ispSet.add(isp);
                versionSet.add(ver);
                
                // online Time
                weekIndex.setTotalOnlineLength(onlineTime);
                if(!areaWeekIndex.containsKey(area)){                    
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    areaWeekIndex.put(area, tmpContainer);
                }
                areaWeekIndex.get(area).setTotalOnlineLength(onlineTime);
  
                if( !ispWeekIndex.containsKey(isp)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    ispWeekIndex.put(isp, tmpContainer);
                }
                ispWeekIndex.get(isp).setTotalOnlineLength(onlineTime);
                
                if(! versionWeekIndex.containsKey(ver)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    versionWeekIndex.put(ver, tmpContainer);
                }
                versionWeekIndex.get(ver).setTotalOnlineLength(onlineTime);
            }
            
            //login User
            weekIndex.setLoginUser(1);
            for(String areaTmp : areaSet )
                areaWeekIndex.get(areaTmp).setLoginUser(1);
            for(String ispTmp : ispSet )
                ispWeekIndex.get(ispTmp).setLoginUser(1);
            for(String verTmp : versionSet )
                versionWeekIndex.get(verTmp).setLoginUser(1);
            
            // new user
            if(! oldUser){
                weekIndex.setNewUser(1);
                for(String areaTmp : areaSet )
                    areaWeekIndex.get(areaTmp).setNewUser(1);
                for(String ispTmp : ispSet )
                    ispWeekIndex.get(ispTmp).setNewUser(1);
                for(String verTmp : versionSet )
                    versionWeekIndex.get(verTmp).setNewUser(1);
            }
            
            // Flux >= 20
            String[] tmpList = flux.keySet().toArray(new String[flux.keySet().size()]); 
            for(String tmpDate: tmpList){
                if(flux.get(tmpDate) <20)
                    flux.remove(tmpDate);
            }
            System.out.println(flux.size());
            
            String[] areaList = areaFlux.keySet().toArray(new String[areaFlux.keySet().size()]);
            for(String areaTmp : areaList){
                String[] tmpList1 = areaFlux.get(areaTmp).keySet().toArray(new String[areaFlux.get(areaTmp).keySet().size()]);
                for(String dateTmp :  tmpList1){
                    if(areaFlux.get(areaTmp).get(dateTmp) < 20)
                      areaFlux.get(areaTmp).remove(dateTmp);
                }
                if(areaFlux.get(areaTmp).isEmpty())
                    areaFlux.remove(areaTmp);
            }
            
            String[] ispList = ispFlux.keySet().toArray(new String[ispFlux.keySet().size()]);
            for(String ispTmp : ispList){
                String[] tmpList1 =  ispFlux.get(ispTmp).keySet().toArray(new String[ispFlux.get(ispTmp).keySet().size()]);
                for(String dateTmp : tmpList1){
                    if(ispFlux.get(ispTmp).get(dateTmp) < 20)
                      ispFlux.get(ispTmp).remove(dateTmp);
                }
                if(ispFlux.get(ispTmp).isEmpty())
                    ispFlux.remove(ispTmp);
            }
            
            String[] versionList = versionFlux.keySet().toArray(new String[versionFlux.keySet().size()]);
            for(String verTmp : versionList){
                String[] tmpList1 = versionFlux.get(verTmp).keySet().toArray(new String[versionFlux.get(verTmp).keySet().size()]);
                for(String dateTmp : tmpList1 ){
                    if(versionFlux.get(verTmp).get(dateTmp) < 20)
                      versionFlux.get(verTmp).remove(dateTmp);
                }
                if(versionFlux.get(verTmp).isEmpty())
                    versionFlux.remove(verTmp);
            }
            
        }
        private void parseBootData(){           
            areaSet.clear();
            ispSet.clear();
            versionSet.clear(); 
            
            dateSet.clear();
            areaDateSet.clear();
            ispDateSet.clear();
            versionDateSet.clear();
            
            HashSet<String> dateStartSet = new HashSet<String>();
            HashSet<String> areaStartSet = new HashSet<String>();
            HashSet<String> ispStartSet = new HashSet<String>();
            HashSet<String> versionStartSet = new HashSet<String>();
            
            if(! hashMap.containsKey("bt"))
                return;
            for(String detail : hashMap.get("bt")){
                String[] fields = detail.split(",");
                String date = fields[0];
                String ver  = fields[1];
                String area = fields[2];
                String isp  = fields[3];
                String startType = fields[4];    
                
                areaSet.add(area);
                ispSet.add(isp);
                versionSet.add(ver);
                
                dateSet.add(date);
                if(! areaDateSet.containsKey(area)){
                    HashSet<String> tmpDateSet = new HashSet<String>(7);
                    areaDateSet.put(area, tmpDateSet);
                }
                areaDateSet.get(area).add(date);
                
                if(! ispDateSet.containsKey(isp)){
                    HashSet<String> tmpDateSet = new HashSet<String>(7);
                    ispDateSet.put(isp, tmpDateSet);
                }
                ispDateSet.get(isp).add(date);
                
                if(! versionDateSet.containsKey(ver)){
                    HashSet<String> tmpDateSet = new HashSet<String>(7);
                    versionDateSet.put(ver, tmpDateSet);
                }
                versionDateSet.get(ver).add(date);
                
                
                // start Type
                dateStartSet.add(startType);
                areaStartSet.add(area + "," + startType);
                ispStartSet.add(isp + "," + startType);
                versionStartSet.add(ver + "," + startType);
            }
            
            // user of different launch type
            for(String type : dateStartSet )
                weekIndex.setStartType(Integer.parseInt(type), 1);
            for(String tmp : areaStartSet){
                String[] dim = tmp.split(",");
                if( ! areaWeekIndex.containsKey(dim[0])){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    areaWeekIndex.put(dim[0], tmpContainer);
                }
                    areaWeekIndex.get(dim[0]).setStartType(Integer.parseInt(dim[1]), 1);
             }
            for(String tmp : ispStartSet){
                String[] dim = tmp.split(",");
                if( ! ispWeekIndex.containsKey(dim[0])){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    ispWeekIndex.put(dim[0], tmpContainer);
                }
                    ispWeekIndex.get(dim[0]).setStartType(Integer.parseInt(dim[1]), 1);
             }
            for(String tmp : versionStartSet){
                String[] dim = tmp.split(",");
                if( ! versionWeekIndex.containsKey(dim[0])){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    versionWeekIndex.put(dim[0], tmpContainer);
                }
                    versionWeekIndex.get(dim[0]).setStartType(Integer.parseInt(dim[1]), 1);
             }
            
            //boot user
            weekIndex.setBootUser(1);
            for(String areaTmp : areaSet )
                areaWeekIndex.get(areaTmp).setBootUser(1);
            for(String ispTmp : ispSet )
                ispWeekIndex.get(ispTmp).setBootUser(1);
            for(String verTmp : versionSet )
                versionWeekIndex.get(verTmp).setBootUser(1);

            // total launch date
            weekIndex.setTotalStartDates(dateSet.size());
            for(String areaTmp : areaDateSet.keySet())
                areaWeekIndex.get(areaTmp).setTotalStartDates(areaDateSet.get(areaTmp).size());
            for(String ispTmp : ispDateSet.keySet())
                ispWeekIndex.get(ispTmp).setTotalStartDates(ispDateSet.get(ispTmp).size());
            for(String verTmp : versionDateSet.keySet())
                versionWeekIndex.get(verTmp).setTotalStartDates(versionDateSet.get(verTmp).size());
        }
        private void parseInstallData(){
            dateSet.clear();
            areaSet.clear();
            ispSet.clear();
            versionSet.clear();
            if( ! hashMap.containsKey("in"))
                return;
            for(String detail : hashMap.get("in")){
                String[] fields = detail.split(",");
                String date = fields[0];
                String ver  = fields[1];
                String area = fields[2];
                String isp  = fields[3];
                
                weekIndex.setInstallNum(1);
                if(flux.containsKey(date))
                    weekIndex.setValidInstallNum(1);
                
                if( ! areaWeekIndex.containsKey(area)){
                  WeekIndexContainer tmpContainer = new WeekIndexContainer();
                  areaWeekIndex.put(area, tmpContainer);
                }
                areaWeekIndex.get(area).setInstallNum(1);
                if(areaFlux.containsKey(area) && areaFlux.get(area).containsKey(date))
                      areaWeekIndex.get(area).setValidInstallNum(1);
              
                if( ! ispWeekIndex.containsKey(isp)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    ispWeekIndex.put(isp, tmpContainer);
                  }
                ispWeekIndex.get(isp).setInstallNum(1);
                if(ispFlux.containsKey(isp) && ispFlux.get(isp).containsKey(date))
                    ispWeekIndex.get(isp).setValidInstallNum(1);

                  
                if( ! versionWeekIndex.containsKey(ver)){
                      WeekIndexContainer tmpContainer = new WeekIndexContainer();
                      versionWeekIndex.put(ver, tmpContainer);
                }
                versionWeekIndex.get(ver).setInstallNum(1);
                if(versionFlux.containsKey(ver) && versionFlux.get(ver).containsKey(date))
                     versionWeekIndex.get(ver).setValidInstallNum(1);
            }
        }
        private void parseUninstallData(){
            dateSet.clear();
            areaSet.clear();
            ispSet.clear();
            versionSet.clear();
            if( ! hashMap.containsKey("un"))
                return;
            for(String detail : hashMap.get("un")){
                String[] fields = detail.split(",");
                String ver  = fields[0];
                String area = fields[1];
                String isp  = fields[2]; 
                areaSet.add(area);
                ispSet.add(isp);
                versionSet.add(ver);
            }
            
            weekIndex.setUninstallUser(1);
            for(String areaTmp : areaSet ){
                if( ! areaWeekIndex.containsKey(areaTmp)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    areaWeekIndex.put(areaTmp, tmpContainer);
                }
                areaWeekIndex.get(areaTmp).setUninstallUser(1);
            }
            
            for(String ispTmp : ispSet ){
                if( ! ispWeekIndex.containsKey(ispTmp)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    ispWeekIndex.put(ispTmp, tmpContainer);
                }
                ispWeekIndex.get(ispTmp).setUninstallUser(1);
            }
            for(String verTmp : versionSet ){
                if( ! versionWeekIndex.containsKey(verTmp)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    versionWeekIndex.put(verTmp, tmpContainer);
                }
                versionWeekIndex.get(verTmp).setUninstallUser(1);
            }  
        }
        private void parseTaskStatData(){
            areaSet.clear();
            ispSet.clear();
            versionSet.clear();           
            if( ! hashMap.containsKey("ts"))
                return;
            for(String detail : hashMap.get("ts")){
                String[] fields = detail.split(",");
                String ver  = fields[0];
                String area = fields[1];
                String isp  = fields[2];
   
                areaSet.add(area);
                ispSet.add(isp);
                versionSet.add(ver);
                 
                weekIndex.setTotalUiOnlineLength(180);
                if(! areaWeekIndex.containsKey(area)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    areaWeekIndex.put(area, tmpContainer);
                }
                areaWeekIndex.get(area).setTotalUiOnlineLength(180);
                
                if(! ispWeekIndex.containsKey(isp)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    ispWeekIndex.put(isp, tmpContainer);
                }
                ispWeekIndex.get(isp).setTotalUiOnlineLength(180);
                
                if(! versionWeekIndex.containsKey(ver)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    versionWeekIndex.put(ver, tmpContainer);
                }
                versionWeekIndex.get(ver).setTotalUiOnlineLength(180);
            }
            
            //ui User
            weekIndex.setUiUser(1);
            for(String areaTmp : areaSet )
                areaWeekIndex.get(areaTmp).setUiUser(1);
            for(String ispTmp : ispSet )
                ispWeekIndex.get(ispTmp).setUiUser(1);
            for(String verTmp : versionSet )
                versionWeekIndex.get(verTmp).setUiUser(1);        
        }
        private void parseWtbhData(){
            areaSet.clear();
            ispSet.clear();
            versionSet.clear();
            if( ! hashMap.containsKey("wt"))
                return;
            for(String detail : hashMap.get("wt")){
                String[] fields = detail.split(",");
                String ver  = fields[0];
                String area = fields[1];
                String isp  = fields[2];
                int playTime = Integer.parseInt(fields[3]);

                areaSet.add(area);
                ispSet.add(isp);
                versionSet.add(ver);
                
                weekIndex.setWatchLength(playTime);
                if(! areaWeekIndex.containsKey(area)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    areaWeekIndex.put(area, tmpContainer);
                }
                areaWeekIndex.get(area).setWatchLength(playTime);
                
                if(! ispWeekIndex.containsKey(isp)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    ispWeekIndex.put(isp, tmpContainer);
                }
                ispWeekIndex.get(isp).setWatchLength(playTime);
                
                if( ! versionWeekIndex.containsKey(ver)){
                    WeekIndexContainer tmpContainer = new WeekIndexContainer();
                    versionWeekIndex.put(ver, tmpContainer);
                }
                versionWeekIndex.get(ver).setWatchLength(playTime);
            }
            
            //watch User
            weekIndex.setWatchUser(1);
            for(String areaTmp : areaSet )
                areaWeekIndex.get(areaTmp).setWatchUser(1);
            for(String ispTmp : ispSet )
                ispWeekIndex.get(ispTmp).setWatchUser(1);
            for(String verTmp : versionSet )
                versionWeekIndex.get(verTmp).setWatchUser(1);           
        }
	    public void reduce(Text Key, Iterable <Text> values, Context context) 
	            throws IOException,InterruptedException{
	        hashMap.clear();
	        oldUser= false;
	        
            for (Text val : values){
                for(String log : val.toString().split("\t")){
                   String[] fields = log.toString().split(":");
                   String logType = fields[0];
                   String data = "";
                   if(fields.length == 2 )
                       data = fields[1];       
                   if(!hashMap.containsKey(logType)){
                       ArrayList<String> tmpList = new ArrayList<String>();
                       hashMap.put(logType, tmpList);
                   }
                   hashMap.get(logType).add(data);
                }
            } 
            
            
            //old user
            if(hashMap.containsKey("o"))
                oldUser = true;
           
            //deal with login data
            parseLoginData();
            
            //deal with boot data
            parseBootData();
            
            //deal with install data
            parseInstallData();
            
            //deal with uninstall data
            parseUninstallData();
            
            //deal with task_stat data
            parseTaskStatData();
            
            //deal with wt_bh data
            parseWtbhData();
               
	    }
	    public void cleanup(Context context)
	            throws IOException, InterruptedException {
	         context.write(new Text("date"), new Text(weekIndex.toString()));
	         for(String area : areaWeekIndex.keySet())
	             context.write(new Text("area," + area),  new Text(areaWeekIndex.get(area).toString()));
	         for(String isp : ispWeekIndex.keySet())
	             context.write(new Text("isp," + isp), new Text(ispWeekIndex.get(isp).toString()));
	         for(String version : versionWeekIndex.keySet())
	             context.write(new Text("version," + version), new Text(versionWeekIndex.get(version).toString()));
	    }
	}

	public static class IdentityMapper extends
	    Mapper<Text, Text, Text, Text>{
	    public  void map (Text key, Text value, Context context)
           throws IOException, InterruptedException{
	        context.write(key, value);
	    }
   }
    
	public static class ClientUserResultReducer extends
        Reducer <Text, Text, NullWritable, Text> {
        private Text newValue   = new Text();
        private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
        private String outputPath = "";
        private String statDate = ""; 
        public void setup(Context context) throws IOException, InterruptedException{
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
            statDate = context.getConfiguration().get("statDate");
        }
        
        public void reduce (Text key,  Iterable <Text> values, Context context)
            throws IOException, InterruptedException{ 
            WeekIndexContainer weekIndex = new WeekIndexContainer(); 
            
            for(Text val : values){
                weekIndex.addAllIndex(val.toString());                
            }
            String[] dims = key.toString().split(",");
            String type = dims[0];
            String dimKey = "";
            if(dims.length == 2){
               outputPath = "F_CLIENT_WEEK_DATE_" + type.toUpperCase();
               dimKey = statDate + "\t" + dims[1]; 
            }else{
               outputPath = "F_CLIENT_WEEK_DATE";
               dimKey = statDate; 
            }

            for(String value : weekIndex.toResult()){
                newValue.set(dimKey + "\t" + value);
                multipleOutputs.write(NullWritable.get(), newValue,  outputPath);
            }
        }
    
        public void cleanup(Context context) throws IOException,InterruptedException {
            multipleOutputs.close();
        }
    }
	
	public int run(String[] args) throws Exception {
	    Configuration conf = getConf();
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        conf = gop.getConfiguration();
        
        conf.set("statDate", conf.get("stat_date"));
        Job job = new Job(conf, "StageWeekClientUser");
        String outputDir = conf.get("output_dir");
        Path tmpOutput = new Path(outputDir + "_tmp");
        FileOutputFormat.setOutputPath(job, tmpOutput);
        tmpOutput.getFileSystem(conf).delete(tmpOutput, true);        
	
		job.setJarByClass(WeekIndexMR.class);		
		for(String path : conf.get("historymac_dir").split(",")){
		    if(path.equals("")) break;
		    MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, HistoryMacMapper.class);
		}
		for(String path : conf.get("login_dir").split(",")){
		    if(path.equals("")) break;
		    MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, LoginMapper.class);
		}
		for(String path : conf.get("boot_dir").split(",")){
	        if(path.equals("")) break;
            MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, BootMapper.class);
		}
		for(String path : conf.get("install_dir").split(",")){
	        if(path.equals("")) break;
		    MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, InstallMapper.class);
		}
		for(String path : conf.get("uninstall_dir").split(",")){
	        if(path.equals("")) break;
		    MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, UninstallMapper.class);
		}
		for(String path : conf.get("taskstat_dir").split(",")){
	        if(path.equals("")) break;
            MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, taskStatMapper.class);
		}
		for(String path : conf.get("wtbh_dir").split(",")){
	         if(path.equals("")) break;
		    MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, WtbhMapper.class);
		}
		
		job.setCombinerClass(stageCombiner.class);
		job.setReducerClass(partResultReducer.class);	
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(conf.getInt("reduce_num", 20));
		int code = job.waitForCompletion(true) ? 0 : 1;

        if(code == 0){                       
            Job combineJob = new Job(conf, "WeekClientUser");
            combineJob.setJarByClass(WeekIndexMR.class);
            
            FileInputFormat.addInputPath(combineJob, new Path(outputDir + "_tmp"));
            Path outputPath =   new Path(outputDir);
            FileOutputFormat.setOutputPath(combineJob, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath, true);
            
            combineJob.setMapperClass(IdentityMapper.class);
            combineJob.setReducerClass(ClientUserResultReducer.class);
            
            combineJob.setInputFormatClass(KeyValueTextInputFormat.class);
            combineJob.setOutputFormatClass(TextOutputFormat.class);
            combineJob.setMapOutputKeyClass(Text.class);
            combineJob.setOutputKeyClass(NullWritable.class);
            combineJob.setOutputValueClass(Text.class);
            
            combineJob.setNumReduceTasks(1);
            code = combineJob.waitForCompletion(true) ? 0 : 1 ;
        }
        
        FileSystem.get(conf).delete(new Path(outputDir + "_tmp"), true);
		return code;
	}

	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner
				.run(new Configuration(), new WeekIndexMR(), args);
		System.out.println(nRet);
	}
}
