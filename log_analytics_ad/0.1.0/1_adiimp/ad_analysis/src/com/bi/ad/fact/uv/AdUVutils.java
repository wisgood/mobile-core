package com.bi.ad.fact.uv;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.IOUtils;

import com.bi.ad.comm.util.format.AreaMappingInfo;

public class AdUVutils {
    public static ArrayList<String> init(String fileName, int n) throws IOException {
        ArrayList<String> arrayList = new ArrayList<String>();
        BufferedReader in = null;       
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
            String line  = null;
            while ((line = in.readLine()) != null) {
                String[] tmp = line.trim().split("\t");
                arrayList.add(tmp[n]);
            }       
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
        return arrayList;
    }
    
    public static HashMap<String, String> init(String fileName, int m, int n) throws IOException {
        HashMap<String, String>  hashMap = new HashMap<String, String>();
        BufferedReader in = null;       
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
            String line  = null;
            while ((line = in.readLine()) != null) {
                String[] tmp = line.trim().split("\t");
                hashMap.put(tmp[m], tmp[n]);
            }       
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
        return hashMap;
    }
    
    
    public static HashMap<String, ArrayList<String>> init(String fileName) throws IOException {
        HashMap<String, ArrayList<String>>  hashMap = new HashMap<String, ArrayList<String>>();
        BufferedReader in = null;       
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
            String line  = null;
            while ((line = in.readLine()) != null) {
                String[] tmp = line.trim().split("\t");
                String adpGroupId = tmp[1];
                for(String adpId : tmp[0].split(",")){
                    if( ! hashMap.containsKey(adpId)){
                        ArrayList<String> adpList = new ArrayList<String>();
                        hashMap.put(adpId,  adpList);
                    }
                    hashMap.get(adpId).add(adpGroupId);
                }
            }       
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
        return hashMap;
    }
    public static boolean legalArea(ArrayList<String> areaList, String area ){
        if (areaList.contains(area) || areaList.contains("0"))
            return true;
        
        if (area.length() == 2 && ! area.equals("99") && areaList.contains("1"))
            return true;
        
        if (area.length() == 3 && areaList.contains("2"))
            return true;
        
        if (area.length() == 6 && (areaList.contains("1") || areaList.contains(area.substring(0, 2))))
            return true;
        return false;
    }
    
    public static ArrayList<String> getAreaList(HashMap<String, String> areaMap, String area){
        ArrayList<String> desArea = new ArrayList<String> ();
        desArea.add("0");
        if(areaMap.containsKey(area))
            desArea.add(area);
        if(area.length() == 2 && ! area.equals("99") && areaMap.containsKey("1"))
            desArea.add("1");               
        if(area.length() == 3 && areaMap.containsKey("2"))
            desArea.add("2");
        if (area.length() == 6 && areaMap.containsKey(area.substring(0, 2)))
            desArea.add(area.substring(0, 2));
        if (area.length() == 6 && areaMap.containsKey("1"))
            desArea.add("1");
        return desArea;
    }
    
    
    public static ArrayList<String> getAreaList(AreaMappingInfo areaMapping, String area, String date){
        ArrayList<String> areaList = new ArrayList<String>();
        areaList.add("t1");
        if(area.length() == 2 ){
            areaList.add("p" + area);
            if( ! area.equals("99"))
                areaList.add("o1");
        }
        if(area.length() == 3){
            areaList.add("o2");
           if( ! areaMapping.getAreaMapping().containsKey(area))
               area = "999";
           areaList.add("p" + area);
        }
        if(area.length() == 6){
            areaList.add("c" + areaMapping.getAgentAreaId(area, date));
            areaList.add("p" + area.substring(0, 2));
            areaList.add("o1");
        }
        return areaList;
    }
    
    public static ArrayList<String> getAdpList( HashMap<String, ArrayList<String>> adpGroup, String adpId){
        ArrayList<String> adpList = new ArrayList<String>();
        adpList.add(adpId);
           
        if(adpGroup.containsKey(adpId)){           
            adpList.addAll(adpGroup.get(adpId));
        }
        
        return adpList;
    }
    
    public static String getTableMiddleName(String area){
        String middleName = "";
        char type = area.charAt(0);
        if( type == 'c'){
            middleName = "CITY";           
        }
        else if(type == 'p'){
            middleName = "PROVINCE";
        }
        else if(type == 'o'){
            middleName = "COUNTRY";            
        }
        else if(type == 't'){
            middleName = "TOTAL";            
        }       
        return middleName;
    }
}
