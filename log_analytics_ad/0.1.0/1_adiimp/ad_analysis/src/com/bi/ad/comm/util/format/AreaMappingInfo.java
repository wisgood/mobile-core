
package com.bi.ad.comm.util.format;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.hadoop.io.IOUtils;

public class AreaMappingInfo {
    private HashMap<String, ArrayList<AreaMappingBase>> areaMapping = new HashMap<String, ArrayList<AreaMappingBase>>();
		

    public void init(String fileName) throws IOException {
		BufferedReader in = null;		
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line = null;			
			String areaId = null;
			while ((line = in.readLine()) != null) {
				String fields[] = line.trim().split("\t");
				areaId = fields[1];
				AreaMappingBase areaMappingBase = new AreaMappingBase(fields);
				if(!areaMapping.containsKey(areaId)){
				   ArrayList<AreaMappingBase> tmpList = new ArrayList<AreaMappingBase>();
				   areaMapping.put(areaId, tmpList);
				}
				areaMapping.get(areaId).add(areaMappingBase);			
			}			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}		
	}
	
    public HashMap<String, ArrayList<AreaMappingBase>> getAreaMapping() {
        return areaMapping;
    }

    public void setAreaMapping(
            HashMap<String, ArrayList<AreaMappingBase>> areaMapping) {
        this.areaMapping = areaMapping;
    }
    
    public String getAgentAreaId (String areaId, String date){
        String agentAreaId = "-1";
        if( ! areaMapping.containsKey(areaId) && areaId.length() == 3)
                areaId = "999";  //other countries
        if( areaMapping.containsKey(areaId)){
            for(AreaMappingBase am : areaMapping.get(areaId)){
                if (am.isValidAgentId(date))
                    agentAreaId = am.getAgentAreaId();
            }
        }
        return agentAreaId;
    }
}