package com.bi.ad.comm.util.format;

public class AreaMappingBase {
	
	public AreaMappingBase (String[] fields){
	    agentAreaId = fields[0];
	    areaId      = fields[1];
	    startDate   = fields[2];
	    endDate     = fields[3];
	}
	
	public String getAgentAreaId() {
        return agentAreaId;
    }
    public void setAgentAreaId(String agentAreaId) {
        this.agentAreaId = agentAreaId;
    }
    public String getAreaId() {
        return areaId;
    }
    public void setAreaId(String areaId) {
        this.areaId = areaId;
    }
    public String getStartDate() {
        return startDate;
    }
    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }
    public String getEndDate() {
        return endDate;
    }
    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }
    
    public boolean isValidAgentId (String date){
        int intDate = Integer.parseInt(date);
        if( intDate >= Integer.parseInt(startDate) && intDate <= Integer.parseInt(endDate))
            return true;
        return false;
    }
    
    private String agentAreaId 	= null;
	private String areaId       = null;
	private String startDate    = null;
	private String endDate      = null;

}
