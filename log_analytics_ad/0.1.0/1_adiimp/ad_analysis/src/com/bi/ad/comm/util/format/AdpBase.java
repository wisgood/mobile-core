package com.bi.ad.comm.util.format;

public class AdpBase {
	
	public AdpBase (String[] fields){
		setId(fields[0]);
		setCode(fields[1]);
		setTypeId(fields[2]);
		setOptFlag(fields[3]);		
	}
	

    public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public String getOptFlag() {
		return optFlag;
	}
	public void setOptFlag(String optFlag) {
	    if(optFlag.equals("1") || optFlag.equals("2"))
	        this.optFlag = "1";
	    else
	        this.optFlag = "0";
	}
	public String getTypeId() {
    	return typeId;
	}   
	public void setTypeId(String typeId) {
    	this.typeId = typeId;
    }

	private String id 	   = null;
	private String code    = null;
	private String optFlag = null;
	private String typeId  = null;

}
