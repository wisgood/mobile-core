package com.bi.logconfig;

import java.io.Serializable;
import java.util.List;

public class LogMethodParametersInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 390276468736310466L;

	private String methodName;
	private List<Integer> orgParameters;
	private List<String> outPutParameters;
	
	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	public List<Integer> getOrgParameters() {
		return orgParameters;
	}

	public void setOrgParameters(List<Integer> orgParameters) {
		this.orgParameters = orgParameters;
	}

	public List<String> getOutPutParameters() {
		return outPutParameters;
	}

	public void setOutPutParameters(List<String> outPutParameters) {
		this.outPutParameters = outPutParameters;
	}

}
