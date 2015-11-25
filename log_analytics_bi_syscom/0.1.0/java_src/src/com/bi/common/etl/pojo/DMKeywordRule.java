package com.bi.common.etl.pojo;

public class DMKeywordRule {

	/**
	 * @elements: urlprefix ,keytemplate
	 * 
	 * @deprecated: different urlprefixs have different keytemplates
	 * 
	 * @author: liuyn
	 * 
	 */
	private String urlPrefix;
	private String keyTemplate;
	
	public DMKeywordRule(String urlPrefix, String keyTemplate) {
		super();
		this.urlPrefix = urlPrefix;
		this.keyTemplate = keyTemplate;
	}

	public String getUrlPrefix() {
		return urlPrefix;
	}

	public void setUrlPrefix(String urlPrefix) {
		this.urlPrefix = urlPrefix;
	}

	public String getKeyTemplate() {
		return keyTemplate;
	}

	public void setKeyTemplate(String keyTemplate) {
		this.keyTemplate = keyTemplate;
	}
	
}
