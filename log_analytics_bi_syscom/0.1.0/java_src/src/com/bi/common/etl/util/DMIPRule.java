package com.bi.common.etl.util;

/**
 * Ip_longstart,ip_longEnd, Province_id, City_id, Isp_id
 * 
 * @author fuys
 * 
 */
public class DMIPRule {
	private long ipLongStart;
	private long ipLongEnd;
	private long provinceId;
	private long cityId;
	private long ispId;

	public DMIPRule(long ipLongStart, long ipLongEnd, long provinceId,
			long cityId, long ispId) {
		super();
		this.ipLongStart = ipLongStart;
		this.ipLongEnd = ipLongEnd;
		this.provinceId = provinceId;
		this.cityId = cityId;
		this.ispId = ispId;
	}

	public long getIpLongStart() {
		return ipLongStart;
	}

	public void setIpLongStart(long ipLongStart) {
		this.ipLongStart = ipLongStart;
	}

	public long getIpLongEnd() {
		return ipLongEnd;
	}

	public void setIpLongEnd(long ipLongEnd) {
		this.ipLongEnd = ipLongEnd;
	}

	public long getProvinceId() {
		return provinceId;
	}

	public void setProvinceId(long provinceId) {
		this.provinceId = provinceId;
	}

	public long getCityId() {
		return cityId;
	}

	public void setCityId(long cityId) {
		this.cityId = cityId;
	}

	public long getIspId() {
		return ispId;
	}

	public void setIspId(long ispId) {
		this.ispId = ispId;
	}

}
