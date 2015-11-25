package com.bi.common.dm.pojo;

/**
 * IP, Ip_long, Province_id, City_id, Isp_id
 * Ip_longΪ�޷����ʽ��City_id��Isp_idΪ���IP����ӵ�ʡ���������Ӫ��
 * 
 * @author fuys
 * 
 */
public class IPInfo {
	private String ipInfoStr;
	private String ipLongStr;
	private String provinceId;
	private String cityId;
	private String ispId;

	public IPInfo(String ipInfoStr, String ipLongStr, String provinceId,
			String cityId, String ispId) {
		super();
		this.ipInfoStr = ipInfoStr;
		this.ipLongStr = ipLongStr;
		this.provinceId = provinceId;
		this.cityId = cityId;
		this.ispId = ispId;
	}

	public String getIpInfoStr() {
		return ipInfoStr;
	}

	public void setIpInfoStr(String ipInfoStr) {
		this.ipInfoStr = ipInfoStr;
	}

	public String getIpLongStr() {
		return ipLongStr;
	}

	public void setIpLongStr(String ipLongStr) {
		this.ipLongStr = ipLongStr;
	}

	public String getProvinceId() {
		return provinceId;
	}

	public void setProvinceId(String provinceId) {
		this.provinceId = provinceId;
	}

	public String getCityId() {
		return cityId;
	}

	public void setCityId(String cityId) {
		this.cityId = cityId;
	}

	public String getIspId() {
		return ispId;
	}

	public void setIspId(String ispId) {
		this.ispId = ispId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cityId == null) ? 0 : cityId.hashCode());
		result = prime * result
				+ ((ipInfoStr == null) ? 0 : ipInfoStr.hashCode());
		result = prime * result
				+ ((ipLongStr == null) ? 0 : ipLongStr.hashCode());
		result = prime * result + ((ispId == null) ? 0 : ispId.hashCode());
		result = prime * result
				+ ((provinceId == null) ? 0 : provinceId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IPInfo other = (IPInfo) obj;
		if (cityId == null) {
			if (other.cityId != null)
				return false;
		} else if (!cityId.equals(other.cityId))
			return false;
		if (ipInfoStr == null) {
			if (other.ipInfoStr != null)
				return false;
		} else if (!ipInfoStr.equals(other.ipInfoStr))
			return false;
		if (ipLongStr == null) {
			if (other.ipLongStr != null)
				return false;
		} else if (!ipLongStr.equals(other.ipLongStr))
			return false;
		if (ispId == null) {
			if (other.ispId != null)
				return false;
		} else if (!ispId.equals(other.ispId))
			return false;
		if (provinceId == null) {
			if (other.provinceId != null)
				return false;
		} else if (!provinceId.equals(other.provinceId))
			return false;
		return true;
	}

}
