package com.bi.ibidian.dimension;

public class DimIPRecord
{
	private long startDecIP;
	private long finalDecIP;
	private int provinceID;
	private int cityID;
	private int ispID;

	public DimIPRecord(long startDecIP, long finalDecIP, int provinceID, int cityID, int ispID)
	{
		this.startDecIP = startDecIP;
		this.finalDecIP = finalDecIP;
		this.provinceID = provinceID;
		this.cityID = cityID;
		this.ispID = ispID;
	}

	public long getStartDecIP()
	{
		return startDecIP;
	}

	public void setStartDecIP(long startDecIP)
	{
		this.startDecIP = startDecIP;
	}

	public long getFinalDecIP()
	{
		return finalDecIP;
	}

	public void setFinalDecIP(long finalDecIP)
	{
		this.finalDecIP = finalDecIP;
	}

	public long getProvinceID()
	{
		return provinceID;
	}

	public void setProvinceID(int provinceID)
	{
		this.provinceID = provinceID;
	}

	public long getCityID()
	{
		return cityID;
	}

	public void setCityID(int cityID)
	{
		this.cityID = cityID;
	}

	public long getIspID()
	{
		return ispID;
	}

	public void setIspID(int ispID)
	{
		this.ispID = ispID;
	}

}
