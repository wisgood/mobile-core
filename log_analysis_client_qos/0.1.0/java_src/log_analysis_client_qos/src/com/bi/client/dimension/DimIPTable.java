package com.bi.client.dimension;

import java.util.ArrayList;

public class DimIPTable
{
	private ArrayList<DimIPRecord> ipTable = null;

	public DimIPTable(ArrayList<DimIPRecord> ipTable)
	{
		this.ipTable = ipTable;
	}

	public void addIPRecord(DimIPRecord record)
	{
		this.ipTable.add(record);
	}

	public DimIPRecord getIPRecord(long decimalIP)
	{
		if (decimalIP != 0)
		{
			for (int i = 0; i < this.ipTable.size(); i++)
			{
				DimIPRecord ipRecord = (DimIPRecord) this.ipTable.get(i);
				if (ipRecord.getStartDecIP() <= decimalIP && decimalIP <= ipRecord.getFinalDecIP())
				{
					return ipRecord;
				}
			}
		}
		return null;
	}

}
