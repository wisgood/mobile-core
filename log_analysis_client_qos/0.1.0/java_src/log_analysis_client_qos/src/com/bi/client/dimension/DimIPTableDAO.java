package com.bi.client.dimension;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;

import com.bi.client.datadefine.CommonEnum;

public class DimIPTableDAO extends AbstractDimDAO
{
	private DimIPTable ipTable = null;

	@Override
	public void parseDimFile(File file) throws IOException
	{
		BufferedReader in = null;
		try
		{
			this.ipTable = new DimIPTable(new ArrayList<DimIPRecord>());
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line;
			while ((line = in.readLine()) != null)
			{
				if (line.contains("#"))
				{
					continue;
				}
				String[] fields = line.split("\t");
				if (this.isContainEmptyStrs(fields))
				{
					continue;
				}
				try
				{
					long startDecIP = Long.parseLong(fields[0]);
					long finalDecIP = Long.parseLong(fields[1]);
					int provinceID = Integer.parseInt(fields[2]);
					int cityID = Integer.parseInt(fields[3]);
					int ispID = Integer.parseInt(fields[4]);
					DimIPRecord ipRecord = new DimIPRecord(startDecIP, finalDecIP, provinceID,
							cityID, ispID);
					if (null != ipRecord)
					{
						ipTable.addIPRecord(ipRecord);
					}
				}
				catch (Exception e)
				{
					System.out.println("ip_table record format error: " + fields);

					continue;
				}
			}
		}
		finally
		{
			in.close();
		}
	}

	@Override
	public Map<CommonEnum, String> getDimTransMap(Object longKey)
	{
		Map<CommonEnum, String> ipTransMap = new WeakHashMap<CommonEnum, String>();
		long decimalIP = (Long) longKey;
		DimIPRecord ipRecord = this.ipTable.getIPRecord(decimalIP);
		if (null == ipRecord)
		{
			ipTransMap.put(CommonEnum.PROVINCE_ID, "-1");
			ipTransMap.put(CommonEnum.CITY_ID, "-1");
			ipTransMap.put(CommonEnum.ISP_ID, "-1");
		}
		else
		{
			ipTransMap.put(CommonEnum.PROVINCE_ID, String.valueOf(ipRecord.getProvinceID()));
			ipTransMap.put(CommonEnum.CITY_ID, String.valueOf(ipRecord.getCityID()));
			ipTransMap.put(CommonEnum.ISP_ID, String.valueOf(ipRecord.getIspID()));
		}
		return ipTransMap;
	}

}
