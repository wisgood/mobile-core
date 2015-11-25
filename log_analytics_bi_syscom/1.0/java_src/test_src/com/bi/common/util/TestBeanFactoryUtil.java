package com.bi.common.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.bi.common.util.BeanFactoryUtil;
import com.bi.logconfig.LogConfInfoEnum;
import com.bi.logconfig.LogMethodParametersInfo;

public class TestBeanFactoryUtil {
	private BeanFactoryUtil beanFactoryUtil;

	@Before
	public void setUp() throws Exception {
		beanFactoryUtil = new BeanFactoryUtil("conf/boot");
	}

	@Test
	public void testGetConfigInfo() {
		LogConfInfoEnum[] logConfInfoEnums = LogConfInfoEnum.values();
		for (int i = 0; i < logConfInfoEnums.length; i++) {
			System.out.println(beanFactoryUtil
					.getConfigFileInfo(logConfInfoEnums[i].getValueStr()));
		}
	}

	@Test
	public void testGetBean() throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		Map<String, LogMethodParametersInfo> outParametersMap = beanFactoryUtil
				.getBeanInfo();
		int size = outParametersMap.size();
		Set<String> keysSet = outParametersMap.keySet();
		Iterator<String> iterator = keysSet.iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();// key
			System.out.println(key);
		}

	}

}
