package com.bi.common.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.bi.common.constrants.UtilComstrantsEnum;
import com.bi.logconfig.LogConfInfoEnum;
import com.bi.logconfig.LogMethodParametersInfo;

public class BeanFactoryUtil {

	private Properties losInfoProps;
	private Map<String, Object> methodToObjMap;
	private List<String> filterMethodList;
	private String lastLineStr;
	private Map<String, LogMethodParametersInfo> outParametersMap;

	private void init(String filePath) throws IOException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		BufferedReader in = null;
		int logInfoEnumValueIndex = 0;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(
					filePath)));
			String line;
			while ((line = in.readLine()) != null) {
				if (line.contains("#loginfo")) {
					lastLineStr = "#loginfo";
				}
				if (line.contains("#classinfo")) {
					lastLineStr = "#classinfo";
				}
				if (line.contains("#methodinfo")) {
					lastLineStr = "#methodinfo";
				}
				if (line.contains("#")) {
					continue;
				}
				if (null != lastLineStr && "#loginfo".contains(lastLineStr)) {
					String logInfoValue = new String(line.substring(
							line.indexOf(UtilComstrantsEnum.equalSign
									.getValueStr()) + 1).trim());
					LogConfInfoEnum[] logConfInfoEnums = LogConfInfoEnum
							.values();
					if (line.toLowerCase().contains(
							logConfInfoEnums[logInfoEnumValueIndex]
									.getValueStr())) {
						losInfoProps.put(
								logConfInfoEnums[logInfoEnumValueIndex]
										.getValueStr(), logInfoValue);

					}
					logInfoEnumValueIndex++;

				}
				if (null != lastLineStr && "#classinfo".contains(lastLineStr)) {
					String className = line;
					Class<?> logUtil = (Class<?>) Class.forName(className);
					Method[] methods = logUtil.getMethods();
					Object logUtilObj = logUtil.newInstance();
					for (int i = 0; i < methods.length; i++) {
						String methodName = methods[i].getName();
						if (!filterMethodList.contains(methodName)) {
							methodToObjMap
									.put(methods[i].getName(), logUtilObj);
						}
					}
				}
				if (null != lastLineStr && "#methodinfo".contains(lastLineStr)) {
					int equalSignIndex = line
							.indexOf(UtilComstrantsEnum.equalSign.getValueStr());
					String outputParametersInfo = new String(line.substring(0,
							equalSignIndex));
					String methodInfo = new String(
							line.substring(equalSignIndex + 1));
					int leftParenthesisIndex = methodInfo
							.indexOf(UtilComstrantsEnum.leftParenthesis
									.getValueStr());
					String methodName = new String(methodInfo.substring(0,
							leftParenthesisIndex));
					int rightParenthesisIndex = methodInfo
							.indexOf(UtilComstrantsEnum.rightParenthesis
									.getValueStr());
					String methodParametesInfo = new String(
							methodInfo.substring(leftParenthesisIndex + 1,
									rightParenthesisIndex));
					String[] methodParametes = methodParametesInfo.split(",",
							-1);
					List<Integer> orgParameterList = new ArrayList<Integer>(
							methodParametes.length);
					List<String> userdefinedParameterList = new ArrayList<String>(
							methodParametes.length);
					for (int i = 0; i < methodParametes.length; i++) {
						boolean isOrgParameters = methodParametes[i]
								.contains("&");
						boolean isUserdefinedParameters = methodParametes[i]
								.contains("$");
						if (isOrgParameters) {
							orgParameterList.add(new Integer(methodParametes[i]
									.substring(1)));
						} else if (isUserdefinedParameters) {
							userdefinedParameterList.add(methodParametes[i]);
						}

					}
					LogMethodParametersInfo logMethodParametersInfo = new LogMethodParametersInfo();
					logMethodParametersInfo.setMethodName(methodName);
					logMethodParametersInfo.setOrgParameters(orgParameterList);
					logMethodParametersInfo
							.setOutPutParameters(userdefinedParameterList);
					outParametersMap.put(outputParametersInfo,
							logMethodParametersInfo);
				}

			}
		} finally {
			in.close();
		}
	}

	public BeanFactoryUtil(String filePath) throws IOException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		String[] basicMethods = { "wait", "equals", "toString", "hashCode",
				"getClass", "notify", "notifyAll" };
		filterMethodList = Arrays.asList(basicMethods);
		losInfoProps = new Properties();
		methodToObjMap = new HashMap<String, Object>(30);
		outParametersMap = new HashMap<String, LogMethodParametersInfo>(30);
		this.init(filePath);
	}

	public String getConfigFileInfo(String name) {
		return losInfoProps.getProperty(name);
	}

	public Map<String, LogMethodParametersInfo> getBeanInfo() {
		return outParametersMap;

	}

	public Map<String, Object> getMethodToObjMap() {
		return methodToObjMap;
	}

}
