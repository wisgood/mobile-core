package com.bi.logs.format;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper.Context;

import com.bi.common.constrants.UtilComstrantsEnum;
import com.bi.common.util.BeanFactoryUtil;
import com.bi.common.util.LogsDataFormatUtil;
import com.bi.logconfig.LogConfInfoEnum;
import com.bi.logconfig.LogMethodParametersInfo;

public class LogNoReduceFormat {
	private BeanFactoryUtil beanFactoryUtil;
	private Map<String, String> configuration;

	private Context context;

	public String formatLog(String originLog) throws Exception {
		String[] fields = originLog.split(
				beanFactoryUtil
						.getConfigFileInfo(LogConfInfoEnum.orgSplitSymbol
								.getValueStr()), -1);

		int filterLength = Integer.parseInt(beanFactoryUtil
				.getConfigFileInfo(LogConfInfoEnum.filterLength.getValueStr()));
		if (fields.length <= filterLength) {
			throw new Exception("short");
		}
		String resultvalueInfo = beanFactoryUtil
				.getConfigFileInfo(LogConfInfoEnum.resultValue.getValueStr());
		String[] outParameters = resultvalueInfo.split(UtilComstrantsEnum.comma
				.getValueStr());
		Integer resultFieldsLength = Integer.parseInt(beanFactoryUtil
				.getConfigFileInfo(LogConfInfoEnum.resultFieldsLength
						.getValueStr()));
		List<String> outPutValueList = new ArrayList<String>(resultFieldsLength);
		Map<String, LogMethodParametersInfo> outParametersMap = beanFactoryUtil
				.getBeanInfo();
		Map<String, Object> methodToObjMap = beanFactoryUtil
				.getMethodToObjMap();
		for (int outParameterIndex = 0; outParameterIndex < outParameters.length; outParameterIndex++) {
			boolean isOrgParameter = outParameters[outParameterIndex]
					.contains("&");

			if (isOrgParameter) {
				Integer orgParameterIndex = Integer
						.parseInt(outParameters[outParameterIndex].substring(1)) - 1;
				outPutValueList.add(fields[orgParameterIndex]);
			} else {
				LogMethodParametersInfo logMethodParametersInfo = outParametersMap
						.get(outParameters[outParameterIndex]);
				String methodName = logMethodParametersInfo.getMethodName();
				Object logOperationOjb = methodToObjMap.get(methodName);
				int methodParametersContainsOrgParametersLength = logMethodParametersInfo
						.getOrgParameters().size();
				int methodParametersLength = logMethodParametersInfo
						.getOrgParameters().size()
						+ logMethodParametersInfo.getOutPutParameters().size();
				Class[] parameterTypes = new Class[methodParametersLength];
				List<String> invokeMethodParameters = new ArrayList<String>(
						methodParametersLength);
				for (int i = 0; i < parameterTypes.length; i++) {
					if (i < methodParametersContainsOrgParametersLength) {
						invokeMethodParameters
								.add(fields[logMethodParametersInfo
										.getOrgParameters().get(i) - 1]);
					} else {
						String outPutParameter = logMethodParametersInfo
								.getOutPutParameters()
								.get(i
										- methodParametersContainsOrgParametersLength);
						if (outPutParameter.contains("$")) {
							if (null == configuration) {
								outPutParameter = context.getConfiguration()
										.get(outPutParameter.substring(1));
							} else {
								outPutParameter = configuration
										.get(outPutParameter.substring(1));
							}
						}
						invokeMethodParameters.add(outPutParameter);
					}
					parameterTypes[i] = String.class;
				}
				Method method = logOperationOjb.getClass().getMethod(
						methodName, parameterTypes);
				outPutValueList.add(method.invoke(logOperationOjb,
						invokeMethodParameters.toArray(new String[0]))
						.toString());
			}
		}
		return LogsDataFormatUtil.mergeArrayBySign(outPutValueList, "\t");
	}

	public void setBeanFactoryUtil(BeanFactoryUtil beanFactoryUtil) {
		this.beanFactoryUtil = beanFactoryUtil;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}

	public void setContext(Context context) {
		this.context = context;
	}

}
