package com.bi.common.constrants;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Test;

public class TestUtilComstrantsEnum {

	@Test
	public void ipFormatUtilTest() {

		try {
			Class<?> logUtilEnum = (Class<?>) Class
					.forName("com.bi.common.util.IPFormatUtil");
			Object obj = logUtilEnum.newInstance();
			Method method = logUtilEnum.getMethod("ipFormat",
					new Class[] { String.class });
			System.out.println(method.invoke(obj, "192.168.117.8"));

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void getValue(String... paras) {

		for (int i = 0; i < paras.length; i++) {
			System.out.println(paras[i]);

		}

	}

	@Test
	public void testGetValue() {
		this.getValue( new String[] { "1", "2", "3" });

	}

}
