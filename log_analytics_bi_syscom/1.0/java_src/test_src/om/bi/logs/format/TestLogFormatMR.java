package om.bi.logs.format;


import org.junit.Before;
import org.junit.Test;

import com.bi.common.util.BeanFactoryUtil;

public class TestLogFormatMR {

	private BeanFactoryUtil beanFactoryUtil;

	@Before
	public void setUp() throws Exception {
		beanFactoryUtil = new BeanFactoryUtil("conf/boot");
	}

	@Test
	public void testLogFormat() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String orginStr = "1381680002,124.164.24.89,1078D2CE2280,2.8.5.30,108161,5.1,1,0,1,0,1";
		
		
		
		
	}

}
