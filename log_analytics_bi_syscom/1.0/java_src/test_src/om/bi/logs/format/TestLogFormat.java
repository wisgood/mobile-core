package om.bi.logs.format;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.bi.common.util.BeanFactoryUtil;
import com.bi.logs.format.LogNoReduceFormat;

public class TestLogFormat {

	private LogNoReduceFormat logFormat;

	@Before
	public void setUp() throws Exception {
		Map<String, String> configuration = new HashMap<String, String>();
		configuration.put("dateid", "20131017");
		BeanFactoryUtil beanFactoryUtil = new BeanFactoryUtil("conf/boot");
		logFormat = new LogNoReduceFormat();
		logFormat.setBeanFactoryUtil(beanFactoryUtil);
		logFormat.setConfiguration(configuration);
	}

	@Test
	public void testFormatLog() throws Exception {
		String originLog = "1381766402,111.122.28.5,485B39B386B1,2.8.6.51,165674,1,0,0,1,0,1,0";
		System.out.print(logFormat.formatLog(originLog));
	}

}
