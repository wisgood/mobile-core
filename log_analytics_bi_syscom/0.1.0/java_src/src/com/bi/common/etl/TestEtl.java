package com.bi.common.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.bi.common.etl.transform.Transform;
import com.test.xml.TestDom4j;

public class TestEtl {

    /**
     * 获取指定xml文档的Document对象,xml文件必须在classpath中可以找到
     * 
     * @param xmlFilePath,xml文件路径
     * @return Document对象
     */
    public static Document parse2Document(String xmlFilePath) {
        SAXReader reader = new SAXReader();
        Document doc = null;
        try {
            doc = reader.read(new File(xmlFilePath));
        }
        catch(DocumentException e) {
            e.printStackTrace();
        }
        return doc;
    }

    public static Map parseXmlData(String xmlFilePath) {
        // 获取xml解析器对象
        // SAXReader reader = new SAXReader();
        // 将xml解析为Document对象
        Document doc = TestDom4j.parse2Document(xmlFilePath);
        Map<Integer, String> map = new HashMap<Integer, String>();
        // 获取文档的根元素
        Element root = doc.getRootElement();
        String[] inputFields = null;

        for (Iterator iter = root.elementIterator(); iter.hasNext();) {
            Element actions = (Element) iter.next();
            String type = actions.elementTextTrim("type");
            String name = actions.elementTextTrim("name");
            String value = actions.elementTextTrim("value");

            if (type.equalsIgnoreCase("fields")
                    && name.equalsIgnoreCase("input")) {
                inputFields = value.split(",");
                break;
            }
        }
        if (inputFields == null) {
            return map;
        }

        for (Iterator iter = root.elementIterator(); iter.hasNext();) {
            Element actions = (Element) iter.next();
            String type = actions.elementTextTrim("type");
            String name = actions.elementTextTrim("name");
            String value = actions.elementTextTrim("value");

            if (type.equalsIgnoreCase("transform")) {
                for (int i = 0; i < inputFields.length; i++) {
                    if (name.equalsIgnoreCase(inputFields[i])) {
                        map.put(i, value);
                    }
                }
            }
        }
        return map;
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        Map map = TestEtl.parseXmlData("D:/workspace/jar/test_bi.xml");
        Map transformMap = new HashMap<Integer, Transform>();
        Set set = map.keySet();
        Iterator it = set.iterator();
        while (it.hasNext()) {
            Integer key = (Integer) it.next();
            String value = (String) map.get(key);
            Class c = Class.forName(value);
            Transform tr = (Transform) c.newInstance();
            transformMap.put(key, tr);
        }

        FileReader reader = new FileReader("D:/workspace/jar/input/fbuffer.csv");
        BufferedReader br = new BufferedReader(reader);
        String line = null;
        String result[];

        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");

            it = set.iterator();
            while (it.hasNext()) {
                Integer key = (Integer) it.next();
                Transform trans = (Transform) transformMap.get(key);
                if (key.intValue() < fields.length) {
                    String formatedField = trans.process(fields[key.intValue()], ";");
                    System.out.print(formatedField + ",");
                }
            }
            System.out.println(line);
        }

        br.close();
        reader.close();
    }
}
