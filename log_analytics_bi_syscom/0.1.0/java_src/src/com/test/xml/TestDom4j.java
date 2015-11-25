package com.test.xml;

import java.io.File;
import java.util.Iterator;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class TestDom4j {
    /**
     * 获取指定xml文档的Document对象,xml文件必须在classpath中可以找到
     * 
     * @param xmlFilePath
     *            xml文件路径
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

    public static void testParseXmlData(String xmlFilePath) {
        // 获取xml解析器对象
        // SAXReader reader = new SAXReader();
        // 将xml解析为Document对象
        Document doc = TestDom4j.parse2Document(xmlFilePath);
        // 获取文档的根元素
        Element root = doc.getRootElement();
        // 定义保存xml数据的缓冲字符串
        StringBuffer sb = new StringBuffer();
        for (Iterator i_action = root.elementIterator(); i_action.hasNext();) {
            Element e_action = (Element) i_action.next();
            for (Iterator a_action = e_action.attributeIterator(); a_action
                    .hasNext();) {
                Attribute attribute = (Attribute) a_action.next();
                sb.append(attribute.getName() + ":" + attribute.getValue());
                sb.append("\n");
            }
        }
        System.out.println(sb);

    }

    public static void main(String[] args) {
        TestDom4j.testParseXmlData("D:/workspace/jar/test.xml");

    }
}
