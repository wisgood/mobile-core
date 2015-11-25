package com.test.ioc;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class BeanFactoryByReflect {
    /**
     * beanMap,用来存放所有根据xml配置文件实例化的对象
     */
    private Map<String, Object> beanMap = new HashMap<String, Object>();

    /**
     * 初始化。在class路径下查找指定的xml文件，并初始化bean工厂
     * 
     * @param xmlUri
     *            ，xml文件在classpath的相对路径。例如：/com/jisong/ioc/zjsioc.xml
     */
    public void init(String xmlUri) throws Exception {
        try {

            // 1.根据根据xml的相对路径，读取xml文件，并获得xml文件的根元素
            // 1.1根据xml的相对路径，获得一个输入流
            SAXReader reader = new SAXReader();// 创建一个解析器对象
            // ClassLoader classLoader = Thread.currentThread()
            // .getContextClassLoader();//获得当前线程的类加载器
            // InputStream in =
            // classLoader.getResourceAsStream(xml);//通过类加载器，获得指定相对路径下文件的输入流

            InputStream in = this.getClass().getResourceAsStream(xmlUri);// 通过当前Class对象，获得指定相对路径下文件的输入流

            // 1.2.从输入流获得document对象
            Document doc = reader.read(in);// 通过解析器对象，读取输入流并转换为一个Document对象

            // 1.3.从document对象，获得xml文件的根元素
            Element root = doc.getRootElement();// 获得根元素
            Element elBean = null;// 定义bean元素变量

            // 2.遍历bean元素，实例化所有bean并初始化其属性值，然后保存在beanMap中
            for (Iterator iteBean = root.elementIterator("bean"); iteBean.hasNext();) {
                elBean = (Element) iteBean.next();// 获得bean元素

                // 2.1获得bean的属性id和class
                Attribute atrId = elBean.attribute("id");
                Attribute atrClass = elBean.attribute("class");

                // 2.2通过Java反射机制，通过class的名称获取Class对象
                Class clsBean = Class.forName(atrClass.getText());

                // 2.3获得指定class的所有属性描述，以初始化其所有属性

                // 获取其属性描述数组
                Field pds[] = clsBean.getDeclaredFields();
                
                // 将属性描述数组转换为HashMap,这样下一步在设置属性的值的时候，速度更快
                Map<String, Field> mapProp = new HashMap<String, Field>();
                for (Field pd : pds) {
                    mapProp.put(pd.getName(), pd);
                }

                // 2.4创建指定class的实例obj
                Object obj = clsBean.newInstance();
                Method mSet = null;// 定义set方法变量

                // 2.5遍历该bean的property属性，并通过Java反射调用其set方法，设置obj的所有属性的值
                for (Iterator iteProp = elBean.elementIterator("property"); iteProp.hasNext();) {
                    // 2.5.1 获得属性元素
                    Element elProp = (Element) iteProp.next();

                    // 2.5.2获取该property的name属性
                    Attribute atrName = elProp.attribute("name");
                    Attribute atrType = elProp.attribute("type");

                    Class clsType = Class.forName(atrType.getText());

                    // 2.5.3获取该property的子元素value的值
                    String strValue = null;
                    // 正常情况下，name元素只能有一个value属性
                    for (Iterator iteValue = elProp.elementIterator("value"); iteValue.hasNext();) {
                        Element elValue = (Element) iteValue.next();
                        strValue = elValue.getText();
                        break;
                    }

                    // 2.5.4调用对象属性名的 set方法，给指定的属性赋值
                    Field tmpPd = mapProp.get(atrName.getValue());
                    if (tmpPd != null) {
                        String fieldName = tmpPd.getName();
                        String methodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
                        mSet = clsBean.getMethod(methodName, clsType);// 取得指定的set方法
                        mSet.invoke(obj, strValue);// 通过Java反射，调用指定对象的方法并传参（即调用指定的set方法）
                    }
                }
                // 将对象放入beanMap中，其中key为id值，value为obj对象
                beanMap.put(atrId.getText(), obj);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 通过id获取bean的对象.
     * 
     * @param beanId
     *            xml文件中bean元素的id属性值
     * @return 返回对应对象
     */
    public Object getBean(String beanId) {
        Object obj = beanMap.get(beanId);
        return obj;
    }

    /**
     * 测试方法.
     */
    public static void main(String[] args) {
        try {
            BeanFactoryByReflect factory = new BeanFactoryByReflect();
            factory.init("ioc.xml");
            Student stud = (Student) factory.getBean("student");
            System.out.println(stud);
            stud = (Student) factory.getBean("student2");
            System.out.println(stud);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
