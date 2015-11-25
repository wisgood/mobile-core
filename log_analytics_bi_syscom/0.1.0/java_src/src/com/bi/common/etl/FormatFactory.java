package com.bi.common.etl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.bi.common.etl.transform.Transform;

public class FormatFactory {
    /**
     * beanMap,用来存放所有根据xml配置文件实例化的对象
     */
    
    private List<TransformData> beanTransform = new ArrayList<TransformData>();
    private String[] fields;
    private String inputFieldSperator = "\t";
    private String outputFieldSperator = "\t";
    private int fieldsLength = 0;
    private int infohashIndex = 0;

    /**
     * 初始化。在class路径下查找指定的xml文件，并初始化bean工厂
     * 
     * @param xmlUri,
     *           xml文件在classpath的相对路径。例如：/com/jisong/ioc/zjsioc.xml
     */
    public void init(String xmlUri) throws Exception {

        // 1.根据根据xml的相对路径，读取xml文件，并获得xml文件的根元素
        // 1.1根据xml的相对路径，获得一个输入流
        SAXReader reader = new SAXReader();// 创建一个解析器对象
//        InputStream in = this.getClass().getResourceAsStream(xmlUri);// 通过当前Class对象，获得指定相对路径下文件的输入流

        InputStream in = new FileInputStream(xmlUri);
        // 1.2.从输入流获得document对象
        Document doc = reader.read(in);// 通过解析器对象，读取输入流并转换为一个Document对象

        // 1.3.从document对象，获得xml文件的根元素
        Element root = doc.getRootElement();// 获得根元素
        this.parseFields(root);
        this.parseBeans(root);
        in.close();
    }
    
    private void parseBeans(Element root) throws Exception {
        Element elementBean = null;// 定义bean元素变量
        try {
            for (Iterator iteBean = root.elementIterator("bean"); iteBean.hasNext();) {

                elementBean = (Element) iteBean.next();// 获得bean元素

                // 2.1获得bean的属性id和class
                Attribute atrId = elementBean.attribute("id");
                Attribute atrClass = elementBean.attribute("class");
                Attribute atrFlow = elementBean.attribute("flow");

                // 2.2通过Java反射机制，通过class的名称获取Class对象
                Class clsBean = Class.forName(atrClass.getText());
                String name = atrId.getText();
                Boolean isFlow = false;
                if (atrFlow != null
                        && "true".equalsIgnoreCase(atrFlow.getText())) {
                    isFlow = true;
                }

                // 2.4创建指定class的实例obj
                Transform obj = (Transform) clsBean.newInstance();
                this.setProperty(clsBean, obj, elementBean);
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].equalsIgnoreCase(name)) {
                        beanTransform.add(new TransformData(i, obj, isFlow));
                        break;
                    }
                }
            }

        }
        catch(Exception e) {
            throw e;
        }
    }
    
    private void setProperty(Class clsBean, Object obj, Element elementBean) throws Exception{
        // 获取其属性描述数组
        Field pds[] = clsBean.getDeclaredFields();
        
        // 将属性描述数组转换为HashMap,这样下一步在设置属性的值的时候，速度更快
        Map<String, Field> mapProp = new HashMap<String, Field>();
        for (Field pd : pds) {
            mapProp.put(pd.getName(), pd);
        }

        // 2.4创建指定class的实例obj
        Method mSet = null;// 定义set方法变量

        // 2.5遍历该bean的property属性，并通过Java反射调用其set方法，设置obj的所有属性的值
        for (Iterator iteProp = elementBean.elementIterator("property"); iteProp.hasNext();) {
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
    }
    
    private void parseFields(Element root) {
        // 1.读取field域
        Iterator iteTest = root.elementIterator("field");
        Element elementBean = null;// 定义bean元素变量
        if (!iteTest.hasNext()) {
            return;
        }

        elementBean = (Element) iteTest.next();
        String strFields = null;
        String strInfohash = null;
        for (Iterator iteProp = elementBean.elementIterator("property"); iteProp.hasNext();) {
            // 2.1 获得属性元素
            Element elProp = (Element) iteProp.next();

            // 2.2获取该property的name属性
            Iterator iteValue = elProp.elementIterator("value");
            if (!iteValue.hasNext()) {
                return;
            }
                
            Element elValue = (Element) iteValue.next();
            String strName = elProp.attribute("name").getText();
            String strValue = elValue.getText();
            if ("FS".equalsIgnoreCase(strName)) {
                inputFieldSperator = strValue;
            }
            else if ("OFS".equalsIgnoreCase(strName)) {
                outputFieldSperator = strValue;
            }
            else if ("fields".equalsIgnoreCase(strName)) {
                strFields = strValue;
            }else if("IH".equalsIgnoreCase(strName)){
                strInfohash = strValue;
            }
        }
        
        if (strFields != null) {
            fields = strFields.split(inputFieldSperator);
            fieldsLength = fields.length;
        }
        
        for(int i=0; i<fields.length;i++){
            if(strInfohash.equalsIgnoreCase(fields[i])){
                infohashIndex = i;
                break;
            }
        }
    }
    
    public String getOutputFieldSperator(){
        return outputFieldSperator;
    }
    
    public int getFieldsLength(){
        return fieldsLength;
    }
    
    public int getInfohashIndex(){
        return infohashIndex;
    }
    
    /**
     * 
     * @param input
     * @return
     */
    public String formatFlow(String input) {
        String output = input;
        String[] fields = input.split(inputFieldSperator);
        Iterator it = beanTransform.iterator();
        
        while (it.hasNext()) {
            TransformData transformData = (TransformData) it.next();

              String fieldFormat = transformData.process(fields, outputFieldSperator);
                output += outputFieldSperator + fieldFormat;
        }
        System.out.println(output);
        return output;
    }
    
    public String getInfohash(String input){
        String infohash = null;
        String[] fields = input.split(inputFieldSperator);
        if(fields.length >= infohashIndex){
            infohash = fields[infohashIndex];
        }
        return infohash;
    }
    
    class TransformData{
        private Transform transform = null;
        private int index = 0;
        private boolean isFlow = false;
        
        TransformData(int index, Transform transform, boolean isFlow){
            this.transform = transform;
            this.index = index;
            this.isFlow = isFlow;
        }
        
        boolean isFlow(){
            return isFlow;
        }
        
        int getIndex(){
            return index;
        }
        
        Transform getTransform(){
            return transform;
        }
        
        String process(String[] fields, String outputSperator){
            if(index >= fields.length){
                return null;
            }
            String result = transform.process(fields[index], outputSperator);
            if(isFlow){
               fields[index] = result; 
            }
            return result;
        }
    }

    /**
     * 测试方法.
     */
    public static void main(String[] args) {
        try {
            FormatFactory factory = new FormatFactory();
            factory.init("test_bi.xml");
            
            FileReader reader = new FileReader("/home/niewf/fbuffer.csv");
            BufferedReader br = new BufferedReader(reader);
            String line = null;

            while ((line = br.readLine()) != null) {
                System.out.println(factory.formatFlow(line));
            }

            br.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
