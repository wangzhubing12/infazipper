package com.wzb.infa.properties;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.wzb.infa.infazipper.App;

public class InfaProperty extends Properties {

	/**
	 * 单例模式，提供getInstance获取一个实例。
	 * 提供addInfaProperty增加配置项
	 */
	private static volatile InfaProperty infaProperty = null;
	private static final long serialVersionUID = 1L;

	private InfaProperty() {
		super();
	}

	public static InfaProperty getInstance() {
		if (infaProperty == null) {
			infaProperty = new InfaProperty();
			try {
				infaProperty.addInfaProperty(App.class.getResourceAsStream("/infa.xml"));
			} catch (DocumentException e) {
				e.printStackTrace();
			}
			return infaProperty;
		} else {
			return infaProperty;
		}
	}
	
	public InfaProperty addInfaProperty(String path) throws DocumentException {
		try {
			FileInputStream fs=new FileInputStream(path);
			addInfaProperty(fs);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return infaProperty;
	}
	private void addInfaProperty(InputStream path) throws DocumentException {
		// 使用dom4j解析infa.xml,读取默认配置文件
		SAXReader reader = new SAXReader();
		// 获取默认配置
		Element defaultConfiguration = reader.read(path).getRootElement();
		Element property = null;
		String attr = null;
		String value = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> its = defaultConfiguration.elementIterator("property"); its.hasNext();) {
			property = its.next();
			attr = ((Element) property.selectSingleNode("name")).getData().toString().trim();
			value = ((Element) property.selectSingleNode("value")).getData().toString().trim();
			put(attr, value);
		}
	}

}
