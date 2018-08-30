package com.wzb.infa.infazipper;

import java.util.ArrayList;

import com.wzb.infa.dbutils.DbUtil;
import com.wzb.infa.dbutils.XMLUtil;
import com.wzb.infa.obj.InfaXML;
import com.wzb.infa.properties.InfaProperty;

public class A1 {
	public static void main(String[] args) {

		try {
			InfaProperty.getInstance().addInfaProperty("D:\\infaXML\\infa.xml");
			XMLUtil xmlUtil = XMLUtil.getInstance();
			ArrayList<InfaXML> xmls = new ArrayList<>();
			xmls.add(xmlUtil.createInfaXML("WZB.CIM_UNIT", "2"));
			xmls.add(xmlUtil.createInfaXML("WZB.CIM_YDLB", "1"));
			xmlUtil.writeInfaXMLToFile(xmls, "D:\\infaXML\\WF_A1_ALL.xml");
			DbUtil.getInstance().release();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
