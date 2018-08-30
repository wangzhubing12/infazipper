package com.wzb.infa.infazipper;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.wzb.infa.dbutils.DbUtil;
import com.wzb.infa.dbutils.XMLUtil;
import com.wzb.infa.obj.InfaXML;
import com.wzb.infa.properties.InfaProperty;

public class A1 {
	private static final Logger log = Logger.getLogger(A1.class);

	public static void main(String[] args) {

		try {
			if (args.length > 0) {
				InfaProperty.getInstance().addInfaProperty(args[0]);
			} else {
				log.error("must give a propertie file");
				System.exit(-1);
			}
			XMLUtil xmlUtil = XMLUtil.getInstance();
			ArrayList<InfaXML> xmls = new ArrayList<>();
			xmls.add(xmlUtil.createInfaXML("WZB.CIM_UNIT", "3"));
			xmls.add(xmlUtil.createInfaXML("WZB.CIM_YDLB", "2"));
			xmlUtil.writeInfaXMLToFile(xmls, "D:\\infaXML\\WF_A1_ALL.xml");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbUtil.getInstance().release();
		}
	}
}
