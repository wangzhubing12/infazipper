package com.wzb.infa.infazipper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.dom4j.DocumentException;

import com.wzb.infa.dbutils.DbUtil;
import com.wzb.infa.dbutils.XMLUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.infazipper.App.StringPadder;
import com.wzb.infa.obj.InfaXML;
import com.wzb.infa.properties.InfaProperty;

public class A1 {
	private static final Logger log = Logger.getLogger(A1.class);

	public static void main(String[] args) {
		InfaProperty infaProperty = null;
		A1 a1 = new A1();
		HashSet<String> tableLIst = a1.getTableLIst();
		int size = tableLIst.size();
		int errorSize = 0;
		int tableCount = 0;// 已经生成XML的表数量

		if (args.length > 0) {
			try {
				infaProperty = InfaProperty.getInstance().addInfaProperty(args[0]);
			} catch (DocumentException e) {
				log.error(e.getMessage());
			}
		} else {
			log.error("must give a propertie file");
			System.exit(-1);
		}
		String mappingType = infaProperty.getProperty("mapping.type", "-1");
		XMLUtil xmlUtil = XMLUtil.getInstance();
		ArrayList<InfaXML> xmls = new ArrayList<>();
		for (String table : tableLIst) {
			log.info("Makeing xml for " + StringPadder.rightPad(table, "-", 30) + "(" + (tableCount + errorSize + 1)
					+ "/" + size + ")");

			try {
				xmls.add(xmlUtil.createInfaXML(table, mappingType));
			} catch (UnsupportedDatatypeException | SQLException | CheckTableExistException e) {
				log.error(e.getMessage());
				errorSize++;
			}
			log.info("success!");

			tableCount++;

			if ((tableCount % Integer.parseInt(infaProperty.getProperty("xml.output.mappings", "20"))) == 0) {
				int fileCount = Math.floorDiv(tableCount,
						Integer.parseInt(infaProperty.getProperty("xml.output.mappings", "20")));
				String fileName = infaProperty.getProperty("work.dir") + infaProperty
						.getProperty("xml.output", "gen.xml").toLowerCase().replace(".xml", fileCount + ".xml");
				try {
					xmlUtil.writeInfaXMLToFile(xmls, fileName);
				} catch (IOException e) {
					log.error(e.getMessage());
					System.exit(-1);
				}
			}
		}

		DbUtil.getInstance().release();
	}

	private HashSet<String> getTableLIst() {
		HashSet<String> tableList = new HashSet<>();// 保存要生成的表清单
		BufferedReader tableListReader = null;// 读取要生成的表名清单的Reader
		try {
			tableListReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(InfaProperty.getInstance().getProperty("work.dir")
							+ InfaProperty.getInstance().getProperty("tables"))));

			String table;
			while ((table = tableListReader.readLine()) != null) {
				tableList.add(table.trim().toUpperCase());
			}
		} catch (FileNotFoundException e) {
			log.error(e.getMessage());
			System.exit(-1);
		} catch (IOException e) {
			log.error(e.getMessage());
			System.exit(-1);
		} finally {
			try {
				tableListReader.close();
			} catch (IOException e) {
			}
		}
		return tableList;
	}
}
