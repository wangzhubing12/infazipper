package com.wzb.infa.infazipper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.dom4j.DocumentException;

import com.wzb.infa.dbutils.DbUtil;
import com.wzb.infa.dbutils.XMLUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.obj.InfaXML;
import com.wzb.infa.properties.InfaProperty;

public class A1 {
	private static final Logger logger = Logger.getLogger(A1.class);

	public static void main(String[] args) {
		InfaProperty infaProperty = null;

		// 获取配置文件
		if (args.length > 0) {
			try {
				infaProperty = InfaProperty.getInstance().addInfaProperty(args[0]);
				logger.debug("load properties:" + infaProperty.size());
			} catch (DocumentException e) {
				logger.error("Property file error:" + args[0] + e.getMessage());
				System.exit(-1);
			}
		} else {
			logger.error("must give a propertie file");
			System.exit(-1);
		}

		XMLUtil xmlUtil = XMLUtil.getInstance(); // XML工具

		A1 a1 = new A1();
		HashSet<String> tableLIst = a1.getTableList(); // 表清单
		String fileName = infaProperty.getProperty("work.dir")
				+ infaProperty.getProperty("xml.output", "gen.xml").toLowerCase(); // 生成的文件名
		int size = tableLIst.size(); // 总的表数量
		int errorSize = 0; // 当前报错的表数量
		int sucessSize = 0;// 当前成功的表数量
		int tableSize = 0;// 当前生成的表总数量
		int fileCount = 0;// 当前生成的文件总数量
		int mapsize = Integer.parseInt(infaProperty.getProperty("xml.output.mappings", "20"));

		String mappingType = infaProperty.getProperty("mapping.type", "-1");
		ArrayList<InfaXML> xmls = new ArrayList<>(); // 保存生成的XML

		for (String table : tableLIst) {
			tableSize++;

			try {
				xmls.add(xmlUtil.createInfaXML(table, mappingType));
				logger.debug("success!");
				sucessSize++;
			} catch (UnsupportedDatatypeException | SQLException | CheckTableExistException e) {
				logger.error(e.getMessage());
				errorSize++;
			} catch (NoPrimaryKeyException e) {
				logger.error(e.getMessage());
				errorSize++;
			}
			logger.info("Make xml for " + StringPadder.rightPad(table, "-", 30) + "(" + tableSize + "/" + size
					+ ")---success:" + sucessSize);
			// 如果生成的表数量达到xx个则切换文件(排除掉报错的)
			if (((sucessSize > 0) && (sucessSize % mapsize) == 0) || tableSize >= size) {

				logger.info("write InfaXML To File:" + fileName.replace(".xml", fileCount + ".xml"));
				try {
					xmlUtil.writeInfaXMLToFile(xmls, fileName.replace(".xml", fileCount + ".xml"));
					fileCount++;
					xmls.clear();
				} catch (IOException e) {
					logger.error(e.getMessage());
					System.exit(-1);
				}
			}

		}
		logger.info("----------------------------------------------------------------");
		logger.info("MAKE FILES:" + (fileCount) + ",WITH ERROR TABLES:" + errorSize);
		DbUtil.getInstance().release();
	}

	/***
	 * 获取要生成的表清单
	 */
	private HashSet<String> getTableList() {
		HashSet<String> tableList = new HashSet<>();// 保存要生成的表清单
		BufferedReader tableListReader = null;// 读取要生成的表名清单的Reader
		try {

			String propertieFileName = InfaProperty.getInstance().getProperty("work.dir")
					+ InfaProperty.getInstance().getProperty("tables");
			logger.debug("Property File Name:" + propertieFileName);
			tableListReader = new BufferedReader(new InputStreamReader(new FileInputStream(propertieFileName)));

			String table;
			while ((table = tableListReader.readLine()) != null) {
				tableList.add(table.trim().toUpperCase());
			}
		} catch (FileNotFoundException e) {
			logger.error("找不到配置文件：" + e.getMessage());
			System.exit(-1);
		} catch (IOException e) {
			logger.error(e.getMessage());
			System.exit(-1);
		} finally {
			try {
				tableListReader.close();
			} catch (IOException e) {
				logger.error("ddd" + e.getMessage());
			}
		}
		return tableList;
	}

	static class StringPadder {

		private static StringBuffer strb;
		private static StringCharacterIterator sci;

		public static String rightPad(String stringToPad, String padder, int size) {
			if (padder.length() == 0) {
				return stringToPad;
			}
			strb = new StringBuffer(stringToPad);
			sci = new StringCharacterIterator(padder);

			while (strb.length() < size) {
				for (char ch = sci.first(); ch != CharacterIterator.DONE; ch = sci.next()) {
					if (strb.length() < size) {
						strb.append(String.valueOf(ch));
					}
				}
			}
			return strb.toString();
		}
	}
}
