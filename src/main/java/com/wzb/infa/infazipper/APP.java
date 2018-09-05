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

import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.dom4j.DocumentException;

import com.wzb.infa.dbutils.DbUtil;
import com.wzb.infa.dbutils.XMLUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.obj.InfaXML;
import com.wzb.infa.properties.InfaProperty;

public class APP {
	private static final Logger logger = Logger.getLogger(APP.class);

	public static void main(String[] args) {

		InfaProperty infaProperty = null;

		// 获取配置文件
		if (args.length > 0) {
			try {
				infaProperty = InfaProperty.getInstance().addInfaProperty(args[0]);
				{
					String errorFileName = infaProperty.getProperty("work.dir")
							+ infaProperty.getProperty("tables.error", "infaxml_error.log").toLowerCase(); // 生成的文件名
					String infoFileName = infaProperty.getProperty("work.dir")
							+ infaProperty.getProperty("tables.info", "infaxml_info.log").toLowerCase(); // 生成的文件名

					// 设置报错写入到日志文件的文件名
					Logger rootLog = LogManager.getRootLogger();
					FileAppender errorFileAppender = ((FileAppender) rootLog.getAppender("errorfile"));
					FileAppender infoFileAppender = ((FileAppender) rootLog.getAppender("infofile"));
					errorFileAppender.setFile(errorFileName);
					infoFileAppender.setFile(infoFileName);
					errorFileAppender.activateOptions();
					infoFileAppender.activateOptions();
				}
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

		APP a1 = new APP();
		HashSet<String> tableLIst = a1.getTableList(); // 表清单
		String fileName = infaProperty.getProperty("work.dir")
				+ infaProperty.getProperty("xml.output", "gen.xml").toLowerCase(); // 生成的文件名

		int size = tableLIst.size(); // 总的表数量
		int errorSize = 0; // 当前报错的表数量
		int sucessSize = 0;// 当前成功的表数量
		int tableSize = 0;// 当前生成的表总数量
		int fileCount = 0;// 当前生成的文件总数量
		int currentSuccess;
		int mapsize = Integer.parseInt(infaProperty.getProperty("xml.output.mappings", "20"));

		String mappingType = infaProperty.getProperty("mapping.type", "-1");
		ArrayList<InfaXML> xmls = new ArrayList<>(); // 保存生成的XML

		for (String table : tableLIst) {
			tableSize++;
			currentSuccess = -1;
			try {
				// 创建XML并加入到ArrayList<InfaXML>
				xmls.add(xmlUtil.createInfaXML(table, mappingType));
				logger.debug("success!");
				sucessSize++;
				currentSuccess = 1;
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
			if (((sucessSize > 0) && (currentSuccess == 1) && (sucessSize % mapsize) == 0) || tableSize >= size) {

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
		// 保存要生成的表清单
		HashSet<String> tableList = new HashSet<>();
		// 保存要生成的目标表清单，判断是否有多个源表加上前缀在截取30位后对应相同目标表
		HashSet<String> tarTableList = new HashSet<>();
		// 读取要生成的表名清单的Reader
		BufferedReader tableListReader = null;
		// 目标表前缀
		String targetTabPrefix = InfaProperty.getInstance().getProperty("target.prefix", "");
		// 目标表获取规则，default则是前缀加表名，database则从数据库取
		String rule = InfaProperty.getInstance().getProperty("target.name.rule", "default");
		if ("database".equals(rule)) {
			logger.debug("target name rule:database");
		} else {
			logger.debug("target name rule:default prifix + source name");
		}
		try {
			String tableListFileName = InfaProperty.getInstance().getProperty("work.dir")
					+ InfaProperty.getInstance().getProperty("tables");
			logger.debug("Table List File Name:" + tableListFileName);

			tableListReader = new BufferedReader(new InputStreamReader(new FileInputStream(tableListFileName)));

			String line;
			String table;
			String tarTable;
			while ((line = tableListReader.readLine()) != null) {
				if (line.length() == 0)
					continue;
				// 源表名
				table = line.trim().toUpperCase();
				// 从数据库获取表前缀的话，无需判断表长度
				if ("database".equals(rule)) {
					tableList.add(table.trim().toUpperCase());
				} else {
					// target table 长度，需要判断源表是否带有OWNER，只取表名部分
					if (line.contains(".")) {
						tarTable = targetTabPrefix + line.split("\\.")[1].trim().toUpperCase();

					} else {
						tarTable = targetTabPrefix + line.trim().toUpperCase();

					}
					if (tarTable.length() > 30)
						tarTable = tarTable.substring(0, 30);

					if (!tarTableList.contains(tarTable)) {
						tarTableList.add(tarTable);
						tableList.add(table.trim().toUpperCase());
					} else {
						logger.error("Same target table name found:" + tarTable + ",ignore this table!");
					}
				}
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
