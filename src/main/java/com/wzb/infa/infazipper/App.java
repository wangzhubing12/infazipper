package com.wzb.infa.infazipper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import com.wzb.infa.dbutils.InfaUtils;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.properties.InfaProperty;

/**
 * Hello world!
 *
 */
public class App {
	public InfaProperty infaProperty = InfaProperty.getInstance();
	private Document doc = null;
	private Logger log = Logger.getLogger(App.class);
	private XMLWriter xmlWriter = null;
	private Connection srcConnect = null;
	private Connection tarConnect = null;
	private InfaUtils infaUtil = new InfaUtils();

	public static void main(String[] args) throws DocumentException, IOException {

		App app = new App();
		if (args.length > 0) {
			// 支持自定义属性
			app.infaProperty.addInfaProperty(args[0]);
		}
		app.init();
		app.run();
		app.quit(0);
	}

	private void init() throws DocumentException, IOException {
		// 数据库连接
		srcConnect = getConnection("s");
		// 初始化错误日志记录
		File noTableLogTo = new File(infaProperty.getProperty("tables.notable"));
		File noPrimaryKeyLogTo = new File(infaProperty.getProperty("tables.noprimarykey"));
		if (!noTableLogTo.exists()) {
			noTableLogTo.createNewFile();
		}
		if (!noPrimaryKeyLogTo.exists()) {
			noPrimaryKeyLogTo.createNewFile();
		}
		initXMLWriter(infaProperty.getProperty("xml.output"));
	}

	private void initXMLWriter(String fileName) {
		// 初始化XML文件打印格式
		OutputFormat format = new OutputFormat("    ", true);
		format.setEncoding("UTF-8");
		try {
			if ("sysout".equals(fileName)) {
				xmlWriter = new XMLWriter(format);
			} else {
				File xmloutFile = new File(fileName);
				xmlWriter = new XMLWriter(new FileOutputStream(xmloutFile), format);
			}
		} catch (UnsupportedEncodingException e) {
			log.warn(e.getMessage());
		} catch (FileNotFoundException e) {
			log.warn(e);
		}

		// 设置INFA XML文件头、POWERMART、REPOSITORY、FOLDER部分
		doc = DocumentHelper.createDocument();
		doc.addDocType("POWERMART", "", "powrmart.dtd");
		Element powrmart = doc.addElement("POWERMART").addAttribute("CREATION_DATE", "04/02/2018 12:47:01")
				.addAttribute("REPOSITORY_VERSION", "186.95");
		Element repository = powrmart.addElement("REPOSITORY").addAttribute("NAME", "infa_rs")
				.addAttribute("VERSION", "186").addAttribute("CODEPAGE", "UTF-8")
				.addAttribute("DATABASETYPE", "Oracle");
		repository.addElement("FOLDER").addAttribute("NAME", infaProperty.getProperty("infa.folder", "TEST"))
				.addAttribute("GROUP", "").addAttribute("OWNER", "pccontroller").addAttribute("SHARED", "NOTSHARED")
				.addAttribute("DESCRIPTION", "").addAttribute("PERMISSIONS", "rwx---r--")
				.addAttribute("UUID", "7ff86550-beb3-4ac6-832d-50ad401c0e97");
	}

	private void run() throws IOException {
		// 定义参数
		ArrayList<String> tableList = new ArrayList<String>();// 保存要生成的表清单
		BufferedReader tableListReader = null;// 读取要生成的表名清单的Reader
		BufferedWriter noTableLogWriter = null;// 数据库没有这个表
		BufferedWriter noPrimaryKeyLogWriter = null;// 数据库表没有主键

		String table = null;
		try {
			// 初始化参数-tableFileFileReader
			tableListReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(infaProperty.getProperty("tables"))));

			noTableLogWriter = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(infaProperty.getProperty("tables.notable"))));

			noPrimaryKeyLogWriter = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(infaProperty.getProperty("tables.noprimarykey"))));
			while ((table = tableListReader.readLine()) != null) {
				tableList.add(table.trim().toUpperCase());
			}
			// 制作XML
			int tableCount = 0;// 已经生成XML的表数量
			for (Iterator<String> it = tableList.iterator(); it.hasNext();) {
				table = it.next();
				System.out.print("begin to make xml for " + table);
				try {
					makeZipperXML(table);
					System.out.println("\tsuccess:");
					tableCount++;
				} catch (NoPrimaryKeyException e) {
					noPrimaryKeyLogWriter.write(table);
					noPrimaryKeyLogWriter.newLine();
					noPrimaryKeyLogWriter.flush();
					System.out.println("\tfailed:" + e.getMessage());
				} catch (CheckTableExistException e) {
					noTableLogWriter.write(table);
					noTableLogWriter.newLine();
					noTableLogWriter.flush();
					System.out.println("\tfailed:" + e.getMessage());
				}
				if ((tableCount % Integer.parseInt(infaProperty.getProperty("xml.output.mappings"))) == 0) {
					printXML(".");
					int fileCount = Math.floorDiv(tableCount,
							Integer.parseInt(infaProperty.getProperty("xml.output.mappings")));
					initXMLWriter(
							infaProperty.getProperty("xml.output").toLowerCase().replace(".xml", fileCount + ".xml"));
				}
			}
			printXML(".");
		} catch (FileNotFoundException e) {
			log.warn(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			log.warn(e.getMessage());
		} catch (IOException e) {
			log.warn(e.getMessage());
		} catch (NullPointerException e) {
			printXML(".");
			e.printStackTrace();
		} finally {
			try {
				tableListReader.close();
				noTableLogWriter.close();
				noPrimaryKeyLogWriter.close();
			} catch (Exception e) {
				log.warn(e.getMessage());
			}
		}
	}

	private void printXML(String path) {
		String v_path = "//" + path;
		try {
			xmlWriter.write(doc.selectSingleNode(v_path));
			xmlWriter.flush();
		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

	private Connection getConnection(String flag) {
		// 检查是否已经连接，如果已经连接则返回！
		if ("s".equals(flag) && srcConnect != null) {
			return srcConnect;
		}
		if ("t".equals(flag) && tarConnect != null) {
			return tarConnect;
		}
		// 如果没有连接则开始建立连接
		Connection connection = null;
		try {
			Class.forName(infaProperty.getProperty("source.driver"));
			String url = infaProperty.getProperty("source.url");
			String username = infaProperty.getProperty("source.username");
			String password = infaProperty.getProperty("source.password");
			connection = DriverManager.getConnection(url, username, password);
			log.info(username + " connect success!");
		} catch (ClassNotFoundException e) {
			log.fatal(e.getMessage());
		} catch (SQLException e) {
			log.fatal(e.getMessage());
		}

		return connection;
	}

	public void closeConnection(Connection connection) {
		try {
			connection.close();
			log.info("connection closed success!");
		} catch (SQLException e) {
			log.fatal(e.getMessage());
		}
	}

	private void quit(int status) {
		try {
			if (srcConnect != null) {
				srcConnect.close();
			}
			if (tarConnect != null) {
				tarConnect.close();
			}
		} catch (SQLException e) {
			log.fatal(e.getMessage());
		}
		log.info("quit " + status);
	}

	private void makeZipperXML(String tableName) throws NoPrimaryKeyException, CheckTableExistException {

		Element srcSource = importSource(tableName);
		Element tarSource = infaUtil.copySourceAsZipperSource(srcSource);

		// 复制第二个源作为目标
		Element target = infaUtil.copySourceAsTarget(tarSource);
		// TODO 目标长度大于30时
		if (target.attributeValue("NAME").length() > 30)
			target.attribute("NAME").setValue(target.attributeValue("NAME").substring(1, 30));
		// 开发mapping
		Element mapping = infaUtil.createZipperMapping(srcSource, tarSource, target);
		// 设置CONFIG
		// Element sessionConfiguration = infaUtil.createSessionConfiguration();
		// 开发work flow
		Element workflow = infaUtil.createWorkflow(mapping);

		Element folder = ((Element) doc.selectSingleNode("//FOLDER"));
		folder.add(srcSource);// srcSource加到folder下，
		folder.add(tarSource);// 第二个源加到folder下, tar source加到folder下
		folder.add(target);// target加到folder下
		folder.add(mapping);// mapping加到folder下
		// folder.add(sessionConfiguration);// sessionConfiguration加到folder下
		folder.add(workflow);// mapping加到folder下

	}

	private Element importSource(String tableName) throws NoPrimaryKeyException, CheckTableExistException {

		// 获取需要生成的表名和所在用户参数
		String table = tableName;
		String owner = infaProperty.getProperty("source.owner").toUpperCase();

		// 根据是否存在对应的XML源表，判断是否已经生成过，如果已经生成，则返回
		try {
			int nodeCount = ((Element) doc.selectSingleNode("//FOLDER/SOURCE[@NAME='" + table + "']")).nodeCount();
			if (nodeCount > 0) {
				log.warn(table + " allready exist xml ,cancel make xml!");
				return null;
			}
		} catch (NullPointerException e) {

		}
		// 根据表名、用户判断表是否存在
		if (!infaUtil.checkTableExist(srcConnect, owner, table)) {
			return null;
		}

		// 开始导入source表的XML
		Element srcSource = null;
		srcSource = infaUtil.importSource(srcConnect, owner, table);
		return srcSource;
	}
}
