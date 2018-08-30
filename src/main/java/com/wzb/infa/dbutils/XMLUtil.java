package com.wzb.infa.dbutils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.obj.InfaAddXML;
import com.wzb.infa.obj.InfaTruncInsertXML;
import com.wzb.infa.obj.InfaXML;
import com.wzb.infa.obj.InfaZipperXML;
import com.wzb.infa.properties.InfaProperty;

public class XMLUtil {
	private static XMLUtil xmlUtil;

	private XMLUtil() {
	}

	public static XMLUtil getInstance() {
		if (xmlUtil == null) {
			xmlUtil = new XMLUtil();
		}
		return xmlUtil;
	}

	public final InfaProperty infaProperty = InfaProperty.getInstance();
	public static Logger logger = Logger.getLogger(XMLUtil.class);
	private Document createDocument() throws IOException {
		logger.info("begin createDocument");
		// 设置INFA XML文件头、POWERMART、REPOSITORY、FOLDER部分
		Document doc = DocumentHelper.createDocument();
		doc.addDocType("POWERMART", "", "powrmart.dtd");
		String createDate = (new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")).format(new Date());
		Element powrmart = doc.addElement("POWERMART").addAttribute("CREATION_DATE", createDate)
				.addAttribute("REPOSITORY_VERSION", "186.95");
		Element repository = powrmart.addElement("REPOSITORY").addAttribute("NAME", "infa_rs")
				.addAttribute("VERSION", "186").addAttribute("CODEPAGE", "UTF-8")
				.addAttribute("DATABASETYPE", "Oracle");
		repository.addElement("FOLDER").addAttribute("NAME", infaProperty.getProperty("infa.folder", "TEST"))
				.addAttribute("GROUP", "").addAttribute("OWNER", "pccontroller").addAttribute("SHARED", "NOTSHARED")
				.addAttribute("DESCRIPTION", "").addAttribute("PERMISSIONS", "rwx---r--")
				.addAttribute("UUID", "7ff86550-beb3-4ac6-832d-50ad401c0e97");
		logger.info("end createDocument");
		return doc;
	}

	public void writeInfaXMLToFile(ArrayList<InfaXML> Infaxmls, String toFileName) throws IOException {
		logger.info("begin writeInfaXMLToFile");
		OutputFormat format = new OutputFormat("    ", true);
		format.setEncoding("GBK");
		File xmloutFile = new File(toFileName);
		XMLWriter xmlWriter = new XMLWriter(new FileOutputStream(xmloutFile), format);
		Document doc = createDocument();
		Element folder = (Element) doc.selectSingleNode("//FOLDER");
		for (InfaXML xml : Infaxmls) {
			logger.info("addToFolder:"+xml.getWorkflowName());
			xml.addToFolder(folder);
			
			
			
		}
		xmlWriter.write(doc);
		logger.info("end writeInfaXMLToFile");
	}

	public InfaXML createInfaXML(String table, String xmlType)
			throws UnsupportedDatatypeException, SQLException, CheckTableExistException {
		logger.info("begin createInfaXML");
		InfaXML xml;
		String username = infaProperty.getProperty("source.username").toUpperCase();
		String sourceTablename;
		String owner;
		// 处理表名
		if (table.contains(".")) {
			owner = table.split("\\.")[0];
			sourceTablename = infaProperty.getProperty("source.prefix", "").toUpperCase() + table.split("\\.")[1];

		} else {
			owner = infaProperty.getProperty("source.owner", username).toUpperCase();
			sourceTablename = infaProperty.getProperty("source.prefix", "").toUpperCase() + table;
		}

		// 表名长度大于30时
		if (sourceTablename.length() > 30) {
			sourceTablename = sourceTablename.substring(0, 30);
		}

		switch (xmlType) {
		case "1":
			xml = new InfaZipperXML(owner, sourceTablename);
			break;
		case "2":
			xml = new InfaAddXML(owner, sourceTablename);
			break;

		default:
			xml = new InfaTruncInsertXML(owner, sourceTablename, false);
			break;
		}
		logger.info("end createInfaXML");
			
		return xml;
	}

}
