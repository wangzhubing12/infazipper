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
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.obj.InfaAddLogXML;
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
		logger.debug("begin createDocument");
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
		logger.debug("end createDocument");
		return doc;
	}

	public void writeInfaXMLToFile(ArrayList<InfaXML> Infaxmls, String toFileName) throws IOException {
		logger.debug("begin writeInfaXMLToFile");
		OutputFormat format = new OutputFormat("    ", true);
		format.setEncoding("GBK");
		File xmloutFile = new File(toFileName);
		
		XMLWriter xmlWriter = new XMLWriter(new FileOutputStream(xmloutFile), format);
		
		Document doc = createDocument();
		Element folder = (Element) doc.selectSingleNode("//FOLDER");

		String[] mutiParams = infaProperty.getProperty("workflow.muti-params", "-1").replaceAll("\n", "").split(";");
		Element workflow;
		for (InfaXML xml : Infaxmls) {
			logger.debug("addToFolder:" + xml.getWorkflowName());

			workflow = xml.getWorkflow();
			if (!"-1".equals(mutiParams[0])) {
				logger.debug("deal mutiParams:" + xml.getWorkflowName());
				// 先添加其他部分
				folder.add(xml.getSource());
				if (xml.getTarSource() == null) {
				} else {
					folder.add(xml.getTarSource());
				}
				folder.add(xml.getTarget());
				folder.add(xml.getMapping());
				Element e;
				String wfName = workflow.attributeValue("NAME");
				String wfNameAdd;
				String wfParam;
				for (String mutiParam : mutiParams) {
					e = workflow.createCopy();
					wfNameAdd = mutiParam.split(":")[0].trim();
					wfParam = mutiParam.split(":")[1].trim();
					// 修改工作流名称
					e.addAttribute("NAME", wfName + wfNameAdd);
					// 修改参数文件路径
					((Element) e.selectSingleNode("ATTRIBUTE[@NAME='Parameter Filename']")).addAttribute("VALUE",
							wfParam);
					folder.add(e);// workflow加到folder下
				}
			} else {
				xml.addToFolder(folder);// workflow加到folder下
			}

		}
		xmlWriter.write(doc);
		xmlWriter.flush();
		logger.debug("end writeInfaXMLToFile");
	}

	public InfaXML createInfaXML(String table, String xmlType)
			throws UnsupportedDatatypeException, SQLException, CheckTableExistException, NoPrimaryKeyException {
		logger.debug("begin createInfaXML"+table);
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
			try {
				xml = new InfaZipperXML(owner, sourceTablename);
			} catch (NoPrimaryKeyException e) {
				String truncateIfAddError = infaProperty.getProperty("truncIfAddError", "NO");
				if ("YES".equals(truncateIfAddError)) {
					logger.debug("make all mapping because truncIfAddError=YES");
					xml = new InfaTruncInsertXML(owner, sourceTablename, true);
				} else {
					throw e;
				}
			}
			break;
		case "2":
			try {
				xml = new InfaAddXML(owner, sourceTablename);
			} catch (NoPrimaryKeyException e) {
				String truncateIfAddError = infaProperty.getProperty("truncIfAddError", "NO");
				if ("YES".equals(truncateIfAddError)) {
					
					xml = new InfaTruncInsertXML(owner, sourceTablename, true);
				} else {
					throw e;
				}
			}
			break;
                case "2.1":
			try {
				xml = new InfaAddLogXML(owner, sourceTablename);
			} catch (NoPrimaryKeyException e) {
				String truncateIfAddError = infaProperty.getProperty("truncIfAddError", "NO");
				if ("YES".equals(truncateIfAddError)) {
					
					xml = new InfaTruncInsertXML(owner, sourceTablename, true);
				} else {
					throw e;
				}
			}
			break;
		case "3":
			xml = new InfaTruncInsertXML(owner, sourceTablename, false);
			break;
		default:
			xml = new InfaTruncInsertXML(owner, sourceTablename, true);
			break;
		}
		logger.debug("end createInfaXML");

		return xml;
	}

}
