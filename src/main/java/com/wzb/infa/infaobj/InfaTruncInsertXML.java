package com.wzb.infa.infaobj;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import com.wzb.infa.infaobj.base.BaseInfaXML;
import com.wzb.infa.infaobj.base.InfaCol;
import com.wzb.infa.infaobj.base.InfaTable;
import com.wzb.infa.infaobj.base.InfaXML;
import org.apache.log4j.Logger;
import org.dom4j.Element;

import com.wzb.infa.dbutils.InfaUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.MutiParamsTruncException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.properties.InfaProperty;

public class InfaTruncInsertXML extends BaseInfaXML implements InfaXML {

	public static Logger logger = Logger.getLogger(InfaTruncInsertXML.class);

	public InfaTruncInsertXML(String owner, String tableName, boolean addHyFlag)
			throws UnsupportedDatatypeException, SQLException, CheckTableExistException, MutiParamsTruncException {
		super();
		InfaProperty infaProperty = InfaProperty.getInstance();
		{
			// BUG 全删全插,如果不配置按分区删除则不支持多工作流,
			String[] mutiParams = infaProperty.getProperty("workflow.muti-params", "-1").replaceAll("\n", "")
					.split(";");
			String truncateByPartition  = infaProperty.getProperty("truncateByPartition", "-1");
			if (mutiParams.length > 1 && (!truncateByPartition.contains("PARTITION"))) {
				throw new MutiParamsTruncException("can not truncate in " + mutiParams.length + " workflows!!!");
			}
		}

		logger.debug("begin InfaTruncInsertXML:" + tableName + (addHyFlag ? " WHIT HY_ID" : ""));

		String targetName = getTargetName(owner, tableName);

		String sourceQualifierName = InfaUtil.infaProperty.getProperty("qualifier.prefix", "SQ_") + tableName;
		String expressionName = "EXPTRANS";
		String sqlFilter = InfaUtil.infaProperty.getProperty("sql.filter", "");
		String srcDBName = InfaUtil.infaProperty.getProperty("source.dbname", "SRCDB");
		String mappingName = InfaUtil.infaProperty.getProperty("map.prefix", "M_") + tableName
				+ InfaUtil.infaProperty.getProperty("map.suffix", "_INC");

		InfaTable infaTable = new InfaTable(owner, tableName);
		int infaTableColSize = infaTable.getCols().size();

		source = infaTable.createSource(tableName, srcDBName);
		target = infaTable.createTarget(targetName, false);

		Element srcQua = infaTable.createSourceQualifier(sourceQualifierName, sqlFilter);

		Element expression = infaTable.createExpression(expressionName, false);

		InfaCol hyid;
		InfaCol hyUpdateDate;
		InfaCol hyUpdateFlag;
		String hy_id;
		String hy_update_date;
		String hy_update_flag;
		String pkString = null;
		try {
			pkString = infaTable.getPkString();
		} catch (NoPrimaryKeyException e) {
			logger.debug(e.getMessage());
			// pkString
			InfaCol firstCOl = infaTable.getCols().get(0);

			if (firstCOl.getDataType().equals("timestamp") || firstCOl.getDataType().equals("date")) {
				pkString = "to_char(" + firstCOl.getColumnName() + ",'YYYYMMDDHH24MISS')";
			} else if (firstCOl.getDataType().equals("number(p,s)")) {
				pkString = "to_char(" + firstCOl.getColumnName() + ")";
			} else if (firstCOl.getDataType().contains("lob") || firstCOl.getDataType().contains("raw")
					|| firstCOl.getDataType().contains("long")) {
				pkString = "noPrimaryKey";
			} else {
				pkString = firstCOl.getColumnName();
			}
		}
		if (addHyFlag) {
			// 先看是否强制使用配置文件中的配置
			String force = infaProperty.getProperty("add.col.force", "false").toUpperCase();
			// 拿到配置文件中的配置(强制又不指定，用HY_ID字段)
			hy_id = infaProperty.getProperty("add.col.hy_id", "HY_ID").toUpperCase();
			hy_update_date = infaProperty.getProperty("add.col.hy_update_date", "HY_UPDATE_DATE").toUpperCase();
			hy_update_flag = infaProperty.getProperty("add.col.hy_update_flag", "HY_UPDATE_FLAG").toUpperCase();

			if ("TRUE".equals(force)) {
				// 如果强制用配置文件中的配置,直接用上面配置文件中的名称

			} else {
				// 不强制用配置文件中的配置

				// 如果默认的字段已经存在于源表中,用名称+"_$"
				if (infaTable.hasCol(hy_id)) {
					hy_id = "HY_ID_$";
				}
				if (infaTable.hasCol(hy_update_date)) {
					hy_update_date = "HY_UPDATE_DATE_$";
				}
				if (infaTable.hasCol(hy_update_flag)) {
					hy_update_flag = "HY_UPDATE_FLAG_$";
				}
			}

			hyid = new InfaCol(hy_id, "", "varchar2", "128", "0", "0", "NOTNULL", String.valueOf(infaTableColSize + 1),
					"NOT A KEY");

			hyUpdateDate = new InfaCol(hy_update_date, "", "DATE", "19", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 2), "NOT A KEY");

			hyUpdateFlag = new InfaCol(hy_update_flag, "", "VARCHAR2", "1", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 3), "NOT A KEY");

			// TARGET ADD COLS
			target.add(hyid.createTargetField(false));
			target.add(hyUpdateDate.createTargetField(false));
			target.add(hyUpdateFlag.createTargetField(false));
			// expression ADD COLS
			expression.add(hyid.createExpressionField(pkString));
			expression.add(hyUpdateDate.createExpressionField("sysdate"));
			expression.add(hyUpdateFlag.createExpressionField("1"));
		}

		mapping = InfaUtil.createMapping(mappingName);

		Element srcInst = InfaUtil.createInstance(source, tableName);
		Element quaInst = InfaUtil.createQualifierInstance(sourceQualifierName, tableName);
		Element expInst = InfaUtil.createTransformInstance(expression);
		Element tarInst = InfaUtil.createTargetInstance(targetName, targetName);

		ArrayList<Element> connectors = new ArrayList<>();
		connectors.addAll(InfaUtil.createConnector(srcInst, source, quaInst, srcQua));
		connectors.addAll(InfaUtil.createConnector(quaInst, srcQua, expInst, expression));
		connectors.addAll(InfaUtil.createConnector(expInst, expression, tarInst, target));

		mapping.add(srcQua);
		mapping.add(expression);
		mapping.add(srcInst);
		mapping.add(quaInst);
		mapping.add(expInst);
		mapping.add(tarInst);
		// connector加到mapping下
		for (Iterator<Element> it = connectors.iterator(); it.hasNext();) {
			// System.out.println(it.next().asXML());
			mapping.add(it.next());
		}

		mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
				tarInst.attributeValue("NAME"));
		// variable
		InfaUtil.createMappingVariables(mapping);

		mapping.addElement("ERPINFO");

		workflow = InfaUtil.createWorkflow(mapping, infaTable.getComment());

		// SCHEDULER
		Element scheduler = InfaUtil.createWorkflowScheduler("SCHEDULERPLAN");

		// TASK
		Element startTarsk = InfaUtil.createStartTarsk();

		// SESSION
		Element session = InfaUtil.createSession(mapping);

		// SESSTRANSFORMATIONINST
		Element sSrcIns = InfaUtil.createSessTransformationInst(srcInst, owner);
		sSrcIns.addElement("ATTRIBUTE").addAttribute("NAME", "Owner Name").addAttribute("VALUE", owner);
		Element squaInst = InfaUtil.createSessTransformationInst(quaInst, owner);
		Element sExpInst = InfaUtil.createSessTransformationInst(expInst, owner);
		Element sTarInst = InfaUtil.createSessTransformationInst(tarInst, owner);

		// CONFIGREFERENCE
		Element configreference = InfaUtil.createConfigreference();

		// SESSIONEXTENSION

		Element srcReaderSE = InfaUtil.createReaderSessionextension(quaInst.attributeValue("NAME"),
				srcInst.attributeValue("NAME"));
		Element quaReaderSE = InfaUtil.createReaderSessionextension(quaInst, "$DBConnectionSRC");
		Element tarWriteSE = InfaUtil.createWriterSessionextension(tarInst);

		((Element) tarWriteSE.selectSingleNode("ATTRIBUTE[@NAME='Truncate target table option']")).addAttribute("VALUE",
				"YES");

		session.add(sSrcIns);
		session.add(squaInst);
		session.add(sExpInst);
		session.add(sTarInst);

		session.add(configreference);

		session.add(srcReaderSE);
		session.add(quaReaderSE);
		session.add(tarWriteSE);

		// session ATTRIBUTE
		InfaUtil.createSessionAttribute(session);

		// TASKINSTANCE
		Element startTarskInstance = InfaUtil.createStartTarskInstance();
		Element sessionInstance = InfaUtil.createSessionInstance(session);

		// WORKFLOWLINK
		Element workflowLink = InfaUtil.createWorkflowLink("start", session.attributeValue("NAME"));

		// workFlow adds
		workflow.add(scheduler);
		workflow.add(startTarsk);
		workflow.add(session);
		workflow.add(startTarskInstance);
		workflow.add(sessionInstance);
		workflow.add(workflowLink);

		// WORKFLOWVARIABLE
		// session ATTRIBUTE
		InfaUtil.createWorkflowVariableAndAttribute(workflow, session);
		logger.debug("end InfaTruncInsertXML:" + tableName);
	}

}
