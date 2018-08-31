package com.wzb.infa.obj;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Element;

import com.wzb.infa.dbutils.InfaUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;

public class InfaTruncInsertXML extends BaseInfaXML implements InfaXML {

	public static Logger logger = Logger.getLogger(InfaTruncInsertXML.class);
	public InfaTruncInsertXML(String owner, String tableName, boolean addHyFlag)
			throws UnsupportedDatatypeException, SQLException, CheckTableExistException {
		super();
		logger.debug("begin InfaTruncInsertXML:"+tableName+(addHyFlag?" WHIT HY_ID":""));
		String targetName = InfaUtil.infaProperty.getProperty("target.prefix", "") + tableName;
		if (targetName.length() > 30) {
			targetName = targetName.substring(0, 30);
		}
		String sourceQualifierName = InfaUtil.infaProperty.getProperty("qualifier.prefix", "SQ_") + tableName;
		String expressionName = "EXPTRANS";
		String sqlFilter = InfaUtil.infaProperty.getProperty("sql.filter", "");
		String srcDBName = InfaUtil.infaProperty.getProperty("target.dbname", "SRCDB");
		String mappingName = InfaUtil.infaProperty.getProperty("map.prefix", "M_") + tableName
				+ InfaUtil.infaProperty.getProperty("map.suffix", "_INC");

		InfaTable infaTable = new InfaTable(owner, tableName);
		int infaTableColSize = infaTable.getCols().size();

		source = infaTable.createSource(tableName, srcDBName);
		target = infaTable.createTarget(targetName, false);

		Element srcQua = infaTable.createSourceQualifier(sourceQualifierName, sqlFilter);

		Element expression = infaTable.createExpression(expressionName, false);

		if (addHyFlag) {
			// ADD COLS
			InfaCol hyid = new InfaCol("HY_ID", "", "varchar2", "24", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 1), "NOT A KEY");

			InfaCol hyUpdateDate = new InfaCol("HY_UPDATE_DATE", "", "DATE", "19", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 2), "NOT A KEY");

			InfaCol hyUpdateFlag = new InfaCol("HY_UPDATE_FLAG", "", "VARCHAR2", "1", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 3), "NOT A KEY");
			// TARGET ADD COLS
			target.add(hyid.createTargetField(false));
			target.add(hyUpdateDate.createTargetField(false));
			target.add(hyUpdateFlag.createTargetField(false));
			// expression ADD COLS
			expression.add(hyid.createExpressionField(infaTable.getCols().get(0).getColumnName()));
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
		logger.debug("end InfaTruncInsertXML:"+tableName);
	}

}
