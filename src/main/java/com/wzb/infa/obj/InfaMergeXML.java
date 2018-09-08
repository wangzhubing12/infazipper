package com.wzb.infa.obj;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Element;

import com.wzb.infa.dbutils.InfaUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.DuplicateColumnExceptiion;
import com.wzb.infa.exceptions.NoColFoundException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.properties.InfaProperty;

public class InfaMergeXML extends BaseInfaXML implements InfaXML {

	private Logger logger = Logger.getLogger(InfaMergeXML.class);

	public InfaMergeXML(String owner, String tableName) throws UnsupportedDatatypeException, SQLException,
			CheckTableExistException, NoPrimaryKeyException, NoColFoundException {
		super();
		logger.debug("begin InfaMergeXML:" + tableName);

		InfaProperty infaProperty = InfaProperty.getInstance();

		String targetName = getTargetName(owner, tableName);
		logger.debug("getTargetName:" + owner + "." + tableName + " ==>" + targetName);
		String incByDateColName = infaProperty.getProperty("incByDateColName", "CZSJ");
		String sqlFilter = infaProperty.getProperty("sql.filter", "");
		if ("".equals(sqlFilter)) {
			sqlFilter = incByDateColName + ">to_date('$$INCTIME','MM/DD/YYYY HH24:MI:SS')";
		} else {
			sqlFilter = sqlFilter + " AND " + incByDateColName + ">to_date('$$INCTIME','MM/DD/YYYY HH24:MI:SS')";
		}

		String srcDBName = infaProperty.getProperty("target.dbname", "TARDB");
		String mappingName = infaProperty.getProperty("map.prefix", "M_") + tableName
				+ infaProperty.getProperty("map.suffix", "_INC");

		InfaTable srcTable = new InfaTable(owner, tableName);
		if (!srcTable.isHasPk()) {
			throw new NoPrimaryKeyException(owner + "." + tableName + " NoPrimaryKey!");
		}
		InfaTable tarTable = srcTable.copy(targetName);
		if (!srcTable.hasCol(incByDateColName)) {
			throw new NoColFoundException(owner + "." + tableName + "." + incByDateColName + " NoColFound!");
		}
		InfaCol hyid;
		InfaCol hyUpdateDate;
		InfaCol hyUpdateFlag;
		InfaCol setParam; // 设置参数的字段
		String hy_id;
		String hy_update_date;
		String hy_update_flag;
		String pkString = srcTable.getPkString();
		{

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
				if (tarTable.hasCol(hy_id)) {
					hy_id = "HY_ID_$";
				}
				if (tarTable.hasCol(hy_update_date)) {
					hy_update_date = "HY_UPDATE_DATE_$";
				}
				if (tarTable.hasCol(hy_update_flag)) {
					hy_update_flag = "HY_UPDATE_FLAG_$";
				}
			}
			// ADD COLS
			int infaTableColSize = tarTable.getCols().size();

			hyid = new InfaCol(hy_id, "", "varchar2", "128", "0", "0", "NOTNULL", String.valueOf(infaTableColSize + 1),
					"NOT A KEY");

			hyUpdateDate = new InfaCol(hy_update_date, "", "DATE", "19", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 2), "NOT A KEY");

			hyUpdateFlag = new InfaCol(hy_update_flag, "", "VARCHAR2", "1", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 3), "NOT A KEY");

			setParam = new InfaCol("SETPARAM", "", "DATE", "19", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 4), "NOT A KEY");

			try {
				tarTable.addCol(hyid);
				tarTable.addCol(hyUpdateDate);
				tarTable.addCol(hyUpdateFlag);
			} catch (DuplicateColumnExceptiion e) {
				logger.fatal(e.getMessage());
			}

		}

		// 源和目标
		source = srcTable.createSource(tableName, srcDBName);
		target = tarTable.createTarget(targetName, false);
		((Element) target.selectSingleNode("TARGETFIELD[@NAME='" + hy_id + "']")).addAttribute("KEYTYPE", "PRIMARY KEY")
				.addAttribute("NULLABLE", "NOTNULL");

		// 构造中间组件
		Element srcQua = srcTable.createSourceQualifier("SQ_S" + tableName, sqlFilter);

		String strategy = infaProperty.getProperty("merge.strategy", "DD_UPDATE").toUpperCase();
		if ("DD_UPDATE".equals(strategy) || "DD_INSERT".equals(strategy)) {

		} else {
			strategy = "DD_UPDATE";
		}
		Element updStg = srcTable.createUpdateStrategy("UPD_" + tableName, strategy, false, null);

		Element updateExp = srcTable.createExpression("EXP_UPDATE", false);
		updateExp.add(hyid.createExpressionField(pkString));
		updateExp.add(hyUpdateDate.createExpressionField("sysdate"));
		updateExp.add(hyUpdateFlag.createExpressionField("2"));
		updateExp.add(setParam.createExpressionField("SETVARIABLE($$INCTIME," + incByDateColName + ")")
				.addAttribute("PORTTYPE", "LOCAL VARIABLE"));

		mapping = InfaUtil.createMapping(mappingName);

		// Instance
		Element sSrcInst = InfaUtil.createInstance(source, tableName);

		Element srcQuaInst = InfaUtil.createQualifierInstance("SQ_S" + tableName, tableName);

		Element updStgInst = InfaUtil.createTransformInstance(updStg);

		Element updateExpInst = InfaUtil.createTransformInstance(updateExp);

		Element tarUpdateInst = InfaUtil.createTargetInstance("UPDATE_" + targetName, targetName);

		ArrayList<Element> connectors = new ArrayList<>();
		connectors.addAll(InfaUtil.createConnector(sSrcInst, source, srcQuaInst, srcQua));
		connectors.addAll(InfaUtil.createConnector(srcQuaInst, srcQua, updStgInst, updStg));
		connectors.addAll(InfaUtil.createConnector(updStgInst, updStg, updateExpInst, updateExp));
		connectors.addAll(InfaUtil.createConnector(updateExpInst, updateExp, tarUpdateInst, target));

		mapping.add(srcQua);// Source Qualifier加到mapping下
		mapping.add(updateExp);
		mapping.add(updStg);

		mapping.add(sSrcInst);
		mapping.add(srcQuaInst);
		mapping.add(updateExpInst);
		mapping.add(updStgInst);
		mapping.add(tarUpdateInst);

		// connector加到mapping下
		for (Iterator<Element> it = connectors.iterator(); it.hasNext();) {
			// System.out.println(it.next().asXML());
			mapping.add(it.next());
		}

		mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
				tarUpdateInst.attributeValue("NAME"));

		// variable
		InfaUtil.createMappingVariables(mapping);
		{
			// 单独增加增量的那个变量
			mapping.addElement("MAPPINGVARIABLE").addAttribute("AGGFUNCTION", "MAX")
					.addAttribute("DATATYPE", "date/time").addAttribute("DEFAULTVALUE", "01/01/1800 0024:00:00")
					.addAttribute("DESCRIPTION", "").addAttribute("ISEXPRESSIONVARIABLE", "NO")
					.addAttribute("ISPARAM", "NO").addAttribute("NAME", "$$INCTIME").addAttribute("PRECISION", "29")
					.addAttribute("SCALE", "9").addAttribute("USERDEFINED", "YES");

		}
		mapping.addElement("ERPINFO");

		workflow = InfaUtil.createWorkflow(mapping, srcTable.getComment());

		// SCHEDULER
		Element scheduler = InfaUtil.createWorkflowScheduler("SCHEDULERPLAN");

		// TASK
		Element startTarsk = InfaUtil.createStartTarsk();

		// SESSION
		Element session = InfaUtil.createSession(mapping);

		// SESSTRANSFORMATIONINST
		Element sSrcInstS = InfaUtil.createSessTransformationInst(sSrcInst, owner);
		Element srcQuaInstS = InfaUtil.createSessTransformationInst(srcQuaInst, owner);
		Element updateExpInstS = InfaUtil.createSessTransformationInst(updateExpInst, owner);
		Element updStgInstS = InfaUtil.createSessTransformationInst(updStgInst, owner);
		Element tarUpdateInstS = InfaUtil.createSessTransformationInst(tarUpdateInst, owner);
		// 源的owner需要修改
		sSrcInstS.addElement("ATTRIBUTE").addAttribute("NAME", "Owner Name").addAttribute("VALUE", owner);

		// CONFIGREFERENCE
		Element configreference = InfaUtil.createConfigreference();

		// SESSIONEXTENSION
		Element sSrcInstSE = InfaUtil.createReaderSessionextension(srcQua.attributeValue("NAME"),
				source.attributeValue("NAME"));

		Element srcQuaInstSE = InfaUtil.createReaderSessionextension(srcQua, "$DBConnectionSRC");

		Element tarUpdateInstSE = InfaUtil.createWriterSessionextension(tarUpdateInst);
		((Element) tarUpdateInstSE.selectSingleNode("ATTRIBUTE[@NAME='Update as Update']")).addAttribute("VALUE", "NO");
		((Element) tarUpdateInstSE.selectSingleNode("ATTRIBUTE[@NAME='Update else Insert']")).addAttribute("VALUE",
				"YES");
		session.add(sSrcInstS);
		session.add(srcQuaInstS);
		session.add(updateExpInstS);
		session.add(updStgInstS);
		session.add(tarUpdateInstS);

		session.add(configreference);

		session.add(sSrcInstSE);
		session.add(srcQuaInstSE);
		session.add(tarUpdateInstSE);

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
		logger.debug("end InfaMergeXML:" + tableName);
	}

}
