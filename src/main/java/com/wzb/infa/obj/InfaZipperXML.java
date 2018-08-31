package com.wzb.infa.obj;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Element;

import com.wzb.infa.dbutils.InfaUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.properties.InfaProperty;

public class InfaZipperXML extends BaseInfaXML implements InfaXML {

	public static Logger logger = Logger.getLogger(InfaZipperXML.class);

	public InfaZipperXML(String owner, String tableName)
			throws UnsupportedDatatypeException, SQLException, CheckTableExistException, NoPrimaryKeyException {
		super();
		logger.debug("begin InfaZipperXML:" + tableName);

		InfaProperty infaProperty = InfaProperty.getInstance();
		String targetName = infaProperty.getProperty("target.prefix", "") + tableName;
		if (targetName.length() > 30) {
			targetName = targetName.substring(0, 30);
		}
		String sqlFilter = infaProperty.getProperty("sql.filter", "");
		String srcDBName = infaProperty.getProperty("target.dbname", "TARDB");
		String tarDBName = infaProperty.getProperty("target.dbname", "SRCDB");
		String mappingName = infaProperty.getProperty("map.prefix", "M_") + tableName
				+ infaProperty.getProperty("map.suffix", "_INC");

		InfaTable srcTable = new InfaTable(owner, tableName);
		if (!srcTable.isHasPk()) {
			throw new NoPrimaryKeyException(owner + "." + tableName + " NoPrimaryKey!");
		}
		InfaTable tarTable = srcTable.copy(targetName);

		InfaCol qysj;
		InfaCol sxsj;
		InfaCol jlzt;
		InfaCol crc32Col;
		String crc32 = srcTable.getCRC32String();
		// String pkString = srcTable.getPkString();
		{
			String _qysj;
			String _sxsj;
			String _jlzt;
			// 先看是否强制使用配置文件中的配置
			String force = infaProperty.getProperty("zipper.col.force", "false").toUpperCase();
			// 拿到配置文件中的配置(强制又不指定，用QYSJ字段)
			_qysj = infaProperty.getProperty("zipper.col.qysj", "QYSJ").toUpperCase();
			_sxsj = infaProperty.getProperty("zipper.col.sxsj", "SXSJ").toUpperCase();
			_jlzt = infaProperty.getProperty("zipper.col.jlzt", "JLZT").toUpperCase();
			if ("TRUE".equals(force)) {
				// 如果强制用配置文件中的配置,直接用上面配置文件中的名称

			} else {
				// 不强制用配置文件中的配置

				// 如果默认的字段已经存在于源表中,用名称+"_$"
				if (tarTable.hasCol(_qysj)) {
					_qysj = "QYSJ_$";
				}
				if (tarTable.hasCol(_sxsj)) {
					_sxsj = "SXSJ_$";
				}
				if (tarTable.hasCol(_jlzt)) {
					_jlzt = "JLZT_$";
				}
			}
			// ADD COLS
			int infaTableColSize = tarTable.getCols().size();
			qysj = new InfaCol(_qysj, "", "DATE", "19", "0", "0", "NOTNULL", String.valueOf(infaTableColSize + 1),
					"NOT A KEY");
			sxsj = new InfaCol(_sxsj, "", "DATE", "19", "0", "0", "NULL", String.valueOf(infaTableColSize + 2),
					"NOT A KEY");
			jlzt = new InfaCol(_jlzt, "", "VARCHAR2", "1", "0", "0", "NOTNULL", String.valueOf(infaTableColSize + 3),
					"NOT A KEY");
			crc32Col = new InfaCol("CRC32", "", "varchar2", "32", "0", "0", "NOTNULL",
					String.valueOf(infaTableColSize + 4), "NOT A KEY");

			tarTable.getCols().add(qysj);
			tarTable.getCols().add(sxsj);
			tarTable.getCols().add(jlzt);

		}

		// 源和目标
		source = srcTable.createSource(tableName, srcDBName);
		tarSource = tarTable.createSource(tableName, tarDBName);
		target = tarTable.createTarget(targetName, true);
		((Element) target.selectSingleNode("TARGETFIELD[@NAME='QYSJ']")).addAttribute("KEYTYPE", "PRIMARY KEY")
				.addAttribute("NULLABLE", "NOTNULL");

		// 构造中间组件
		Element srcQua = srcTable.createSourceQualifier("SQ_S" + tableName, sqlFilter);
		Element tarQua = tarTable.createSourceQualifier("SQ_T" + targetName, sqlFilter);

		Element srcExp = srcTable.createExpression("EXP_S", crc32Col, crc32);
		Element tarExp = tarTable.createExpression("EXP_T", crc32Col, crc32);

		Element srcStr = srcTable.createSorter("STR_S", crc32Col);
		Element tarStr = tarTable.createSorter("STR_T", crc32Col);

		Element joiner = srcTable.createJoiner(tarTable, crc32Col);

		Element router = srcTable.createRouter(joiner, source);

		Element upd3Stg = srcTable.createUpdateStrategy("UPD_D_" + targetName, "DD_UPDATE", true, qysj);

		Element delStg = tarTable.createUpdateStrategy("DELETE_" + targetName, "DD_UPDATE", true, qysj);

		Element insertExp = srcTable.createExpression("EXP_INSERT", false);

		insertExp.add(qysj.createExpressionField("sysdate"));
		insertExp.add(sxsj.createExpressionField(""));
		insertExp.add(jlzt.createExpressionField("1"));

		Element deleteExp = srcTable.createExpression("EXP_DELETE", true);

		deleteExp.add(qysj.createExpressionField());
		deleteExp.add(sxsj.createExpressionField("sysdate"));
		deleteExp.add(jlzt.createExpressionField("3"));

		Element update3Exp = srcTable.createExpression("EXP_UPDATE_3", true);
		update3Exp.add(qysj.createExpressionField());
		update3Exp.add(sxsj.createExpressionField("sysdate"));
		update3Exp.add(jlzt.createExpressionField("3"));

		Element update1Exp = srcTable.createExpression("EXP_UPDATE_1", false);
		update1Exp.add(qysj.createExpressionField("sysdate"));
		update1Exp.add(sxsj.createExpressionField(""));
		update1Exp.add(jlzt.createExpressionField("2"));

		mapping = InfaUtil.createMapping(mappingName);

		// Instance
		Element sSrcInst = InfaUtil.createInstance(source, tableName);
		Element sTarInst = InfaUtil.createInstance(tarSource, targetName);

		Element srcQuaInst = InfaUtil.createQualifierInstance("SQ_S" + tableName, tableName);
		Element tarQuaInst = InfaUtil.createQualifierInstance("SQ_T" + targetName, targetName);

		Element srcExpInst = InfaUtil.createTransformInstance(srcExp);
		Element tarExpInst = InfaUtil.createTransformInstance(tarExp);

		Element srcStrInst = InfaUtil.createTransformInstance(srcStr);
		Element tarStrInst = InfaUtil.createTransformInstance(tarStr);

		Element joinerInst = InfaUtil.createTransformInstance(joiner);

		Element routerInst = InfaUtil.createTransformInstance(router);

		Element upd3StgInst = InfaUtil.createTransformInstance(upd3Stg);
		Element delStgInst = InfaUtil.createTransformInstance(delStg);

		Element insertExpInst = InfaUtil.createTransformInstance(insertExp);
		Element update3ExpInst = InfaUtil.createTransformInstance(update3Exp);
		Element update1ExpInst = InfaUtil.createTransformInstance(update1Exp);
		Element deleteExpInst = InfaUtil.createTransformInstance(deleteExp);

		Element tarInsertInst = InfaUtil.createTargetInstance("INSERT_" + targetName, targetName);
		Element tarUpdate3Inst = InfaUtil.createTargetInstance("UPDATE_DEL_" + targetName, targetName);
		Element tarUpdate1Inst = InfaUtil.createTargetInstance("UPDATE_INS_" + targetName, targetName);
		Element tarDeleteInst = InfaUtil.createTargetInstance("DELETE_" + targetName, targetName);

		ArrayList<Element> connectors = new ArrayList<>();
		connectors.addAll(InfaUtil.createConnector(sSrcInst, source, srcQuaInst, srcQua));
		connectors.addAll(InfaUtil.createConnector(srcQuaInst, srcQua, srcExpInst, srcExp));
		connectors.addAll(InfaUtil.createConnector(srcExpInst, srcExp, srcStrInst, srcStr));
		connectors.addAll(InfaUtil.createConnector(srcStrInst, srcStr, joinerInst, joiner, "_SRC"));

		connectors.addAll(InfaUtil.createConnector(sTarInst, tarSource, tarQuaInst, tarQua));
		connectors.addAll(InfaUtil.createConnector(tarQuaInst, tarQua, tarExpInst, tarExp));
		connectors.addAll(InfaUtil.createConnector(tarExpInst, tarExp, tarStrInst, tarStr));
		connectors.addAll(InfaUtil.createConnector(tarStrInst, tarStr, joinerInst, joiner, "_TAR"));

		connectors.addAll(InfaUtil.createConnector(joinerInst, joiner, routerInst, router));

		connectors.addAll(InfaUtil.createConnector(routerInst, router, "_SRC_I", insertExpInst, insertExp));
		connectors.addAll(InfaUtil.createConnector(routerInst, router, "_TAR_U", upd3StgInst, upd3Stg));
		connectors.addAll(InfaUtil.createConnector(routerInst, router, "_SRC_U", update1ExpInst, update1Exp));
		connectors.addAll(InfaUtil.createConnector(routerInst, router, "_TAR_D", delStgInst, delStg));

		connectors.addAll(InfaUtil.createConnector(upd3StgInst, upd3Stg, update3ExpInst, update3Exp));

		connectors.addAll(InfaUtil.createConnector(insertExpInst, insertExp, tarInsertInst, target));
		connectors.addAll(InfaUtil.createConnector(update3ExpInst, update3Exp, tarUpdate3Inst, target));
		connectors.addAll(InfaUtil.createConnector(update1ExpInst, update1Exp, tarUpdate1Inst, target));
		connectors.addAll(InfaUtil.createConnector(delStgInst, delStg, deleteExpInst, deleteExp));
		connectors.addAll(InfaUtil.createConnector(deleteExpInst, deleteExp, tarDeleteInst, target));

		mapping.add(srcQua);// Source Qualifier加到mapping下
		mapping.add(tarQua);// Source Qualifier加到mapping下
		mapping.add(srcExp);// 源EXPTRANS加到mapping下
		mapping.add(tarExp);// 目标EXPTRANS加到mapping下
		mapping.add(srcStr);// 源Sorter加到mapping下
		mapping.add(tarStr);// 目标Sorter加到mapping下
		mapping.add(joiner);// joiner加到mapping下
		mapping.add(router);// router加到mapping下
		mapping.add(insertExp);
		mapping.add(update3Exp);
		mapping.add(update1Exp);
		mapping.add(deleteExp);
		mapping.add(upd3Stg);
		mapping.add(delStg);

		mapping.add(sSrcInst);
		mapping.add(sTarInst);
		mapping.add(srcQuaInst);
		mapping.add(tarQuaInst);
		mapping.add(srcExpInst);
		mapping.add(tarExpInst);
		mapping.add(srcStrInst);
		mapping.add(tarStrInst);
		mapping.add(joinerInst);
		mapping.add(routerInst);
		mapping.add(insertExpInst);
		mapping.add(update3ExpInst);
		mapping.add(update1ExpInst);
		mapping.add(deleteExpInst);
		mapping.add(upd3StgInst);
		mapping.add(delStgInst);
		mapping.add(tarInsertInst);
		mapping.add(tarUpdate3Inst);
		mapping.add(tarUpdate1Inst);
		mapping.add(tarDeleteInst);

		// connector加到mapping下
		for (Iterator<Element> it = connectors.iterator(); it.hasNext();) {
			// System.out.println(it.next().asXML());
			mapping.add(it.next());
		}

		mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
				tarInsertInst.attributeValue("NAME"));
		mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
				tarUpdate3Inst.attributeValue("NAME"));
		mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
				tarUpdate1Inst.attributeValue("NAME"));
		mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
				tarDeleteInst.attributeValue("NAME"));

		// variable
		InfaUtil.createMappingVariables(mapping);

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
		Element sTarInstS = InfaUtil.createSessTransformationInst(sTarInst, owner);
		Element srcQuaInstS = InfaUtil.createSessTransformationInst(srcQuaInst, owner);
		Element tarQuaInstS = InfaUtil.createSessTransformationInst(tarQuaInst, owner);
		Element srcExpInstS = InfaUtil.createSessTransformationInst(srcExpInst, owner);
		Element tarExpInstS = InfaUtil.createSessTransformationInst(tarExpInst, owner);
		Element srcStrInstS = InfaUtil.createSessTransformationInst(srcStrInst, owner);
		Element tarStrInstS = InfaUtil.createSessTransformationInst(tarStrInst, owner);
		Element joinerInstS = InfaUtil.createSessTransformationInst(joinerInst, owner);
		Element routerInstS = InfaUtil.createSessTransformationInst(routerInst, owner);
		Element insertExpInstS = InfaUtil.createSessTransformationInst(insertExpInst, owner);
		Element update3ExpInstS = InfaUtil.createSessTransformationInst(update3ExpInst, owner);
		Element update1ExpInstS = InfaUtil.createSessTransformationInst(update1ExpInst, owner);
		Element deleteExpInstS = InfaUtil.createSessTransformationInst(deleteExpInst, owner);
		Element updStg3InstS = InfaUtil.createSessTransformationInst(upd3StgInst, owner);
		Element delStgInstS = InfaUtil.createSessTransformationInst(delStgInst, owner);
		Element tarInsertInstS = InfaUtil.createSessTransformationInst(tarInsertInst, owner);
		Element tarUpdate3InstS = InfaUtil.createSessTransformationInst(tarUpdate3Inst, owner);
		Element tarUpdate1InstS = InfaUtil.createSessTransformationInst(tarUpdate1Inst, owner);
		Element tarDeleteInstS = InfaUtil.createSessTransformationInst(tarDeleteInst, owner);

		// 源的owner需要修改
		sSrcInstS.addElement("ATTRIBUTE").addAttribute("NAME", "Owner Name").addAttribute("VALUE", owner);

		// CONFIGREFERENCE
		Element configreference = InfaUtil.createConfigreference();

		// SESSIONEXTENSION
		Element sSrcInstSE = InfaUtil.createReaderSessionextension(srcQua.attributeValue("NAME"),
				source.attributeValue("NAME"));
		Element sTarInstSE = InfaUtil.createReaderSessionextension(tarQua.attributeValue("NAME"),
				tarSource.attributeValue("NAME"));
		Element srcQuaInstSE = InfaUtil.createReaderSessionextension(srcQua, "$DBConnectionSRC");
		Element tarQuaInstSE = InfaUtil.createReaderSessionextension(tarQua, "$DBConnectionTAR");

		Element tarInsertInstSE = InfaUtil.createWriterSessionextension(tarInsertInst);
		Element tarUpdate3InstSE = InfaUtil.createWriterSessionextension(tarUpdate3Inst);
		Element tarUpdate1InstSE = InfaUtil.createWriterSessionextension(tarUpdate1Inst);
		Element tarDeleteInstSE = InfaUtil.createWriterSessionextension(tarDeleteInst);

		session.add(sSrcInstS);
		session.add(sTarInstS);
		session.add(srcQuaInstS);
		session.add(tarQuaInstS);
		session.add(srcExpInstS);
		session.add(tarExpInstS);
		session.add(srcStrInstS);
		session.add(tarStrInstS);
		session.add(joinerInstS);
		session.add(routerInstS);
		session.add(insertExpInstS);
		session.add(update3ExpInstS);
		session.add(update1ExpInstS);
		session.add(deleteExpInstS);
		session.add(updStg3InstS);
		session.add(delStgInstS);
		session.add(tarInsertInstS);
		session.add(tarUpdate3InstS);
		session.add(tarUpdate1InstS);
		session.add(tarDeleteInstS);

		session.add(configreference);

		session.add(sSrcInstSE);
		session.add(sTarInstSE);
		session.add(srcQuaInstSE);
		session.add(tarQuaInstSE);
		session.add(tarInsertInstSE);
		session.add(tarUpdate3InstSE);
		session.add(tarUpdate1InstSE);
		session.add(tarDeleteInstSE);

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
		logger.debug("end InfaZipperXML:" + tableName);
	}

}
