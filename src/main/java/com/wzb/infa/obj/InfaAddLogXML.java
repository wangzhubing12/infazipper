package com.wzb.infa.obj;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Element;

import com.wzb.infa.dbutils.InfaUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.DuplicateColumnExceptiion;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.properties.InfaProperty;

public class InfaAddLogXML extends BaseInfaXML implements InfaXML {

    private Logger logger = Logger.getLogger(InfaAddLogXML.class);

    public InfaAddLogXML(String owner, String tableName)
            throws UnsupportedDatatypeException, SQLException, CheckTableExistException, NoPrimaryKeyException {
        super();
        logger.debug("begin InfaAddXML:" + tableName);

        InfaProperty infaProperty = InfaProperty.getInstance();
        String deleteLogName = infaProperty.getProperty("add.delete.log", "ETL_DELETE_LOG").toUpperCase();
        String targetName = getTargetName(owner,tableName);
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

        InfaCol hyid;
        InfaCol hyUpdateDate;
        InfaCol hyUpdateFlag;
        String hy_id;
        String hy_update_date;
        String hy_update_flag;
        InfaCol crc32Col;
        String crc32 = srcTable.getCRC32String();
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

            crc32Col = new InfaCol("CRC32", "", "varchar2", "32", "0", "0", "NOTNULL",
                    String.valueOf(infaTableColSize + 4), "NOT A KEY");

            try {
                tarTable.addCol(hyid);
                tarTable.addCol(hyUpdateDate);
                tarTable.addCol(hyUpdateFlag);
            } catch (DuplicateColumnExceptiion e) {
                logger.fatal(e.getMessage());
            }

        }

        ArrayList<InfaCol> logCols = new ArrayList<>();

        InfaCol logDeleteDate = new InfaCol("DELETE_DATE", "", "DATE", "19", "0", "0", "NOTNULL",
                "1", "NOT A KEY");
        InfaCol logTableName = new InfaCol("TABLE_NAME", "", "VARCHAR2", "30", "0", "0", "NOTNULL",
                "2", "NOT A KEY");
        InfaCol logPK = new InfaCol("PRIMARY_KEY", "", "VARCHAR2", "128", "0", "0", "NOTNULL",
                "3", "NOT A KEY");
        InfaCol logContent = new InfaCol("CONTENT", "", "CLOB", tarTable.getColsLenth(), "0", "0", "NOTNULL",
                "4", "NOT A KEY");
        logCols.add(logDeleteDate);
        logCols.add(logTableName);
        logCols.add(logPK);
        logCols.add(logContent);

        InfaTable logTable = new InfaTable(owner, deleteLogName, logCols);

        // 源和目标
        source = srcTable.createSource(tableName, srcDBName);
        tarSource = tarTable.createSource(tableName, tarDBName);
        target = tarTable.createTarget(targetName, false);
        ((Element) target.selectSingleNode("TARGETFIELD[@NAME='" + hy_id + "']")).addAttribute("KEYTYPE", "PRIMARY KEY")
                .addAttribute("NULLABLE", "NOTNULL");

        Element logTar = logTable.createTarget(deleteLogName, false);
        // 构造中间组件
        Element srcQua = srcTable.createSourceQualifier("SQ_S" + tableName, sqlFilter);
        Element tarQua = tarTable.createSourceQualifier("SQ_T" + targetName, sqlFilter);

        Element srcExp = srcTable.createExpression("EXP_S", crc32Col, crc32);
        Element tarExp = tarTable.createExpression("EXP_T", crc32Col, crc32);

        Element srcStr = srcTable.createSorter("STR_S", crc32Col);
        Element tarStr = tarTable.createSorter("STR_T", crc32Col);

        Element joiner = srcTable.createJoiner(tarTable, crc32Col);

        Element router = srcTable.createRouter(joiner, source);

        Element updStg = srcTable.createUpdateStrategy("UPD_" + tableName, "DD_UPDATE", false, null);
        Element delStg = tarTable.createUpdateStrategy(hyid);

        Element insertExp = srcTable.createExpression("EXP_INSERT", false);

        insertExp.add(hyid.createExpressionField(pkString));
        insertExp.add(hyUpdateDate.createExpressionField("sysdate"));
        insertExp.add(hyUpdateFlag.createExpressionField("1"));

        Element updateExp = srcTable.createExpression("EXP_UPDATE", false);
        updateExp.add(hyid.createExpressionField(pkString));
        updateExp.add(hyUpdateDate.createExpressionField("sysdate"));
        updateExp.add(hyUpdateFlag.createExpressionField("2"));

        Element logExp = tarTable.createExpression("EXP_LOG", false);
        {
            logExp.add(logDeleteDate.createExpressionField("SYSDATE"));
            logExp.add(logTableName.createExpressionField("$PM" + tarTable.getName() + "@TableName"));
            logExp.add(logPK.createExpressionField(pkString));
            logExp.add(logContent.createExpressionField(crc32.substring(6, crc32.length() - 1)));
        }
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

        Element updStgInst = InfaUtil.createTransformInstance(updStg);
        Element delStgInst = InfaUtil.createTransformInstance(delStg);

        Element insertExpInst = InfaUtil.createTransformInstance(insertExp);
        Element updateExpInst = InfaUtil.createTransformInstance(updateExp);

        Element logExpInst = InfaUtil.createTransformInstance(logExp);

        Element tarInsertInst = InfaUtil.createTargetInstance("INSERT_" + targetName, targetName);
        Element tarUpdateInst = InfaUtil.createTargetInstance("UPDATE_" + targetName, targetName);
        Element tarDeleteInst = InfaUtil.createTargetInstance("DELETE_" + targetName, targetName);

        Element logTarInst = InfaUtil.createTargetInstance(deleteLogName, deleteLogName);

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
        connectors.addAll(InfaUtil.createConnector(routerInst, router, "_SRC_U", updStgInst, updStg));
        connectors.addAll(InfaUtil.createConnector(routerInst, router, "_TAR_D", delStgInst, delStg));
        connectors.addAll(InfaUtil.createConnector(routerInst, router, "_TAR_D", logExpInst, logExp));

        connectors.addAll(InfaUtil.createConnector(updStgInst, updStg, updateExpInst, updateExp));

        connectors.addAll(InfaUtil.createConnector(insertExpInst, insertExp, tarInsertInst, target));
        connectors.addAll(InfaUtil.createConnector(updateExpInst, updateExp, tarUpdateInst, target));
        connectors.addAll(InfaUtil.createConnector(delStgInst, delStg, tarDeleteInst, target));
        connectors.addAll(InfaUtil.createConnector(logExpInst, logExp, logTarInst, logTar));

        mapping.add(srcQua);// Source Qualifier加到mapping下
        mapping.add(tarQua);// Source Qualifier加到mapping下
        mapping.add(srcExp);// 源EXPTRANS加到mapping下
        mapping.add(tarExp);// 目标EXPTRANS加到mapping下
        mapping.add(srcStr);// 源Sorter加到mapping下
        mapping.add(tarStr);// 目标Sorter加到mapping下
        mapping.add(joiner);// joiner加到mapping下
        mapping.add(router);// router加到mapping下
        mapping.add(insertExp);
        mapping.add(updateExp);
        mapping.add(logExp);
        mapping.add(updStg);
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
        mapping.add(updateExpInst);
        mapping.add(logExpInst);
        mapping.add(updStgInst);
        mapping.add(delStgInst);
        mapping.add(tarInsertInst);
        mapping.add(tarUpdateInst);
        mapping.add(tarDeleteInst);
        mapping.add(logTarInst);

        // connector加到mapping下
        for (Iterator<Element> it = connectors.iterator(); it.hasNext();) {
            // System.out.println(it.next().asXML());
            mapping.add(it.next());
        }

        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                tarInsertInst.attributeValue("NAME"));
        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                tarUpdateInst.attributeValue("NAME"));
        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                tarDeleteInst.attributeValue("NAME"));
        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                logTarInst.attributeValue("NAME"));
        
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
        Element updateExpInstS = InfaUtil.createSessTransformationInst(updateExpInst, owner);
        Element logExpInstS = InfaUtil.createSessTransformationInst(logExpInst, owner);
        Element updStgInstS = InfaUtil.createSessTransformationInst(updStgInst, owner);
        Element delStgInstS = InfaUtil.createSessTransformationInst(delStgInst, owner);
        Element tarInsertInstS = InfaUtil.createSessTransformationInst(tarInsertInst, owner);
        Element tarUpdateInstS = InfaUtil.createSessTransformationInst(tarUpdateInst, owner);
        Element tarDeleteInstS = InfaUtil.createSessTransformationInst(tarDeleteInst, owner);
        Element logTarInstS = InfaUtil.createSessTransformationInst(logTarInst, owner);       
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
        Element tarUpdateInstSE = InfaUtil.createWriterSessionextension(tarUpdateInst);
        Element tarDeleteInstSE = InfaUtil.createWriterSessionextension(tarDeleteInst);
        Element logTarInstSE = InfaUtil.createWriterSessionextension(logTarInst);

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
        session.add(updateExpInstS);
        session.add(logExpInstS);
        session.add(updStgInstS);
        session.add(delStgInstS);
        session.add(tarInsertInstS);
        session.add(tarUpdateInstS);
        session.add(tarDeleteInstS);
        session.add(logTarInstS);

        session.add(configreference);

        session.add(sSrcInstSE);
        session.add(sTarInstSE);
        session.add(srcQuaInstSE);
        session.add(tarQuaInstSE);
        session.add(tarInsertInstSE);
        session.add(tarUpdateInstSE);
        session.add(tarDeleteInstSE);
        session.add(logTarInstSE);
        
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
        logger.debug("end InfaAddXML:" + tableName);
    }

}
