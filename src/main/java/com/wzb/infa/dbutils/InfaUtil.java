package com.wzb.infa.dbutils;

import java.util.ArrayList;
import java.util.Iterator;

import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.wzb.infa.properties.InfaProperty;

/**
 * * 创建各种INFA对象组件类型 1
 */
public class InfaUtil {

    public static final InfaProperty infaProperty = InfaProperty.getInstance();

    public static Element createInstance(Element source, String instanceName) {
        Element sourceInstance = DocumentHelper.createElement("INSTANCE")
                .addAttribute("DBDNAME", source.attributeValue("DBDNAME")).addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", instanceName).addAttribute("TRANSFORMATION_NAME", source.attributeValue("NAME"))
                .addAttribute("TRANSFORMATION_TYPE", "Source Definition").addAttribute("TYPE", "SOURCE");
        return sourceInstance;
    }

    public static Element createQualifierInstance(String sourceQualifierName, String sourceName) {
        Element qualifierInstance = DocumentHelper.createElement("INSTANCE").addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", sourceQualifierName).addAttribute("REUSABLE", "NO")
                .addAttribute("TRANSFORMATION_NAME", sourceQualifierName)
                .addAttribute("TRANSFORMATION_TYPE", "Source Qualifier").addAttribute("TYPE", "TRANSFORMATION");
        qualifierInstance.addElement("ASSOCIATED_SOURCE_INSTANCE").addAttribute("NAME", sourceName);
        return qualifierInstance;
    }

    public static Element createTransformInstance(Element transform) {
        Element transformInstance = DocumentHelper.createElement("INSTANCE").addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", transform.attributeValue("NAME")).addAttribute("REUSABLE", "NO")
                .addAttribute("TRANSFORMATION_NAME", transform.attributeValue("NAME"))
                .addAttribute("TRANSFORMATION_TYPE", transform.attributeValue("TYPE"))
                .addAttribute("TYPE", "TRANSFORMATION");
        return transformInstance;
    }

    public static Element createTargetInstance(String instanceName, String targetName) {
        Element targetInstance = DocumentHelper.createElement("INSTANCE").addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", instanceName).addAttribute("TRANSFORMATION_NAME", targetName)
                .addAttribute("TRANSFORMATION_TYPE", "Target Definition").addAttribute("TYPE", "TARGET");
        return targetInstance;
    }

    public static ArrayList<Element> createConnector(Element fromInstance, Element fromElement, Element toInstance,
            Element toElement) {
        String fromInstanceName = fromInstance.attributeValue("NAME");
        String fromInstancetype = fromInstance.attributeValue("TRANSFORMATION_TYPE");
        String toInstanceName = toInstance.attributeValue("NAME");
        String toInstancetype = toInstance.attributeValue("TRANSFORMATION_TYPE");
        String fromFieldType = "SOURCE".equals(fromElement.getName()) ? "SOURCEFIELD" : "TRANSFORMFIELD";
        String toFieldType = "TARGET".equals(toElement.getName()) ? "TARGETFIELD" : "TRANSFORMFIELD";
        ArrayList<Element> connectors = new ArrayList<>();

        Element fromField;
        Element toField;

        for (@SuppressWarnings("unchecked") Iterator<Element> it = fromElement.elementIterator(fromFieldType); it.hasNext();) {

            fromField = it.next();
            toField = (Element) toElement.selectSingleNode(toFieldType + "[@NAME='" + fromField.attributeValue("NAME") + "']");
            //to没有对应的字段名称则跳过
            if (toField == null) {
                continue;
            }
            boolean linkfrom = false;// 判断前from的字段是否可拉出一条线
            boolean linkto = false;// 判断前to的字段是否可拉入一条线
            // 源为SOURCE时,或者存在OUTPUT的字段则可以连接
            if ("SOURCE".equals(fromElement.getName()) || fromField.attributeValue("PORTTYPE").contains("OUTPUT")) {
                linkfrom = true;
            }
            // 目标为TARGET时,或者存在INPUT的字段则可以连接
            if ("TARGET".equals(toElement.getName()) || toField.attributeValue("PORTTYPE").contains("INPUT")) {
                linkto = true;
            }

            if (linkfrom && linkto) {
                connectors.add(createConnector(fromField.attributeValue("NAME"), fromInstanceName, fromInstancetype,
                        fromField.attributeValue("NAME"), toInstanceName, toInstancetype));
            }
        }

        return connectors;
    }

    public static ArrayList<Element> createConnector(Element fromInstance, Element fromElement, Element joinerInstance,
            Element joiner, String joinerFieldPrefix) {
        String fromInstanceName = fromInstance.attributeValue("NAME");
        String fromInstancetype = fromInstance.attributeValue("TRANSFORMATION_TYPE");
        String toInstanceName = joinerInstance.attributeValue("NAME");
        String toInstancetype = joinerInstance.attributeValue("TRANSFORMATION_TYPE");
        ArrayList<Element> connectors = new ArrayList<>();

        Element from;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = fromElement.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            from = it.next();
            connectors.add(createConnector(from.attributeValue("NAME"), fromInstanceName, fromInstancetype,
                    from.attributeValue("NAME") + joinerFieldPrefix, toInstanceName, toInstancetype));
        }
        return connectors;
    }

    public static ArrayList<Element> createConnector(Element routerInstance, Element router, String groupPrefix,
            Element toInstance, Element toElement) {
        String fromInstanceName = routerInstance.attributeValue("NAME");
        String fromInstancetype = routerInstance.attributeValue("TRANSFORMATION_TYPE");
        String toInstanceName = toInstance.attributeValue("NAME");
        String toInstancetype = toInstance.attributeValue("TRANSFORMATION_TYPE");
        ArrayList<Element> connectors = new ArrayList<>();
        Element to;
        Element connector;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = toElement.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            to = it.next();
            if (to.attributeValue("PORTTYPE").contains("INPUT")) {
                // 源名称=to字段名称+groupPrefix 目标名称to字段名称
                // 插入连接 无需修改 COL_SRC_I-->COL
                // 删除连接 无需修改 COL_TAR_D-->COL
                // 更新连接 1、插入：COL_SRC_U-->COL
                // 更新连接 2、删除：COL_TAR_U-->COL
                connector = createConnector(to.attributeValue("NAME") + groupPrefix, fromInstanceName, fromInstancetype,
                        to.attributeValue("NAME"), toInstanceName, toInstancetype);

                connectors.add(connector);
            }
        }
        return connectors;
    }

    private static Element createConnector(String fromField, String fromInstance, String fromInstancetype,
            String toField, String toInstance, String toInstancetype) {
        Element connector = DocumentHelper.createElement("CONNECTOR").addAttribute("FROMFIELD", fromField)
                .addAttribute("FROMINSTANCE", fromInstance).addAttribute("FROMINSTANCETYPE", fromInstancetype)
                .addAttribute("TOFIELD", toField).addAttribute("TOINSTANCE", toInstance)
                .addAttribute("TOINSTANCETYPE", toInstancetype);

        return connector;
    }

    public static Element createWorkflowScheduler(String schedulerName) {
        Element scheduler = DocumentHelper.createElement("SCHEDULER").addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", schedulerName).addAttribute("REUSABLE", "NO").addAttribute("VERSIONNUMBER", "1");
        scheduler.addElement("SCHEDULEINFO").addAttribute("SCHEDULETYPE", "ONDEMAND");
        return scheduler;
    }

    public static Element createSessTransformationInst(Element instance, String owner) {
        Element sessTransformationInst = DocumentHelper.createElement("SESSTRANSFORMATIONINST");
        String transformationType = instance.attributeValue("TRANSFORMATION_TYPE");
        String transformationName = instance.attributeValue("TRANSFORMATION_NAME");
        String sInstanceName = instance.attributeValue("NAME");

        sessTransformationInst.addAttribute("ISREPARTITIONPOINT", "YES").addAttribute("PARTITIONTYPE", "PASS THROUGH")
                .addAttribute("PIPELINE", "0").addAttribute("SINSTANCENAME", sInstanceName).addAttribute("STAGE", "0")
                .addAttribute("TRANSFORMATIONNAME", transformationName)
                .addAttribute("TRANSFORMATIONTYPE", transformationType);

        if ("Source Definition".equals(transformationType)) {
            sessTransformationInst.addAttribute("ISREPARTITIONPOINT", "NO");
            sessTransformationInst.remove(sessTransformationInst.attribute("PARTITIONTYPE"));
        }

        return sessTransformationInst;
    }

    public static Element createReaderSessionextension(String sqInstanceName, String sInstanceName) {
        Element sessionextension = DocumentHelper.createElement("SESSIONEXTENSION")
                .addAttribute("DSQINSTNAME", sqInstanceName).addAttribute("DSQINSTTYPE", "Source Qualifier")
                .addAttribute("NAME", "Relational Reader").addAttribute("SINSTANCENAME", sInstanceName)
                .addAttribute("SUBTYPE", "Relational Reader").addAttribute("TRANSFORMATIONTYPE", "Source Definition")
                .addAttribute("TYPE", "READER");
        return sessionextension;
    }

    public static Element createReaderSessionextension(Element instance, String varriable) {
        Element sessionextension = DocumentHelper.createElement("SESSIONEXTENSION")
                .addAttribute("NAME", "Relational Reader")
                .addAttribute("SINSTANCENAME", instance.attributeValue("NAME"))
                .addAttribute("SUBTYPE", "Relational Reader").addAttribute("TRANSFORMATIONTYPE", "Source Qualifier")
                .addAttribute("TYPE", "READER");
        sessionextension.addElement("CONNECTIONREFERENCE").addAttribute("CNXREFNAME", "DB Connection")
                .addAttribute("CONNECTIONNAME", "").addAttribute("CONNECTIONNUMBER", "1")
                .addAttribute("CONNECTIONSUBTYPE", "").addAttribute("CONNECTIONTYPE", "Relational")
                .addAttribute("VARIABLE", varriable);

        return sessionextension;
    }

    public static Element createWriterSessionextension(Element instance) {
        Element sessionextension = DocumentHelper.createElement("SESSIONEXTENSION")
                .addAttribute("NAME", "Relational Writer")
                .addAttribute("SINSTANCENAME", instance.attributeValue("NAME"))
                .addAttribute("SUBTYPE", "Relational Writer").addAttribute("TRANSFORMATIONTYPE", "Target Definition")
                .addAttribute("TYPE", "Writer");
        sessionextension.addElement("CONNECTIONREFERENCE").addAttribute("CNXREFNAME", "DB Connection")
                .addAttribute("CONNECTIONNAME", "").addAttribute("CONNECTIONNUMBER", "1")
                .addAttribute("CONNECTIONSUBTYPE", "").addAttribute("CONNECTIONTYPE", "Relational")
                .addAttribute("VARIABLE", "$DBConnectionTAR");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Target load type").addAttribute("VALUE",
                "Normal");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Insert").addAttribute("VALUE", "YES");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Update as Update").addAttribute("VALUE", "YES");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Update as Insert").addAttribute("VALUE", "NO");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Update else Insert").addAttribute("VALUE", "NO");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Delete").addAttribute("VALUE", "YES");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Truncate target table option")
                .addAttribute("VALUE", "NO");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Reject file directory").addAttribute("VALUE",
                "$PMBadFileDir/");
        sessionextension.addElement("ATTRIBUTE").addAttribute("NAME", "Reject filename").addAttribute("VALUE",
                instance.attributeValue("NAME") + ".bad");
        return sessionextension;
    }

    public static Element createMapping(String mappingName) {
        Element mapping = DocumentHelper.createElement("MAPPING").addAttribute("DESCRIPTION", "")
                .addAttribute("ISVALID", "YES").addAttribute("NAME", mappingName).addAttribute("OBJECTVERSION", "1")
                .addAttribute("VERSIONNUMBER", "1");
        return mapping;

    }

    public static Element createWorkflow(Element mapping, String workflowCOmments) {
        Element workflow = DocumentHelper.createElement("WORKFLOW").addAttribute("DESCRIPTION", workflowCOmments)
                .addAttribute("ISENABLED", "YES").addAttribute("ISRUNNABLESERVICE", "NO")
                .addAttribute("ISSERVICE", "NO").addAttribute("ISVALID", "YES")
                .addAttribute("NAME", "WF_" + mapping.attributeValue("NAME")).addAttribute("REUSABLE_SCHEDULER", "NO")
                .addAttribute("SCHEDULERNAME", "SCHEDULERPLAN")
                .addAttribute("SERVERNAME", infaProperty.getProperty("service.name", "infa_is"))
                .addAttribute("SERVER_DOMAINNAME", infaProperty.getProperty("server.domainname", "GDDW_GRID_DM"))
                .addAttribute("SUSPEND_ON_ERROR", "NO").addAttribute("TASKS_MUST_RUN_ON_SERVER", "NO")
                .addAttribute("VERSIONNUMBER", "1");
        return workflow;
    }

    public static Element createStartTarsk() {
        Element startTarsk = DocumentHelper.createElement("TASK").addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", "start").addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Start")
                .addAttribute("VERSIONNUMBER", "1");
        return startTarsk;
    }

    public static Element createSession(Element mapping) {
        Element session = DocumentHelper.createElement("SESSION").addAttribute("DESCRIPTION", "")
                .addAttribute("ISVALID", "YES").addAttribute("MAPPINGNAME", mapping.attributeValue("NAME"))
                .addAttribute("NAME", infaProperty.getProperty("session.prefix", "S_") + mapping.attributeValue("NAME"))
                .addAttribute("REUSABLE", "NO").addAttribute("SORTORDER", "Binary").addAttribute("VERSIONNUMBER", "1");
        return session;
    }

    public static void createSessionAttribute(Element session) {
        session.addElement("ATTRIBUTE").addAttribute("NAME", "General Options").addAttribute("VALUE", "");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Write Backward Compatible Session Log File")
                .addAttribute("VALUE", "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Session Log File Name").addAttribute("VALUE",
                session.attributeValue("NAME") + ".log");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Session Log File directory").addAttribute("VALUE",
                "$PMSessionLogDir/");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Parameter Filename").addAttribute("VALUE", "");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Enable Test Load").addAttribute("VALUE", "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "$Source connection value").addAttribute("VALUE", "");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "$Target connection value").addAttribute("VALUE", "");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Treat source rows as").addAttribute("VALUE",
                "Data driven");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Commit Type").addAttribute("VALUE", "Target");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Commit Interval").addAttribute("VALUE", "10000");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Commit On End Of File").addAttribute("VALUE", "YES");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Rollback Transactions on Errors").addAttribute("VALUE",
                "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Recovery Strategy").addAttribute("VALUE",
                "Fail task and continue workflow");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Java Classpath").addAttribute("VALUE", "");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Performance").addAttribute("VALUE", "");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "DTM buffer size").addAttribute("VALUE", "auto");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Collect performance data").addAttribute("VALUE", "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Write performance data to repository")
                .addAttribute("VALUE", "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Incremental Aggregation").addAttribute("VALUE", "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Session retry on deadlock").addAttribute("VALUE", "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Pushdown Optimization").addAttribute("VALUE", "None");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Allow Temporary View for Pushdown").addAttribute("VALUE",
                "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Allow Temporary Sequence for Pushdown")
                .addAttribute("VALUE", "NO");
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Allow Pushdown for User Incompatible Connections")
                .addAttribute("VALUE", "NO");

    }

    public static Element createStartTarskInstance() {
        Element startTarskInstance = DocumentHelper.createElement("TASKINSTANCE").addAttribute("DESCRIPTION", "")
                .addAttribute("ISENABLED", "YES").addAttribute("NAME", "start").addAttribute("REUSABLE", "NO")
                .addAttribute("TASKNAME", "start").addAttribute("TASKTYPE", "Start");
        return startTarskInstance;
    }

    public static Element createSessionInstance(Element session) {
        Element sessionInstance = DocumentHelper.createElement("TASKINSTANCE").addAttribute("DESCRIPTION", "")
                .addAttribute("FAIL_PARENT_IF_INSTANCE_DID_NOT_RUN", "NO")
                .addAttribute("FAIL_PARENT_IF_INSTANCE_FAILS", "YES").addAttribute("ISENABLED", "YES")
                .addAttribute("NAME", session.attributeValue("NAME")).addAttribute("REUSABLE", "NO")
                .addAttribute("TASKNAME", session.attributeValue("NAME")).addAttribute("TASKTYPE", "Session")
                .addAttribute("TREAT_INPUTLINK_AS_AND", "YES");
        return sessionInstance;
    }

    public static Element createWorkflowLink(String fromTaskName, String toTaskName) {
        Element workflowLink = DocumentHelper.createElement("WORKFLOWLINK").addAttribute("CONDITION", "")
                .addAttribute("FROMTASK", fromTaskName).addAttribute("TOTASK", toTaskName);
        return workflowLink;
    }

    public static void createWorkflowVariableAndAttribute(Element workflow, Element session) {
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "date/time").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "The time this task started").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$start.StartTime")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "date/time").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "The time this task completed").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$start.EndTime")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Status of this task  execution").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$start.Status")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Status of the previous task that is not disabled")
                .addAttribute("ISNULL", "NO").addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$start.PrevTaskStatus").addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Error code for this task s execution").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$start.ErrorCode")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "string").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Error message for this task s execution").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$start.ErrorMsg")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "date/time").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "The time this task started").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".StartTime")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "date/time").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "The time this task completed").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".EndTime")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Status of this task&apos;s execution").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".Status")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Status of the previous task that is not disabled")
                .addAttribute("ISNULL", "NO").addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".PrevTaskStatus")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Error code for this task&apos;s execution").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".ErrorCode")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "string").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Error message for this task&apos;s execution")
                .addAttribute("ISNULL", "NO").addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".ErrorMsg")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Rows successfully read").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".SrcSuccessRows")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Rows failed to read").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".SrcFailedRows")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Rows successfully loaded").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".TgtSuccessRows")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Rows failed to load").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".TgtFailedRows")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "Total number of transformation errors").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".TotalTransErrors")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "First error code").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".FirstErrorCode")
                .addAttribute("USERDEFINED", "NO");
        workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "string").addAttribute("DEFAULTVALUE", "")
                .addAttribute("DESCRIPTION ", "First error message").addAttribute("ISNULL", "NO")
                .addAttribute("ISPERSISTENT", "NO")
                .addAttribute("NAME", "$" + session.attributeValue("NAME") + ".FirstErrorMsg")
                .addAttribute("USERDEFINED", "NO");
        // ATTRIBUTE
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Parameter Filename").addAttribute("VALUE",
                infaProperty.getProperty("workflow.paramter.filename", ""));
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Write Backward Compatible Workflow Log File")
                .addAttribute("VALUE", "NO");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Workflow Log File Name").addAttribute("VALUE",
                session.attributeValue("NAME") + ".log");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Workflow Log File Directory").addAttribute("VALUE",
                "$PMWorkflowLogDir/");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Save Workflow log by").addAttribute("VALUE", "By runs");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Save workflow log for these runs").addAttribute("VALUE",
                "0");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Service Name").addAttribute("VALUE", "");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Service Timeout").addAttribute("VALUE", "0");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Is Service Visible").addAttribute("VALUE", "NO");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Is Service Protected").addAttribute("VALUE", "NO");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Enable HA recovery").addAttribute("VALUE", "NO");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Automatically recover terminated tasks")
                .addAttribute("VALUE", "NO");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Service Level Name").addAttribute("VALUE", "Default");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Allow concurrent run with unique run instance name")
                .addAttribute("VALUE", "NO");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Allow concurrent run with same run instance name")
                .addAttribute("VALUE", "NO");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Maximum number of concurrent runs").addAttribute("VALUE",
                "0");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Assigned Web Services Hubs").addAttribute("VALUE", "");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Maximum number of concurrent runs per Hub")
                .addAttribute("VALUE", "1000");
        workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Expected Service Time").addAttribute("VALUE", "1");

    }

    public static Element createConfigreference() {
        Element configreference = DocumentHelper.createElement("CONFIGREFERENCE")
                .addAttribute("REFOBJECTNAME", "default_session_config").addAttribute("TYPE", "Session config");
        return configreference;
    }

    public static Element createMappingVariables(Element mapping) {
        String mappingVariables = infaProperty.getProperty("mapping.variable", "");
        if (!"".equals(mappingVariables) && mappingVariables != null) {
            String[] variablesSplit = mappingVariables.split(",");
            for (int i = 0; i < variablesSplit.length; i = i + 3) {
                mapping.addElement("MAPPINGVARIABLE").addAttribute("DATATYPE", variablesSplit[i + 1])
                        .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                        .addAttribute("ISEXPRESSIONVARIABLE", "NO").addAttribute("ISPARAM", "YES")
                        .addAttribute("NAME", variablesSplit[i]).addAttribute("PRECISION", variablesSplit[i + 2])
                        .addAttribute("SCALE", "0").addAttribute("USERDEFINED", "YES");
            }
        }
        return mapping;
    }

}
