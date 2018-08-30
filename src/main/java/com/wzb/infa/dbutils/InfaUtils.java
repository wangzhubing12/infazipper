package com.wzb.infa.dbutils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;
import com.wzb.infa.properties.InfaProperty;

/**
 * *
 * 创建各种INFA对象组件类型如Source Qualifier，会创建对应的字段 创建各种内置的对象如TRANSFORMATION类型时，不含对应字段
 *1
 */
public class InfaUtils {

    private Document doc = null;
    private XMLWriter xmlWriter = null;
    private final Logger log = Logger.getLogger(InfaUtils.class);
    private final InfaProperty infaProperty = InfaProperty.getInstance();
    public DbUtil dbUtil = DbUtil.getInstance();
    private String owner;
    private int swtNamePostfix = 1; // EXPRESSION等组件后缀序列
    private int instanceNumber = 1; // mapping中instance个数顺序，放在description里面，建立sessionInstance时可以对应设置参数
    private final static HashMap<String, String> dataTypeO2I = new HashMap<String, String>() {
        /**
         * 数据类型对应，将oracle的数据类型转对应成INFA的数据类型 此处写的不全，新的类型需要不断添加进来
         */
        private static final long serialVersionUID = 1L;

        {
            put("blob", "binary");
            put("clob", "string");
            put("date", "date/time");
            put("long", "text");
            put("long row", "binary");
            put("nchar", "nstring");
            put("char", "string");
            put("nclob", "ntext");
            put("number", "double");
            put("number(p,s)", "decimal");
            put("nvarchar2", "nstring");
            put("raw", "binary");
            put("timestamp", "date/time");
            put("timestamp(0)", "date/time");
            put("timestamp(3)", "date/time");
            put("timestamp(6)", "date/time");
            put("timestamp(6) with time zone", "date/time");
            put("timestamp(7)", "date/time");
            put("timestamp(9)", "date/time");
            put("timestamp(9) with time zone", "date/time");
            put("varchar", "string");
            put("varchar2", "string");
            put("xmltype", "text");
        }
    };


    // 这个方法不导入字段备注信息，同时分开导出主键信息，效率较高
    private Element createSource(String owner, String table)
            throws NoPrimaryKeyException, UnsupportedDatatypeException, SQLException {
        ResultSet rsCols;

        Element el = DocumentHelper.createElement("SOURCE").addAttribute("BUSINESSNAME", "")
                .addAttribute("DATABASETYPE", "Oracle")
                .addAttribute("DBDNAME", infaProperty.getProperty("source.dbname", owner))
                .addAttribute("DESCRIPTION", dbUtil.getTabComments(owner, table)).addAttribute("NAME", table)
                .addAttribute("OBJECTVERSION", "1").addAttribute("OWNERNAME", owner).addAttribute("VERSIONNUMBER", "1");
        rsCols = dbUtil.getTabCols(owner, table);
        if (!dbUtil.isTabExist(owner, table)) {
            throw new SQLException("no table found" + owner + ":" + table);
        }

        String dataType;
        String dataLength ;// 字符类型的数据长度
        String dataPhysicalLength = "0";// 字符类型的数据长度
        String dataPrecision;// 数据、时间类型的数据长度
        String dataScale;// 数据、时间类型的数据精度
        String dataPhysicalOffset = "0";// 所有类型的物理偏移
        String dataOffset = "0";// 数据、时间类型的物理偏移
        while (rsCols.next()) {
            dataType = rsCols.getString("DATATYPE");
            if (dataType.contains("timestamp")) {
                dataType = "timestamp";
                
                dataPhysicalLength = "29";
                dataPrecision = "29";
                dataScale = "9";
                dataPhysicalOffset = String
                        .valueOf(Integer.parseInt(dataPhysicalOffset) + Integer.parseInt(dataPhysicalLength));
                dataOffset = String.valueOf(Integer.parseInt(dataOffset) + 29);
            } else if (dataType.equals("date")) {
                
                dataPhysicalLength = "19";
                dataPrecision = "19";
                dataScale = "0";
                dataPhysicalOffset = String
                        .valueOf(Integer.parseInt(dataPhysicalOffset) + Integer.parseInt(dataPhysicalLength));
                dataOffset = String.valueOf(Integer.parseInt(dataOffset) + 19);
            } else if (dataType.contains("number") || dataType.contains("float") || dataType.contains("double")) {
                dataType = "number(p,s)";
                
                dataPhysicalLength = rsCols.getString("PRECISION");
                if ("0".equals(dataPhysicalLength)) {
                    dataPhysicalLength = "22";
                }
                dataPrecision = dataPhysicalLength;
                dataScale = rsCols.getString("SCALE");
                dataPhysicalOffset = String
                        .valueOf(Integer.parseInt(dataPhysicalOffset) + Integer.parseInt(dataPhysicalLength));
                dataOffset = String.valueOf(Integer.parseInt(dataOffset) + Integer.parseInt(dataPrecision));
            } else if (dataType.contains("char")) {
                dataLength = rsCols.getString("LENGTH");
                dataPhysicalLength = dataLength;
                dataPrecision = "0";
                dataScale = "0";
                dataPhysicalOffset = String
                        .valueOf(Integer.parseInt(dataPhysicalOffset) + Integer.parseInt(dataPhysicalLength));
            } else if (!dataTypeO2I.containsKey(dataType)) {
                throw new UnsupportedDatatypeException("Unsupported Data type" + owner + "." + table + "."
                        + rsCols.getString("NAME") + ":" + rsCols.getString("DATATYPE"));
            } else {
                dataLength = rsCols.getString("LENGTH");
                dataPhysicalLength = dataLength;
                dataPrecision = "0";
                dataScale = "0";
                dataPhysicalOffset = String
                        .valueOf(Integer.parseInt(dataPhysicalOffset) + Integer.parseInt(dataPhysicalLength));
            }

            el.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", dataType)
                    .addAttribute("DESCRIPTION", "").addAttribute("FIELDNUMBER", rsCols.getString("FIELDNUMBER"))
                    .addAttribute("FIELDPROPERTY", "0").addAttribute("FIELDTYPE", "ELEMITEM")
                    .addAttribute("HIDDEN", "NO").addAttribute("KEYTYPE", "NOT A KEY")
                    .addAttribute("LENGTH", dataPrecision).addAttribute("LEVEL", "0")
                    .addAttribute("NAME", rsCols.getString("NAME"))
                    .addAttribute("NULLABLE", rsCols.getString("NULLABLE")).addAttribute("OCCURS", "0")
                    .addAttribute("OFFSET",
                            String.valueOf(Integer.parseInt(dataOffset) - Integer.parseInt(dataPrecision)))
                    .addAttribute("PHYSICALLENGTH", dataPhysicalLength)
                    .addAttribute("PHYSICALOFFSET",
                            String.valueOf(Integer.parseInt(dataPhysicalOffset) - Integer.parseInt(dataPhysicalLength)))
                    .addAttribute("PICTURETEXT", "").addAttribute("PRECISION", dataPhysicalLength)
                    .addAttribute("SCALE", dataScale).addAttribute("USAGE_FLAGS", "");
        }
        try {
                rsCols.close();
        } catch (SQLException e) {
            //e.printStackTrace();
        }
        return el;
    }

    // 这个方法不导入字段备注信息，同时分开导出主键信息，效率较高
    private Element createSourceWithPrimaryKey(String owner, String table)
            throws NoPrimaryKeyException, UnsupportedDatatypeException, SQLException {
        ResultSet rsPrimaryCols = null;

        Element el = createSource(owner, table);
        try {
            rsPrimaryCols = dbUtil.getPrimaryCols(owner, table);

            boolean hasPrimaryKey = false;
            while (rsPrimaryCols.next()) {
                hasPrimaryKey = true;
                ((Element) el.selectSingleNode("SOURCEFIELD[@NAME='" + rsPrimaryCols.getString(1) + "']"))
                        .addAttribute("KEYTYPE", "PRIMARY KEY").addAttribute("NULLABLE", "NOTNULL");
            }

            if (!hasPrimaryKey) {
                throw new NoPrimaryKeyException(" no PrimaryKey!");
            }
            return el;
        } catch (SQLException e) {
            throw new NoPrimaryKeyException(e.getMessage());
        } finally {
            try {
                if (rsPrimaryCols != null) {
                    rsPrimaryCols.close();
                }
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
    }

    // 这个方法不导入字段备注信息，同时分开导出主键信息，效率较高
    private String getPrimarykey(String owner, String table)
            throws NoPrimaryKeyException, UnsupportedDatatypeException, SQLException {
        ResultSet rsPrimaryCols = null;
        String pk = "";
        try {
            rsPrimaryCols = dbUtil.getPrimaryCols(owner, table);

            boolean hasPrimaryKey = false;
            while (rsPrimaryCols.next()) {
                hasPrimaryKey = true;
                pk = pk + rsPrimaryCols.getString(1) + "||'_'||";
            }

            if (!hasPrimaryKey) {
                throw new NoPrimaryKeyException(" no PrimaryKey!");
            }
            return pk.substring(0, pk.length() - 7);
        } catch (SQLException e) {
            throw new NoPrimaryKeyException(e.getMessage());
        } finally {
            try {
                if (rsPrimaryCols != null) {
                    rsPrimaryCols.close();
                }
            } catch (SQLException e) {
               // e.printStackTrace();
            }
        }
    }

    private Element copySourceAsZipperSource(Element srcSource) {
        // 计算出下一个SOURCEFIELD字段的PHYSICALOFFSET值,为本SOURCEFIELD的PHYSICALOFFSET+PHYSICALLENGTH
        Element lastField = (Element) srcSource.selectSingleNode("SOURCEFIELD[last()]");
        String PHYSICALOFFSET = lastField.attribute("PHYSICALOFFSET").getValue();
        String PHYSICALLENGTH = lastField.attribute("PHYSICALLENGTH").getValue();
        Integer fieldNumber = Integer.parseInt(lastField.attribute("FIELDNUMBER").getValue()) + 1;
        Integer nextOffset = Integer.parseInt(PHYSICALOFFSET) + Integer.parseInt(PHYSICALLENGTH);
        // 复制源并增加拉链表的三个字段作为第二个源
        Element tarSource = srcSource.createCopy();
        // 判断已有的字段中是否已经存在三个字段，如果有需要用新的字段来代替
        String[] zipperCols = getZipperCols(srcSource, "SOURCEFIELD");
        tarSource.attribute("DBDNAME").setValue(infaProperty.getProperty("target.dbname", "TARGETDB"));
        tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "date")
                .addAttribute("DESCRIPTION", "insert time").addAttribute("FIELDNUMBER", String.valueOf(fieldNumber))
                .addAttribute("FIELDPROPERTY", "0").addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO")
                .addAttribute("KEYTYPE", "PRIMARY KEY").addAttribute("LENGTH", "19").addAttribute("LEVEL", "0")
                .addAttribute("NAME", zipperCols[0]).addAttribute("NULLABLE", "NOTNULL").addAttribute("OCCURS", "0")
                .addAttribute("OFFSET", "19").addAttribute("PHYSICALLENGTH", "19")
                .addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset)).addAttribute("PICTURETEXT", "")
                .addAttribute("PRECISION", "19").addAttribute("SCALE", "0").addAttribute("USAGE_FLAGS", "");
        tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "date")
                .addAttribute("DESCRIPTION", "delete or update time")
                .addAttribute("FIELDNUMBER", String.valueOf(fieldNumber + 1)).addAttribute("FIELDPROPERTY", "0")
                .addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO").addAttribute("KEYTYPE", "NOT A KEY")
                .addAttribute("LENGTH", "19").addAttribute("LEVEL", "0").addAttribute("NAME", zipperCols[1])
                .addAttribute("NULLABLE", "NULL").addAttribute("OCCURS", "0").addAttribute("OFFSET", "38")
                .addAttribute("PHYSICALLENGTH", "19").addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset + 19))
                .addAttribute("PICTURETEXT", "").addAttribute("PRECISION", "19").addAttribute("SCALE", "0")
                .addAttribute("USAGE_FLAGS", "");
        tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "varchar2")
                .addAttribute("DESCRIPTION", "data status").addAttribute("FIELDNUMBER", String.valueOf(fieldNumber + 2))
                .addAttribute("FIELDPROPERTY", "0").addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO")
                .addAttribute("KEYTYPE", "NOT A KEY").addAttribute("LENGTH", "1").addAttribute("LEVEL", "0")
                .addAttribute("NAME", zipperCols[2]).addAttribute("NULLABLE", "NULL").addAttribute("OCCURS", "0")
                .addAttribute("OFFSET", "57").addAttribute("PHYSICALLENGTH", "1")
                .addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset + 38)).addAttribute("PICTURETEXT", "")
                .addAttribute("PRECISION", "1").addAttribute("SCALE", "0").addAttribute("USAGE_FLAGS", "");
        return tarSource;
    }

    private Element copySourceAsAddSource(Element srcSource) {
        // 计算出下一个SOURCEFIELD字段的PHYSICALOFFSET值,为本SOURCEFIELD的PHYSICALOFFSET+PHYSICALLENGTH
        Element lastField = (Element) srcSource.selectSingleNode("SOURCEFIELD[last()]");
        String PHYSICALOFFSET = lastField.attribute("PHYSICALOFFSET").getValue();
        String PHYSICALLENGTH = lastField.attribute("PHYSICALLENGTH").getValue();
        Integer fieldNumber = Integer.parseInt(lastField.attribute("FIELDNUMBER").getValue()) + 1;
        Integer nextOffset = Integer.parseInt(PHYSICALOFFSET) + Integer.parseInt(PHYSICALLENGTH);
        // 复制源并增加表的三个字段作为第二个源
        Element tarSource = srcSource.createCopy();

        // 判断已有的字段中是否已经存在三个字段，如果有需要用新的字段来代替
        String[] addCols = getAddCols(srcSource, "SOURCEFIELD");
        tarSource.attribute("DBDNAME").setValue(infaProperty.getProperty("target.dbname", "TARGETDB"));
        tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "varchar2")
                .addAttribute("DESCRIPTION", "PK").addAttribute("FIELDNUMBER", String.valueOf(fieldNumber))
                .addAttribute("FIELDPROPERTY", "0").addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO")
                .addAttribute("KEYTYPE", "NOT A KEY").addAttribute("LENGTH", "19").addAttribute("LEVEL", "0")
                .addAttribute("NAME", addCols[0]).addAttribute("NULLABLE", "NOTNULL").addAttribute("OCCURS", "0")
                .addAttribute("OFFSET", "57").addAttribute("PHYSICALLENGTH", "128")
                .addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset)).addAttribute("PICTURETEXT", "")
                .addAttribute("PRECISION", "128").addAttribute("SCALE", "0").addAttribute("USAGE_FLAGS", "");

        tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "date")
                .addAttribute("DESCRIPTION", "delete or update time")
                .addAttribute("FIELDNUMBER", String.valueOf(fieldNumber + 1)).addAttribute("FIELDPROPERTY", "0")
                .addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO").addAttribute("KEYTYPE", "NOT A KEY")
                .addAttribute("LENGTH", "19").addAttribute("LEVEL", "0").addAttribute("NAME", addCols[1])
                .addAttribute("NULLABLE", "NULL").addAttribute("OCCURS", "0").addAttribute("OFFSET", "38")
                .addAttribute("PHYSICALLENGTH", "19").addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset + 128))
                .addAttribute("PICTURETEXT", "").addAttribute("PRECISION", "19").addAttribute("SCALE", "0")
                .addAttribute("USAGE_FLAGS", "");

        tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "varchar2")
                .addAttribute("DESCRIPTION", "data status").addAttribute("FIELDNUMBER", String.valueOf(fieldNumber + 2))
                .addAttribute("FIELDPROPERTY", "0").addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO")
                .addAttribute("KEYTYPE", "NOT A KEY").addAttribute("LENGTH", "1").addAttribute("LEVEL", "0")
                .addAttribute("NAME", addCols[2]).addAttribute("NULLABLE", "NULL").addAttribute("OCCURS", "0")
                .addAttribute("OFFSET", "57").addAttribute("PHYSICALLENGTH", "1")
                .addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset + 147)).addAttribute("PICTURETEXT", "")
                .addAttribute("PRECISION", "1").addAttribute("SCALE", "0").addAttribute("USAGE_FLAGS", "");
        return tarSource;
    }

    @SuppressWarnings("unchecked")
    private Element copySourceAsTarget(Element tarSource) {
        Element target = tarSource.createCopy();
        target.setName("TARGET");
        target.addAttribute("CONSTRAINT", "");
        target.addAttribute("TABLEOPTIONS", "");
        target.remove(target.attribute("DBDNAME"));
        target.remove(target.attribute("OWNERNAME"));

        Element e;
        for (Iterator<Element> it = target.elementIterator(); it.hasNext();) {
            e = it.next();
            e.setName("TARGETFIELD");
            e.remove(e.attribute("FIELDPROPERTY"));
            e.remove(e.attribute("FIELDTYPE"));
            e.remove(e.attribute("HIDDEN"));
            e.remove(e.attribute("LENGTH"));
            e.remove(e.attribute("LEVEL"));
            e.remove(e.attribute("OCCURS"));
            e.remove(e.attribute("OFFSET"));
            e.remove(e.attribute("PHYSICALLENGTH"));
            e.remove(e.attribute("PHYSICALOFFSET"));
            e.remove(e.attribute("USAGE_FLAGS"));
        }
        return target;
    }

    private ArrayList<Element> createMappingVariables(String mappingVariables) {
        String[] variablesSplit = mappingVariables.split(",");
        ArrayList<Element> els = new ArrayList<>();
        Element el ;
        for (int i = 0; i < variablesSplit.length; i = i + 3) {
            el = DocumentHelper.createElement("MAPPINGVARIABLE").addAttribute("DATATYPE", variablesSplit[i + 1])
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                    .addAttribute("ISEXPRESSIONVARIABLE", "NO").addAttribute("ISPARAM", "YES")
                    .addAttribute("NAME", variablesSplit[i]).addAttribute("PRECISION", variablesSplit[i + 2])
                    .addAttribute("SCALE", "0").addAttribute("USERDEFINED", "YES");
            els.add(el);
        }
        return els;
    }

    private ArrayList<Element> createRouterToConnector(Element routerInstance, Element router, Element toInstance,
            Element toElement, String groupPrefix) {
        String fromInstanceName = routerInstance.attributeValue("NAME");
        String fromInstancetype = routerInstance.attributeValue("TRANSFORMATION_TYPE");
        String toInstanceName = toInstance.attributeValue("NAME");
        String toInstancetype = toInstance.attributeValue("TRANSFORMATION_TYPE");
        ArrayList<Element> connectors = new ArrayList<>();
        Element to ;
        Element connector ;
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

    private ArrayList<Element> createToJoinerConnector(Element fromInstance, Element fromElement,
            Element joinerInstance, Element joiner, String joinerFieldPrefix) {
        String fromInstanceName = fromInstance.attributeValue("NAME");
        String fromInstancetype = fromInstance.attributeValue("TRANSFORMATION_TYPE");
        String toInstanceName = joinerInstance.attributeValue("NAME");
        String toInstancetype = joinerInstance.attributeValue("TRANSFORMATION_TYPE");
        ArrayList<Element> connectors = new ArrayList<>();

        Element from ;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = fromElement.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            from = it.next();
            connectors.add(createConnector(from.attributeValue("NAME"), fromInstanceName, fromInstancetype,
                    from.attributeValue("NAME") + joinerFieldPrefix, toInstanceName, toInstancetype));
        }
        return connectors;
    }

    private Element createTargetInstance(Element target, String instancePrefixName) {
        Element targetInstance = DocumentHelper.createElement("INSTANCE")
                .addAttribute("DESCRIPTION", String.valueOf(instanceNumber++))
                .addAttribute("NAME", instancePrefixName + target.attributeValue("NAME"))
                .addAttribute("TRANSFORMATION_NAME", target.attributeValue("NAME"))
                .addAttribute("TRANSFORMATION_TYPE", "Target Definition").addAttribute("TYPE", "TARGET");
        return targetInstance;
    }

    private Element createTransformInstance(Element transform) {
        Element transformInstance = DocumentHelper.createElement("INSTANCE")
                .addAttribute("DESCRIPTION", String.valueOf(instanceNumber++))
                .addAttribute("NAME", transform.attributeValue("NAME")).addAttribute("REUSABLE", "NO")
                .addAttribute("TRANSFORMATION_NAME", transform.attributeValue("NAME"))
                .addAttribute("TRANSFORMATION_TYPE", transform.attributeValue("TYPE"))
                .addAttribute("TYPE", "TRANSFORMATION");
        return transformInstance;
    }

    private Element createQualifierInstance(Element srcQualifier, Element srcSource) {
        Element qualifierInstance = DocumentHelper.createElement("INSTANCE")
                .addAttribute("DESCRIPTION", String.valueOf(instanceNumber++))
                .addAttribute("NAME", srcQualifier.attributeValue("NAME") + instanceNumber)
                .addAttribute("REUSABLE", "NO").addAttribute("TRANSFORMATION_NAME", srcQualifier.attributeValue("NAME"))
                .addAttribute("TRANSFORMATION_TYPE", "Source Qualifier").addAttribute("TYPE", "TRANSFORMATION");
        qualifierInstance.addElement("ASSOCIATED_SOURCE_INSTANCE").addAttribute("NAME",
                srcSource.attributeValue("NAME"));
        return qualifierInstance;
    }

    private Element createSourceInstance(Element srcSource) {
        Element sourceInstance = DocumentHelper.createElement("INSTANCE")
                .addAttribute("DBDNAME", srcSource.attributeValue("DBDNAME"))
                .addAttribute("DESCRIPTION", String.valueOf(instanceNumber++))
                .addAttribute("NAME", (instanceNumber == 2 ? "S_" : "T_") + srcSource.attributeValue("NAME"))
                .addAttribute("TRANSFORMATION_NAME", srcSource.attributeValue("NAME"))
                .addAttribute("TRANSFORMATION_TYPE", "Source Definition").addAttribute("TYPE", "SOURCE");
        return sourceInstance;
    }

    private Element copySourceAsUpdateStrategy(Element tarSource, String updateStrategyName) {
        Element updateStrategy = createTransformation(updateStrategyName, "Update Strategy");
        // TRANSFORMFIELD
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = tarSource.elementIterator("SOURCEFIELD"); it.hasNext();) {
            e = it.next();
            updateStrategy.addElement("TRANSFORMFIELD")
                    .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(e.attributeValue("DATATYPE"), "string"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INPUT")
                    .addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Update Strategy Expression")
                .addAttribute("VALUE", "DD_UPDATE");
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Forward Rejected Rows").addAttribute("VALUE",
                "YES");
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE",
                "Normal");
        return updateStrategy;
    }

    private Element copyTargetAsDeleteStrategy(Element target, String deleteStrategyName) {
        Element updateStrategy = createTransformation(deleteStrategyName, "Update Strategy");
        // TRANSFORMFIELD
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = target.elementIterator("TARGETFIELD"); it.hasNext();) {
            e = it.next();
            if ("PRIMARY KEY".equals(e.attributeValue("KEYTYPE"))) {
                updateStrategy.addElement("TRANSFORMFIELD")
                        .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(e.attributeValue("DATATYPE"), "string"))
                        .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INPUT")
                        .addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
                        .addAttribute("PORTTYPE", "INPUT/OUTPUT")
                        .addAttribute("PRECISION", e.attributeValue("PRECISION"))
                        .addAttribute("SCALE", e.attributeValue("SCALE"));
            }
        }
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Update Strategy Expression")
                .addAttribute("VALUE", "DD_DELETE");
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Forward Rejected Rows").addAttribute("VALUE",
                "YES");
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE",
                "Normal");
        return updateStrategy;
    }

    private Element copySourceAsDeleteStrategy(Element tarSource, String deleteStrategyName) {
        Element updateStrategy = createTransformation(deleteStrategyName, "Update Strategy");
        // TRANSFORMFIELD
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = tarSource.elementIterator("SOURCEFIELD"); it.hasNext();) {
            e = it.next();
            if ("PRIMARY KEY".equals(e.attributeValue("KEYTYPE"))) {
                updateStrategy.addElement("TRANSFORMFIELD")
                        .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(e.attributeValue("DATATYPE"), "string"))
                        .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INPUT")
                        .addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
                        .addAttribute("PORTTYPE", "INPUT/OUTPUT")
                        .addAttribute("PRECISION", e.attributeValue("PRECISION"))
                        .addAttribute("SCALE", e.attributeValue("SCALE"));
            }
        }
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Update Strategy Expression")
                .addAttribute("VALUE", "DD_UPDATE");
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Forward Rejected Rows").addAttribute("VALUE",
                "YES");
        updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE",
                "Normal");
        return updateStrategy;
    }

    private Element copyJoinerAsRouter(Element joiner, Element source) {
        Element router = createTransformation("RTRTRANS", "Router");
        // Groups
        String primaryKey = ((Element) source.selectSingleNode("SOURCEFIELD[@KEYTYPE='PRIMARY KEY']"))
                .attributeValue("NAME");
        String insertGroup = "NOT ISNULL(" + primaryKey + "_SRC) AND ISNULL(" + primaryKey + "_TAR)";
        String deleteGroup = "ISNULL(" + primaryKey + "_SRC) AND NOT ISNULL(" + primaryKey + "_TAR)";
        String updatetGroup = "NOT ISNULL(" + primaryKey + "_SRC) AND NOT ISNULL(" + primaryKey
                + "_TAR) AND CRC32_SRC != CRC32_TAR";
        router.addElement("GROUP").addAttribute("DESCRIPTION", "").addAttribute("NAME", "INPUT")
                .addAttribute("ORDER", "1").addAttribute("TYPE", "INPUT");
        router.addElement("GROUP").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", insertGroup)
                .addAttribute("NAME", "INSERT").addAttribute("ORDER", "2").addAttribute("TYPE", "OUTPUT");
        router.addElement("GROUP").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", updatetGroup)
                .addAttribute("NAME", "UPDATE").addAttribute("ORDER", "3").addAttribute("TYPE", "OUTPUT");
        router.addElement("GROUP").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", deleteGroup)
                .addAttribute("NAME", "DELETE").addAttribute("ORDER", "4").addAttribute("TYPE", "OUTPUT");
        router.addElement("GROUP").addAttribute("DESCRIPTION", "").addAttribute("NAME", "DEFAULT")
                .addAttribute("ORDER", "5").addAttribute("TYPE", "OUTPUT/DEFAULT");

        Element e;
        // TRANSFORMFIELD INPUT
        for (@SuppressWarnings("unchecked") Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INPUT")
                    .addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "INPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }
        // TRANSFORMFIELD INSERT
        for (@SuppressWarnings("unchecked") Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INSERT")
                    .addAttribute("NAME", e.attributeValue("NAME") + "_I").addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("REF_FIELD", e.attributeValue("NAME"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }
        // TRANSFORMFIELD UPDATE
        for (@SuppressWarnings("unchecked") Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "UPDATE")
                    .addAttribute("NAME", e.attributeValue("NAME") + "_U").addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("REF_FIELD", e.attributeValue("NAME"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }
        // TRANSFORMFIELD DELETE
        for (@SuppressWarnings("unchecked") Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "DELETE")
                    .addAttribute("NAME", e.attributeValue("NAME") + "_D").addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("REF_FIELD", e.attributeValue("NAME"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }
        // TRANSFORMFIELD DEFAULT
        for (@SuppressWarnings("unchecked") Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "DEFAULT")
                    .addAttribute("NAME", e.attributeValue("NAME") + "_DF").addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("REF_FIELD", e.attributeValue("NAME"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }
        // TABLEATTRIBUTE
        router.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE", "Normal");
        return router;
    }

    private Element copySorterAsJoiner(Element srcSorter, Element tarSorter) {
        Element joiner = createTransformation("JNRTRANS", "Joiner");
        StringBuilder joinCondition = new StringBuilder();
        // TRANSFORMFIELD
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = srcSorter.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            // 关联字段获取
            if ("YES".equals(e.attributeValue("ISSORTKEY"))) {
                joinCondition.append(e.attributeValue("NAME"));
                joinCondition.append("_SRC");
                joinCondition.append(" = ");
                joinCondition.append(e.attributeValue("NAME"));
                joinCondition.append("_TAR");
                joinCondition.append(" AND ");
            }

            joiner.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                    .addAttribute("NAME", e.attributeValue("NAME") + "_SRC").addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "INPUT/OUTPUT/MASTER")
                    .addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }

        for (@SuppressWarnings("unchecked") Iterator<Element> it = tarSorter.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            joiner.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                    .addAttribute("NAME", e.attributeValue("NAME") + "_TAR").addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }

        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Case Sensitive String Comparison")
                .addAttribute("VALUE", "YES");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Cache Directory").addAttribute("VALUE",
                "$PMCacheDir/");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Join Condition").addAttribute("VALUE",
                joinCondition.substring(0, joinCondition.length() - 5));
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Join Type").addAttribute("VALUE", "Full Outer Join");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Null ordering in master").addAttribute("VALUE",
                "Null Is Highest Value");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Null ordering in detail").addAttribute("VALUE",
                "Null Is Highest Value");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE", "Normal");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Joiner Data Cache Size").addAttribute("VALUE",
                "Auto");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Joiner Index Cache Size").addAttribute("VALUE",
                "Auto");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Sorted Input").addAttribute("VALUE", "YES");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Master Sort Order").addAttribute("VALUE", "Auto");
        joiner.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Transformation Scope").addAttribute("VALUE",
                "All Input");

        return joiner;
    }

    private Element copyQualifierAsExpression(Element srcQualifier, Element addTransformfield) {
        Element exptrans = createTransformation("EXPTRANS" + swtNamePostfix++, "Expression");
        if (addTransformfield != null) {
            exptrans.add(addTransformfield.createCopy());
        }

        // TRANSFORMFIELD
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = srcQualifier.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            exptrans.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                    .addAttribute("EXPRESSION", e.attributeValue("NAME")).addAttribute("EXPRESSIONTYPE", "GENERAL")
                    .addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }
        return exptrans;
    }

    private Element createCRC32Expressionfield(Element srcQualifier) {
        String crc32 = getCRC32(srcQualifier);
        Element crc32Expressionfield = DocumentHelper.createElement("TRANSFORMFIELD").addAttribute("DATATYPE", "string")
                .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", crc32)
                .addAttribute("EXPRESSIONTYPE", "GENERAL").addAttribute("NAME", "CRC32").addAttribute("PICTURETEXT", "")
                .addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", "32").addAttribute("SCALE", "0");
        return crc32Expressionfield;
    }

    private Element copyExpressionAsSorter(Element expression, Element source) {
        Element sorter = createTransformation("SRTTRANS" + swtNamePostfix++, "Sorter");
        // TRANSFORMFIELD
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = expression.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            sorter.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("ISSORTKEY", "NO")
                    .addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("SCALE", e.attributeValue("SCALE")).addAttribute("SORTDIRECTION", "ASCENDING");
        }
        // 设置按照主键排序，从source中获取主键
        for (@SuppressWarnings("unchecked") Iterator<Element> it = source.elementIterator("SOURCEFIELD"); it.hasNext();) {
            e = it.next();
            if ("PRIMARY KEY".equals(e.attributeValue("KEYTYPE"))) {
                ((Element) sorter.selectSingleNode("TRANSFORMFIELD[@NAME='" + e.attributeValue("NAME") + "']"))
                        .attribute("ISSORTKEY").setValue("YES");
            }
        }
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Sorter Cache Size").addAttribute("VALUE", "Auto");
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Case Sensitive").addAttribute("VALUE", "YES");
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Work Directory").addAttribute("VALUE", "$PMTempDir/");
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Distinct").addAttribute("VALUE", "NO");
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE", "Normal");
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Null Treated Low").addAttribute("VALUE", "NO");
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Merge Only").addAttribute("VALUE", "NO");
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Partitioning").addAttribute("VALUE",
                "Order records for individual partitions");
        sorter.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Transformation Scope").addAttribute("VALUE",
                "All Input");
        return sorter;
    }

    private Element copySourceAsQualifier(Element source) {
        Element qualifier = createTransformation(
                infaProperty.getProperty("qualifier.prefix", "SQ_") + source.attributeValue("NAME"),
                "Source Qualifier");

        // TRANSFORMFIELD
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = source.elementIterator(); it.hasNext();) {
            e = it.next();
            qualifier.addElement("TRANSFORMFIELD")
                    .addAttribute("DATATYPE", dataTypeO2I.getOrDefault(e.attributeValue("DATATYPE"), "string"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                    .addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
                    .addAttribute("SCALE", e.attributeValue("SCALE"));
        }
        // 默认TABLEATTRIBUTE
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Sql Query").addAttribute("VALUE", "");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "User Defined Join").addAttribute("VALUE", "");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Source Filter").addAttribute("VALUE", "");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Number Of Sorted Ports").addAttribute("VALUE",
                "0");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE", "Normal");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Select Distinct").addAttribute("VALUE", "NO");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Is Partitionable").addAttribute("VALUE", "NO");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Pre SQL").addAttribute("VALUE", "");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Post SQL").addAttribute("VALUE", "");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Output is deterministic").addAttribute("VALUE",
                "No");
        qualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Output is repeatable").addAttribute("VALUE",
                "Never");
        return qualifier;
    }

    @SuppressWarnings("unused")
    private Element createSessionConfiguration() {
        Element sessionConfiguration = DocumentHelper.createElement("CONFIG");
        return sessionConfiguration;
    }

    /**
     * *
     * private Element createZipperWorkflow(Element mapping) { Element workflow
     * = DocumentHelper.createElement("WORKFLOW") .addAttribute("DESCRIPTION",
     * mapping.attributeValue("DESCRIPTION")).addAttribute("ISENABLED", "YES")
     * .addAttribute("ISRUNNABLESERVICE", "NO").addAttribute("ISSERVICE",
     * "NO").addAttribute("ISVALID", "YES") .addAttribute("NAME",
     * infaProperty.getProperty("workflow.prefix", "WF_") +
     * mapping.attributeValue("NAME")) .addAttribute("REUSABLE_SCHEDULER",
     * "NO").addAttribute("SCHEDULERNAME", "计划程序") .addAttribute("SERVERNAME",
     * infaProperty.getProperty("service.name", "infa_is"))
     * .addAttribute("SERVER_DOMAINNAME",
     * infaProperty.getProperty("server.domainname", "GDDW_GRID_DM"))
     * .addAttribute("SUSPEND_ON_ERROR",
     * "NO").addAttribute("TASKS_MUST_RUN_ON_SERVER", "NO")
     * .addAttribute("VERSIONNUMBER", "1");
     *
     * // SCHEDULER Element scheduler = createWorkflowScheduler("计划程序");
     *
     * // TASK Element startTarsk =
     * DocumentHelper.createElement("TASK").addAttribute("DESCRIPTION", "")
     * .addAttribute("NAME", "start").addAttribute("REUSABLE",
     * "NO").addAttribute("TYPE", "Start") .addAttribute("VERSIONNUMBER", "1");
     * Element session = createZipperSession(mapping);
     *
     * // TASKINSTANCE Element startTarskInstance =
     * DocumentHelper.createElement("TASKINSTANCE").addAttribute("DESCRIPTION",
     * "") .addAttribute("ISENABLED", "YES").addAttribute("NAME",
     * "start").addAttribute("REUSABLE", "NO") .addAttribute("TASKNAME",
     * "start").addAttribute("TASKTYPE", "Start"); Element sessionInstance =
     * DocumentHelper.createElement("TASKINSTANCE").addAttribute("DESCRIPTION",
     * "") .addAttribute("FAIL_PARENT_IF_INSTANCE_DID_NOT_RUN", "NO")
     * .addAttribute("FAIL_PARENT_IF_INSTANCE_FAILS",
     * "YES").addAttribute("ISENABLED", "YES") .addAttribute("NAME",
     * session.attributeValue("NAME")).addAttribute("REUSABLE", "NO")
     * .addAttribute("TASKNAME",
     * session.attributeValue("NAME")).addAttribute("TASKTYPE", "Session")
     * .addAttribute("TREAT_INPUTLINK_AS_AND", "YES"); // WORKFLOWLINK Element
     * workflowLink =
     * DocumentHelper.createElement("WORKFLOWLINK").addAttribute("CONDITION",
     * "") .addAttribute("FROMTASK", "start").addAttribute("TOTASK",
     * session.attributeValue("NAME"));
     *
     * // workFlow adds workflow.add(scheduler); workflow.add(startTarsk);
     * workflow.add(session); workflow.add(startTarskInstance);
     * workflow.add(sessionInstance); workflow.add(workflowLink);
     *
     * // WORKFLOWVARIABLE
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "date/time").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION
     * ", "The time this task started").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME",
     * "$start.StartTime") .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "date/time").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION
     * ", "The time this task completed").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME",
     * "$start.EndTime") .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Status of this task execution").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$start.Status")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Status of the previous task that is not disabled")
     * .addAttribute("ISNULL", "NO").addAttribute("ISPERSISTENT", "NO")
     * .addAttribute("NAME",
     * "$start.PrevTaskStatus").addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Error code for this task s execution").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME",
     * "$start.ErrorCode") .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "string").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Error message for this task s execution").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO").addAttribute("NAME",
     * "$start.ErrorMsg") .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "date/time").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION
     * ", "The time this task started").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".StartTime")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "date/time").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION
     * ", "The time this task completed").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".EndTime") .addAttribute("USERDEFINED",
     * "NO"); workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Status of this task&apos;s execution").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".Status") .addAttribute("USERDEFINED",
     * "NO"); workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Status of the previous task that is not disabled")
     * .addAttribute("ISNULL", "NO").addAttribute("ISPERSISTENT", "NO")
     * .addAttribute("NAME", "$" + session.attributeValue("NAME") +
     * ".PrevTaskStatus") .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Error code for this task&apos;s execution").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".ErrorCode")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "string").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Error message for this task&apos;s execution") .addAttribute("ISNULL",
     * "NO").addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".ErrorMsg")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Rows successfully read").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".SrcSuccessRows")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Rows failed to read").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".SrcFailedRows")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Rows successfully loaded").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".TgtSuccessRows")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Rows failed to load").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".TgtFailedRows")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "Total number of transformation errors").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".TotalTransErrors")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "integer").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "First error code").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".FirstErrorCode")
     * .addAttribute("USERDEFINED", "NO");
     * workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE",
     * "string").addAttribute("DEFAULTVALUE", "") .addAttribute("DESCRIPTION ",
     * "First error message").addAttribute("ISNULL", "NO")
     * .addAttribute("ISPERSISTENT", "NO") .addAttribute("NAME", "$" +
     * session.attributeValue("NAME") + ".FirstErrorMsg")
     * .addAttribute("USERDEFINED", "NO"); // ATTRIBUTE
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Parameter
     * Filename").addAttribute("VALUE",
     * infaProperty.getProperty("workflow.paramter.filename", ""));
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Write Backward
     * Compatible Workflow Log File") .addAttribute("VALUE", "NO");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Workflow Log File
     * Name").addAttribute("VALUE", session.attributeValue("NAME") + ".log");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Workflow Log File
     * Directory").addAttribute("VALUE", "$PMWorkflowLogDir/");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Save Workflow log
     * by").addAttribute("VALUE", "By runs");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Save workflow log
     * for these runs").addAttribute("VALUE", "0");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Service
     * Name").addAttribute("VALUE", "");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Service
     * Timeout").addAttribute("VALUE", "0");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Is Service
     * Visible").addAttribute("VALUE", "NO");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Is Service
     * Protected").addAttribute("VALUE", "NO");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Enable HA
     * recovery").addAttribute("VALUE", "NO");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Automatically
     * recover terminated tasks") .addAttribute("VALUE", "NO");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Service Level
     * Name").addAttribute("VALUE", "Default");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Allow concurrent
     * run with unique run instance name") .addAttribute("VALUE", "NO");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Allow concurrent
     * run with same run instance name") .addAttribute("VALUE", "NO");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Maximum number of
     * concurrent runs").addAttribute("VALUE", "0");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Assigned Web
     * Services Hubs").addAttribute("VALUE", "");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Maximum number of
     * concurrent runs per Hub") .addAttribute("VALUE", "1000");
     * workflow.addElement("ATTRIBUTE").addAttribute("NAME", "Expected Service
     * Time").addAttribute("VALUE", "1"); return workflow; }
	 ***
     */
    private Element createWriterSessionextension(Element instance) {
        // <SESSIONEXTENSION NAME ="Relational Writer" SINSTANCENAME
        // ="INSERTT_PMIS_CIM_CUSTOMER" SUBTYPE ="Relational Writer" TRANSFORMATIONTYPE
        // ="Target Definition" TYPE ="WRITER">

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

    private Element createReaderSessionextension(Element instanceQualifier, Element instanceDefinition) {
        Element sessionextension = DocumentHelper.createElement("SESSIONEXTENSION")
                .addAttribute("DSQINSTNAME", instanceQualifier.attributeValue("NAME"))
                .addAttribute("DSQINSTTYPE", "Source Qualifier").addAttribute("NAME", "Relational Reader")
                .addAttribute("SINSTANCENAME", instanceDefinition.attributeValue("NAME"))
                .addAttribute("SUBTYPE", "Relational Reader").addAttribute("TRANSFORMATIONTYPE", "Source Definition")
                .addAttribute("TYPE", "READER");
        return sessionextension;
    }

    private Element createReaderSessionextension(Element instance) {
        Element sessionextension = DocumentHelper.createElement("SESSIONEXTENSION")
                .addAttribute("NAME", "Relational Reader")
                .addAttribute("SINSTANCENAME", instance.attributeValue("NAME"))
                .addAttribute("SUBTYPE", "Relational Reader").addAttribute("TRANSFORMATIONTYPE", "Source Qualifier")
                .addAttribute("TYPE", "READER");
        if ("4".equals(instance.attributeValue("DESCRIPTION"))) {
            sessionextension.addElement("CONNECTIONREFERENCE").addAttribute("CNXREFNAME", "DB Connection")
                    .addAttribute("CONNECTIONNAME", "").addAttribute("CONNECTIONNUMBER", "1")
                    .addAttribute("CONNECTIONSUBTYPE", "").addAttribute("CONNECTIONTYPE", "Relational")
                    .addAttribute("VARIABLE", "$DBConnectionTAR");
        } else {
            sessionextension.addElement("CONNECTIONREFERENCE").addAttribute("CNXREFNAME", "DB Connection")
                    .addAttribute("CONNECTIONNAME", "").addAttribute("CONNECTIONNUMBER", "1")
                    .addAttribute("CONNECTIONSUBTYPE", "").addAttribute("CONNECTIONTYPE", "Relational")
                    .addAttribute("VARIABLE", "$DBConnectionSRC");
        }
        return sessionextension;
    }

    private Element createSessTransformationInst(Element instance) {
        Element sessTransformationInst = DocumentHelper.createElement("SESSTRANSFORMATIONINST");
        if ("Source Definition".equals(instance.attributeValue("TRANSFORMATION_TYPE"))) {
            sessTransformationInst.addAttribute("ISREPARTITIONPOINT", "NO").addAttribute("PIPELINE", "0")
                    .addAttribute("SINSTANCENAME", instance.attributeValue("NAME")).addAttribute("STAGE", "0")
                    .addAttribute("TRANSFORMATIONNAME", instance.attributeValue("TRANSFORMATION_NAME"))
                    .addAttribute("TRANSFORMATIONTYPE", instance.attributeValue("TRANSFORMATION_TYPE"));
            // 如果是源的话，很可能有source owner
            if ("1".equals(instance.attributeValue("DESCRIPTION"))) {
                sessTransformationInst.addElement("ATTRIBUTE").addAttribute("NAME", "Owner Name").addAttribute("VALUE",
                        owner);
            }
        } else if ("Source Qualifier".equals(instance.attributeValue("TRANSFORMATION_TYPE"))) {
            sessTransformationInst.addAttribute("ISREPARTITIONPOINT", "YES")
                    .addAttribute("PARTITIONTYPE", "PASS THROUGH")
                    .addAttribute("PIPELINE",
                            String.valueOf(Integer.parseInt(instance.attributeValue("DESCRIPTION")) - 2))
                    .addAttribute("SINSTANCENAME", instance.attributeValue("NAME"))
                    .addAttribute("STAGE", String.valueOf(Integer.parseInt(instance.attributeValue("DESCRIPTION")) - 4))
                    .addAttribute("TRANSFORMATIONNAME", instance.attributeValue("TRANSFORMATION_NAME"))
                    .addAttribute("TRANSFORMATIONTYPE", instance.attributeValue("TRANSFORMATION_TYPE"));
        } else if ("Target Definition".equals(instance.attributeValue("TRANSFORMATIONTYPE"))) {
            sessTransformationInst.addAttribute("ISREPARTITIONPOINT", "YES")
                    .addAttribute("PARTITIONTYPE", "PASS THROUGH").addAttribute("PIPELINE", "2")
                    .addAttribute("SINSTANCENAME", instance.attributeValue("NAME"))
                    .addAttribute("STAGE",
                            String.valueOf(Integer.parseInt(instance.attributeValue("DESCRIPTION")) - 17))
                    .addAttribute("TRANSFORMATIONNAME", instance.attributeValue("TRANSFORMATION_NAME"))
                    .addAttribute("TRANSFORMATIONTYPE", instance.attributeValue("TRANSFORMATION_TYPE"));
        } else if ("5".equals(instance.attributeValue("DESCRIPTION"))
                || "7".equals(instance.attributeValue("DESCRIPTION"))) {
            sessTransformationInst.addAttribute("ISREPARTITIONPOINT", "YES")
                    .addAttribute("PARTITIONTYPE", "PASS THROUGH").addAttribute("PIPELINE", "1")
                    .addAttribute("SINSTANCENAME", instance.attributeValue("NAME")).addAttribute("STAGE", "5")
                    .addAttribute("TRANSFORMATIONNAME", instance.attributeValue("TRANSFORMATION_NAME"))
                    .addAttribute("TRANSFORMATIONTYPE", instance.attributeValue("TRANSFORMATION_TYPE"));
            sessTransformationInst.addElement("PARTITION").addAttribute("DESCRIPTION", "").addAttribute("NAME",
                    "分区编号1");
        } else {
            sessTransformationInst.addAttribute("ISREPARTITIONPOINT", "YES")
                    .addAttribute("PARTITIONTYPE", "PASS THROUGH").addAttribute("PIPELINE", "2")
                    .addAttribute("SINSTANCENAME", instance.attributeValue("NAME")).addAttribute("STAGE", "6")
                    .addAttribute("TRANSFORMATIONNAME", instance.attributeValue("TRANSFORMATION_NAME"))
                    .addAttribute("TRANSFORMATIONTYPE", instance.attributeValue("TRANSFORMATION_TYPE"));
            sessTransformationInst.addElement("PARTITION").addAttribute("DESCRIPTION", "").addAttribute("NAME",
                    "分区编号1");
        }
        return sessTransformationInst;
    }

    private Element createWorkflowScheduler(String schedulerName) {
        Element scheduler = DocumentHelper.createElement("SCHEDULER").addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", schedulerName).addAttribute("REUSABLE", "NO").addAttribute("VERSIONNUMBER", "1");
        scheduler.addElement("SCHEDULEINFO").addAttribute("SCHEDULETYPE", "ONDEMAND");
        return scheduler;
    }

    private String getCRC32(Element srcQualifier) {
        StringBuilder sb = new StringBuilder();
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = srcQualifier.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            e = it.next();
            if (null == e.attributeValue("DATATYPE")) {
                sb.append("to_char(");
                sb.append(e.attributeValue("NAME"));
                sb.append(")||");
            } else switch (e.attributeValue("DATATYPE")) {
                case "date/time":
                    sb.append("to_char(");
                    sb.append(e.attributeValue("NAME"));
                    sb.append(",'YYYYMMDDHH24MISS')||");
                    break;
                case "nstring":
                case "string":
                case "text":
                case "ntext":
                    sb.append(e.attributeValue("NAME"));
                    sb.append("||");
                    break;
            // binary不支持to_char crc32校验
                case "binary":
                    break;
            // blob不校验
                case "blob":
                    break;
            // clob不校验
                case "clob":
                    break;
                default:
                    sb.append("to_char(");
                    sb.append(e.attributeValue("NAME"));
                    sb.append(")||");
                    break;
            }
        }
        String crc32 = sb.substring(0, sb.length() - 2);
        return "CRC32(" + crc32 + ")";
    }

    /**
     * *
     * 判断拉链表用哪三个拉链字段名称 根据是否配置文件指定，是否源表存在等来综合判断
     */
    private String[] getZipperCols(Element source, String nodeType) {
        String[] zipperCols = new String[3];
        // 先看是否强制使用配置文件中的配置
        String force = infaProperty.getProperty("zipper.col.force", "false").toUpperCase();
        if ("TRUE".equals(force)) {
            // 如果强制用配置文件中的配置(强制又不指定，仍然用默认的三个字段)
            zipperCols[0] = infaProperty.getProperty("zipper.col.qysj", "QYSJ").toUpperCase();
            zipperCols[1] = infaProperty.getProperty("zipper.col.sxsj", "SXSJ").toUpperCase();
            zipperCols[2] = infaProperty.getProperty("zipper.col.jlzt", "JLZT").toUpperCase();
        } else {
            // 不强制用配置文件中的配置

            // 先设置默认使用的值
            zipperCols[0] = "QYSJ";
            zipperCols[1] = "SXSJ";
            zipperCols[2] = "JLZT";
            // 获取source是否存在三个字段
            Element eqysj = ((Element) source.selectSingleNode(nodeType + "[@NAME='QYSJ']"));
            Element esxsj = ((Element) source.selectSingleNode(nodeType + "[@NAME='SXSJ']"));
            Element ejlzt = ((Element) source.selectSingleNode(nodeType + "[@NAME='JLZT']"));
            // 如果某个字段已经存在于源表中，就从配置文件中取，如果配置文件中没有配置，用名称+"_$"
            if (eqysj != null) {
                zipperCols[0] = infaProperty.getProperty("zipper.col.qysj", "QYSJ_$").toUpperCase();
            } else if (esxsj != null) {
                zipperCols[1] = infaProperty.getProperty("zipper.col.sxsj", "SXSJ_$").toUpperCase();
            } else if (ejlzt != null) {
                zipperCols[2] = infaProperty.getProperty("zipper.col.jlzt", "JLZT_$").toUpperCase();
            }
        }

        return zipperCols;
    }

    private String[] getAddCols(Element source, String nodeType) {
        String[] AddCols = new String[3];
        // 先看是否强制使用配置文件中的配置
        String force = infaProperty.getProperty("add.col.force", "false").toUpperCase();
        if ("TRUE".equals(force)) {
            // 如果强制用配置文件中的配置(强制又不指定，仍然用默认的三个字段)
            AddCols[0] = infaProperty.getProperty("add.col.hy_id", "HY_ID").toUpperCase();
            AddCols[1] = infaProperty.getProperty("add.col.hy_update_date", "HY_UPDATE_DATE").toUpperCase();
            AddCols[2] = infaProperty.getProperty("add.col.hy_update_flag", "HY_UPDATE_FLAG").toUpperCase();
        } else {
            // 不强制用配置文件中的配置

            // 先设置默认使用的值
            AddCols[0] = "HY_ID";
            AddCols[1] = "HY_UPDATE_DATE";
            AddCols[2] = "HY_UPDATE_FLAG";
            // 获取source是否存在三个字段
            Element ehyid = ((Element) source.selectSingleNode(nodeType + "[@NAME='HY_ID']"));
            Element ehyipdatedate = ((Element) source.selectSingleNode(nodeType + "[@NAME='HY_UPDATE_DATE']"));
            Element ehyflag = ((Element) source.selectSingleNode(nodeType + "[@NAME='HY_UPDATE_FLAG']"));
            // 如果某个字段已经存在于源表中，就从配置文件中取，如果配置文件中没有配置，用名称+"_$"
            if (ehyid != null) {
                AddCols[0] = infaProperty.getProperty("add.col.hy_id", "HY_ID_$").toUpperCase();
            } else if (ehyipdatedate != null) {
                AddCols[1] = infaProperty.getProperty("add.col.hy_update_date", "HY_UPDATE_DATE_$").toUpperCase();
            } else if (ehyflag != null) {
                AddCols[2] = infaProperty.getProperty("add.col.hy_update_flag", "HY_UPDATE_FLAG_$").toUpperCase();
            }
        }

        return AddCols;
    }

    /**
     * Transformation不含里面的TRANSFORMFIELD和TABLEATTRIBUTE
     */
    private Element createTransformation(String TransformationName, String TransformationType) {
        Element transformation = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", TransformationName).addAttribute("OBJECTVERSION", "1")
                .addAttribute("REUSABLE", "NO").addAttribute("TYPE", TransformationType)
                .addAttribute("VERSIONNUMBER", "1");

        return transformation;
    }

    private ArrayList<Element> createConnector(Element fromInstance, Element fromElement, Element toInstance,
            Element toElement) {
        String fromInstanceName = fromInstance.attributeValue("NAME");
        String fromInstancetype = fromInstance.attributeValue("TRANSFORMATION_TYPE");
        String toInstanceName = toInstance.attributeValue("NAME");
        String toInstancetype = toInstance.attributeValue("TRANSFORMATION_TYPE");
        ArrayList<Element> connectors = new ArrayList<>();

        Element from;
        Element to;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = fromElement.elementIterator("SOURCEFIELD"); it.hasNext();) {
            from = it.next();
            to = (Element) toElement.selectSingleNode("TRANSFORMFIELD[@NAME='" + from.attributeValue("NAME") + "']");
            if (to.attributeValue("PORTTYPE").contains("INPUT")) {
                connectors.add(createConnector(from.attributeValue("NAME"), fromInstanceName, fromInstancetype,
                        from.attributeValue("NAME"), toInstanceName, toInstancetype));
            }
        }
        boolean linkto = false;// 判断是否创建连接，如目标没有INPUT时不连接
        // 目标为TARGET时可以连接
        if ("TARGET".equals(toElement.getName())) {
            linkto = true;
        }
        for (@SuppressWarnings("unchecked") Iterator<Element> it = fromElement.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
            from = it.next();
            to = (Element) toElement.selectSingleNode("TRANSFORMFIELD[@NAME='" + from.attributeValue("NAME") + "']");
            if (linkto || to.attributeValue("PORTTYPE").contains("INPUT")) {
                connectors.add(createConnector(from.attributeValue("NAME"), fromInstanceName, fromInstancetype,
                        from.attributeValue("NAME"), toInstanceName, toInstancetype));
            }

        }
        return connectors;
    }

    private Element createConnector(String fromField, String fromInstance, String fromInstancetype, String toField,
            String toInstance, String toInstancetype) {
        Element connector = DocumentHelper.createElement("CONNECTOR").addAttribute("FROMFIELD", fromField)
                .addAttribute("FROMINSTANCE", fromInstance).addAttribute("FROMINSTANCETYPE", fromInstancetype)
                .addAttribute("TOFIELD", toField).addAttribute("TOINSTANCE", toInstance)
                .addAttribute("TOINSTANCETYPE", toInstancetype);

        return connector;
    }

    private Element createWorkflow(Element mapping) {
        Element workflow = DocumentHelper.createElement("WORKFLOW")
                .addAttribute("DESCRIPTION", mapping.attributeValue("DESCRIPTION")).addAttribute("ISENABLED", "YES")
                .addAttribute("ISRUNNABLESERVICE", "NO").addAttribute("ISSERVICE", "NO").addAttribute("ISVALID", "YES")
                .addAttribute("NAME",
                        infaProperty.getProperty("workflow.prefix", "WF_") + mapping.attributeValue("NAME"))
                .addAttribute("REUSABLE_SCHEDULER", "NO").addAttribute("SCHEDULERNAME", "计划程序")
                .addAttribute("SERVERNAME", infaProperty.getProperty("service.name", "infa_is"))
                .addAttribute("SERVER_DOMAINNAME", infaProperty.getProperty("server.domainname", "GDDW_GRID_DM"))
                .addAttribute("SUSPEND_ON_ERROR", "NO").addAttribute("TASKS_MUST_RUN_ON_SERVER", "NO")
                .addAttribute("VERSIONNUMBER", "1");

        // SCHEDULER
        Element scheduler = createWorkflowScheduler("计划程序");

        // TASK
        Element startTarsk = DocumentHelper.createElement("TASK").addAttribute("DESCRIPTION", "")
                .addAttribute("NAME", "start").addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Start")
                .addAttribute("VERSIONNUMBER", "1");

        Element session;
        String mappingType = infaProperty.getProperty("mapping.type", "-1");

        if (null == mappingType) {
            session = createTruncateThenDeleteSession(mapping);
        } else {
            switch (mappingType) {
                case "1":
                    session = createZipperSession(mapping);
                    break;
                case "2":
                    session = createAddSession(mapping);
                    break;
                default:
                    session = createTruncateThenDeleteSession(mapping);
                    break;
            }
        }

        // TASKINSTANCE
        Element startTarskInstance = DocumentHelper.createElement("TASKINSTANCE").addAttribute("DESCRIPTION", "")
                .addAttribute("ISENABLED", "YES").addAttribute("NAME", "start").addAttribute("REUSABLE", "NO")
                .addAttribute("TASKNAME", "start").addAttribute("TASKTYPE", "Start");
        Element sessionInstance = DocumentHelper.createElement("TASKINSTANCE").addAttribute("DESCRIPTION", "")
                .addAttribute("FAIL_PARENT_IF_INSTANCE_DID_NOT_RUN", "NO")
                .addAttribute("FAIL_PARENT_IF_INSTANCE_FAILS", "YES").addAttribute("ISENABLED", "YES")
                .addAttribute("NAME", session.attributeValue("NAME")).addAttribute("REUSABLE", "NO")
                .addAttribute("TASKNAME", session.attributeValue("NAME")).addAttribute("TASKTYPE", "Session")
                .addAttribute("TREAT_INPUTLINK_AS_AND", "YES");
        // WORKFLOWLINK
        Element workflowLink = DocumentHelper.createElement("WORKFLOWLINK").addAttribute("CONDITION", "")
                .addAttribute("FROMTASK", "start").addAttribute("TOTASK", session.attributeValue("NAME"));

        // workFlow adds
        workflow.add(scheduler);
        workflow.add(startTarsk);
        workflow.add(session);
        workflow.add(startTarskInstance);
        workflow.add(sessionInstance);
        workflow.add(workflowLink);

        // WORKFLOWVARIABLE
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
        return workflow;
    }
    // 创建session

    private Element createTruncateThenDeleteSession(Element mapping) {
        // TODO
        Element session = DocumentHelper.createElement("SESSION")
                .addAttribute("DESCRIPTION", mapping.attributeValue("DESCRIPTION")).addAttribute("ISVALID", "YES")
                .addAttribute("MAPPINGNAME", mapping.attributeValue("NAME"))
                .addAttribute("NAME", infaProperty.getProperty("session.prefix", "S_") + mapping.attributeValue("NAME"))
                .addAttribute("REUSABLE", "NO").addAttribute("SORTORDER", "Binary").addAttribute("VERSIONNUMBER", "1");

        // SESSTRANSFORMATIONINST
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = mapping.elementIterator("INSTANCE"); it.hasNext();) {
            e = it.next();
            session.add(createSessTransformationInst(e));
        }
        // CONFIGREFERENCE
        session.addElement("CONFIGREFERENCE").addAttribute("REFOBJECTNAME", "default_session_config")
                .addAttribute("TYPE", "Session config");
        // SESSIONEXTENSION
        // 第一个 source 的 SESSIONEXTENSION
        Element instance1 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='1']");
        Element instance3 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='2']");
        Element instance21 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='4']");

        Element sessionextension1 = createReaderSessionextension(instance3, instance1);
        Element sessionextension3 = createReaderSessionextension(instance3);
        Element sessionextension21 = createWriterSessionextension(instance21);

        ((Element) sessionextension21.selectSingleNode("ATTRIBUTE[@NAME='Truncate target table option']"))
                .addAttribute("VALUE", "YES");

        session.add(sessionextension1);
        session.add(sessionextension3);
        session.add(sessionextension21);

        // ATTRIBUTE
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

        return session;
    }

    private Element createZipperSession(Element mapping) {
        // TODO
        Element session = DocumentHelper.createElement("SESSION")
                .addAttribute("DESCRIPTION", mapping.attributeValue("DESCRIPTION")).addAttribute("ISVALID", "YES")
                .addAttribute("MAPPINGNAME", mapping.attributeValue("NAME"))
                .addAttribute("NAME", infaProperty.getProperty("session.prefix", "S_") + mapping.attributeValue("NAME"))
                .addAttribute("REUSABLE", "NO").addAttribute("SORTORDER", "Binary").addAttribute("VERSIONNUMBER", "1");

        // SESSTRANSFORMATIONINST
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = mapping.elementIterator("INSTANCE"); it.hasNext();) {
            e = it.next();
            session.add(createSessTransformationInst(e));
        }
        // CONFIGREFERENCE
        session.addElement("CONFIGREFERENCE").addAttribute("REFOBJECTNAME", "default_session_config")
                .addAttribute("TYPE", "Session config");
        // SESSIONEXTENSION
        // 第一个 source 的 SESSIONEXTENSION
        Element instance1 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='1']");
        Element instance2 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='2']");
        Element instance3 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='3']");
        Element instance4 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='4']");
        Element instance18 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='18']");
        Element instance19 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='19']");
        Element instance20 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='20']");
        Element instance21 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='21']");

        Element sessionextension1 = createReaderSessionextension(instance3, instance1);
        Element sessionextension2 = createReaderSessionextension(instance4, instance2);
        Element sessionextension3 = createReaderSessionextension(instance3);
        Element sessionextension4 = createReaderSessionextension(instance4);
        session.add(sessionextension1);
        session.add(sessionextension2);
        session.add(sessionextension3);
        session.add(sessionextension4);
        session.add(createWriterSessionextension(instance18));
        session.add(createWriterSessionextension(instance19));
        session.add(createWriterSessionextension(instance20));
        session.add(createWriterSessionextension(instance21));

        // ATTRIBUTE
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
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Commit Type").addAttribute("VALUE", "SOURCE");
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

        return session;
    }

    private Element createAddSession(Element mapping) {
        // TODO
        Element session = DocumentHelper.createElement("SESSION")
                .addAttribute("DESCRIPTION", mapping.attributeValue("DESCRIPTION")).addAttribute("ISVALID", "YES")
                .addAttribute("MAPPINGNAME", mapping.attributeValue("NAME"))
                .addAttribute("NAME", infaProperty.getProperty("session.prefix", "S_") + mapping.attributeValue("NAME"))
                .addAttribute("REUSABLE", "NO").addAttribute("SORTORDER", "Binary").addAttribute("VERSIONNUMBER", "1");

        // SESSTRANSFORMATIONINST
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = mapping.elementIterator("INSTANCE"); it.hasNext();) {
            e = it.next();
            session.add(createSessTransformationInst(e));
        }
        // CONFIGREFERENCE
        session.addElement("CONFIGREFERENCE").addAttribute("REFOBJECTNAME", "default_session_config")
                .addAttribute("TYPE", "Session config");
        // SESSIONEXTENSION
        // 第一个 source 的 SESSIONEXTENSION
        Element instance1 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='1']");
        Element instance2 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='2']");
        Element instance3 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='3']");
        Element instance4 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='4']");
        Element instance15 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='15']");
        Element instance16 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='16']");
        Element instance17 = (Element) mapping.selectSingleNode("INSTANCE[@DESCRIPTION='17']");

        Element sessionextension1 = createReaderSessionextension(instance3, instance1);
        Element sessionextension2 = createReaderSessionextension(instance4, instance2);
        Element sessionextension3 = createReaderSessionextension(instance3);
        Element sessionextension4 = createReaderSessionextension(instance4);
        session.add(sessionextension1);
        session.add(sessionextension2);
        session.add(sessionextension3);
        session.add(sessionextension4);
        session.add(createWriterSessionextension(instance15));
        session.add(createWriterSessionextension(instance16));
        session.add(createWriterSessionextension(instance17));

        // ATTRIBUTE
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
        session.addElement("ATTRIBUTE").addAttribute("NAME", "Commit Type").addAttribute("VALUE", "SOURCE");
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

        return session;
    }

    // 创建MAPPING
    private Element createTruncateThenDeleteMapping(Element srcSource, Element tarSource, Element target) {
        swtNamePostfix = 1;// 每次新生成MAPPING时重置组件计数
        instanceNumber = 1;// 每次新生成MAPPING时重置instance计数
        String mapname = infaProperty.getProperty("map.prefix", "M_") + srcSource.attributeValue("NAME")
                + infaProperty.getProperty("map.suffix", "_ALL");
        Element mapping = DocumentHelper.createElement("MAPPING")
                .addAttribute("DESCRIPTION", srcSource.attributeValue("DESCRIPTION")).addAttribute("ISVALID", "YES")
                .addAttribute("NAME", mapname).addAttribute("OBJECTVERSION", "1").addAttribute("VERSIONNUMBER", "1");

        Element srcQualifier = copySourceAsQualifier(srcSource);// 开发源Source Qualifier
        Element tarQualifier = copySourceAsQualifier(tarSource);// 开发源target Qualifier
        Element expression = copyQualifierAsExpression(tarQualifier, null);

        {
            String[] addCols = getAddCols(srcSource, "SOURCEFIELD");// 3个字段
            String firstStr = ((Element) srcSource.selectSingleNode("SOURCEFIELD[@FIELDNUMBER=\"1\"]"))
                    .attributeValue("NAME");
            // 需要将expression中的字段改成对应的值
            ((Element) expression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[0] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", firstStr)
                    .addAttribute("PORTTYPE", "OUTPUT");
            ((Element) expression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[1] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "SYSDATE")
                    .addAttribute("PORTTYPE", "OUTPUT");
            ((Element) expression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[2] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "1")
                    .addAttribute("PORTTYPE", "OUTPUT");
        }
        Element srcSourceInstance = createSourceInstance(srcSource);
        Element srcQualifierInstance = createQualifierInstance(srcQualifier, srcSource);
        Element expressionInstance = createTransformInstance(expression);
        Element targetInstance = createTargetInstance(target, "");

        ArrayList<Element> connectors = new ArrayList<>();
        connectors.addAll(createConnector(srcSourceInstance, srcSource, srcQualifierInstance, srcQualifier));
        connectors.addAll(createConnector(srcQualifierInstance, srcQualifier, expressionInstance, expression));
        connectors.addAll(createConnector(expressionInstance, expression, targetInstance, target));

        mapping.add(srcQualifier);
        mapping.add(expression);
        mapping.add(srcSourceInstance);
        mapping.add(srcQualifierInstance);
        mapping.add(expressionInstance);
        mapping.add(targetInstance);
        // connector加到mapping下
        for (Iterator<Element> it = connectors.iterator(); it.hasNext();) {
            // System.out.println(it.next().asXML());
            mapping.add(it.next());
        }

        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                targetInstance.attributeValue("NAME"));

        // variable
        String mappingVariables = infaProperty.getProperty("mapping.variable");
        if ((null != mappingVariables) && (!"".equals(mappingVariables))) {
            ArrayList<Element> variables = createMappingVariables(mappingVariables);
            for (Iterator<Element> it = variables.iterator(); it.hasNext();) {
                mapping.add(it.next());
            }
        }
        mapping.addElement("ERPINFO");

        return mapping;
    }

    private Element createAddMapping(Element srcSource, Element tarSource, Element target)
            throws NoPrimaryKeyException, UnsupportedDatatypeException, SQLException {
        swtNamePostfix = 1;// 每次新生成MAPPING时重置组件计数
        instanceNumber = 1;// 每次新生成MAPPING时重置instance计数
        String[] addCols = getAddCols(srcSource, "SOURCEFIELD");// 3个字段

        String mapname = infaProperty.getProperty("map.prefix", "M_") + srcSource.attributeValue("NAME")
                + infaProperty.getProperty("map.suffix", "_INC");
        Element mapping = DocumentHelper.createElement("MAPPING")
                .addAttribute("DESCRIPTION", srcSource.attributeValue("DESCRIPTION")).addAttribute("ISVALID", "YES")
                .addAttribute("NAME", mapname).addAttribute("OBJECTVERSION", "1").addAttribute("VERSIONNUMBER", "1");

        Element srcQualifier = copySourceAsQualifier(srcSource);// 开发源Source Qualifier

        Element tarQualifier = copySourceAsQualifier(tarSource);// 开发目标Source Qualifier
        tarQualifier.addAttribute("NAME", tarQualifier.attributeValue("NAME") + "1");// 防止名称冲突

        {

            String sqlFilter = infaProperty.getProperty("sql.filter");
            if (null != sqlFilter && !"".equals(sqlFilter)) {
                ((Element) srcQualifier.selectSingleNode("TABLEATTRIBUTE[@NAME='Source Filter']")).addAttribute("VALUE",
                        infaProperty.getProperty("sql.filter"));// Source Filter的查询SQL
                ((Element) tarQualifier.selectSingleNode("TABLEATTRIBUTE[@NAME='Source Filter']")).addAttribute("VALUE",
                        infaProperty.getProperty("sql.filter"));// Source Filter的查询SQL
            }
        }
        Element crc32Expressionfield = createCRC32Expressionfield(srcQualifier);
        Element srcExpression = copyQualifierAsExpression(srcQualifier, crc32Expressionfield);// 开发源Expression
        Element tarExpression = copyQualifierAsExpression(tarQualifier, crc32Expressionfield);// 开发目标Expression
        Element srcSorter = copyExpressionAsSorter(srcExpression, srcSource);// 开发源Sorter
        Element tarSorter = copyExpressionAsSorter(tarExpression, tarSource);// 开发目标Sorter
        Element joiner = copySorterAsJoiner(srcSorter, tarSorter);// 开发目标Joiner
        Element router = copyJoinerAsRouter(joiner, srcSource);// 开发目标router
        Element insertExpression = copyQualifierAsExpression(tarQualifier, null);// 开发目标insertExpression

        String tableName = srcSource.attributeValue("NAME");
        String pkStr = getPrimarykey(owner, tableName);
        {

            // 需要将insertExpression中的字段改成对应的值
            ((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[0] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", pkStr)
                    .addAttribute("PORTTYPE", "OUTPUT");
            ((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[1] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "SYSDATE")
                    .addAttribute("PORTTYPE", "OUTPUT");
            ((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[2] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "1")
                    .addAttribute("PORTTYPE", "OUTPUT");
        }
        Element deleteStrategy = copyTargetAsDeleteStrategy(target, "DELETE");// 开发deleteUpdateStrategy

        Element updateStrategy = copySourceAsUpdateStrategy(srcSource, "UPDATE");// 开发updateStrategy
        Element updateExpression = copyQualifierAsExpression(tarQualifier, null);// 开发updateInsertExpression
        {
            // 需要将updateInsertExpression中的增加拉链字段对应的值
            ((Element) updateExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[0] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", pkStr)
                    .addAttribute("PORTTYPE", "OUTPUT");
            ((Element) updateExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[1] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "sysdate")
                    .addAttribute("PORTTYPE", "OUTPUT");
            ((Element) updateExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + addCols[2] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "2")
                    .addAttribute("PORTTYPE", "OUTPUT");
        }

        Element srcSourceInstance = createSourceInstance(srcSource);
        Element tarSourceInstance = createSourceInstance(tarSource);
        Element srcQualifierInstance = createQualifierInstance(srcQualifier, srcSource);
        Element tarQualifierInstance = createQualifierInstance(tarQualifier, tarSource);
        Element srcExpressionInstance = createTransformInstance(srcExpression);
        Element tarExpressionInstance = createTransformInstance(tarExpression);
        Element srcSorterInstance = createTransformInstance(srcSorter);
        Element tarSorterInstance = createTransformInstance(tarSorter);
        Element joinerInstance = createTransformInstance(joiner);
        Element routerInstance = createTransformInstance(router);
        Element insertExpressionInstance = createTransformInstance(insertExpression);

        Element deleteStrategyInstance = createTransformInstance(deleteStrategy);

        Element updateInsertStrategyInstance = createTransformInstance(updateStrategy);
        Element updateInsertExpressionInstance = createTransformInstance(updateExpression);

        Element targetInsertInstance = createTargetInstance(target, "INSERT_");
        Element targetDeleteInstance = createTargetInstance(target, "DELETE_");
        Element targetUpdateInstance = createTargetInstance(target, "UPDATE_");

        ArrayList<Element> connectors = new ArrayList<>();
        connectors.addAll(createConnector(srcSourceInstance, srcSource, srcQualifierInstance, srcQualifier));
        connectors.addAll(createConnector(tarSourceInstance, tarSource, tarQualifierInstance, tarQualifier));
        connectors.addAll(createConnector(srcQualifierInstance, srcQualifier, srcExpressionInstance, srcExpression));
        connectors.addAll(createConnector(tarQualifierInstance, tarQualifier, tarExpressionInstance, tarExpression));
        connectors.addAll(createConnector(srcExpressionInstance, srcExpression, srcSorterInstance, srcSorter));
        connectors.addAll(createConnector(tarExpressionInstance, tarExpression, tarSorterInstance, tarSorter));
        connectors.addAll(createToJoinerConnector(srcSorterInstance, srcSorter, joinerInstance, joiner, "_SRC"));
        connectors.addAll(createToJoinerConnector(tarSorterInstance, tarSorter, joinerInstance, joiner, "_TAR"));
        connectors.addAll(createConnector(joinerInstance, joiner, routerInstance, router));

        // insert Connector
        connectors.addAll(
                createRouterToConnector(routerInstance, router, insertExpressionInstance, insertExpression, "_SRC_I"));
        connectors.addAll(createConnector(insertExpressionInstance, insertExpression, targetInsertInstance, target));

        // delete Connector
        connectors.addAll(
                createRouterToConnector(routerInstance, router, deleteStrategyInstance, deleteStrategy, "_TAR_D"));
        connectors.addAll(createConnector(deleteStrategyInstance, deleteStrategy, targetDeleteInstance, target));
        // update Connector

        connectors.addAll(createRouterToConnector(routerInstance, router, updateInsertStrategyInstance, updateStrategy,
                "_SRC_U"));

        connectors.addAll(createConnector(updateInsertStrategyInstance, updateStrategy, updateInsertExpressionInstance,
                updateExpression));

        connectors.addAll(
                createConnector(updateInsertExpressionInstance, updateExpression, targetUpdateInstance, target));

        mapping.add(srcQualifier);// Source Qualifier加到mapping下
        mapping.add(tarQualifier);// Source Qualifier加到mapping下
        mapping.add(srcExpression);// 源EXPTRANS加到mapping下
        mapping.add(tarExpression);// 目标EXPTRANS加到mapping下
        mapping.add(srcSorter);// 源Sorter加到mapping下
        mapping.add(tarSorter);// 目标Sorter加到mapping下
        mapping.add(joiner);// joiner加到mapping下
        mapping.add(router);// router加到mapping下
        mapping.add(insertExpression);// insertExpression加到mapping下
        mapping.add(deleteStrategy);// deleteUpdateStrategy加到mapping下
        mapping.add(updateStrategy);// updateInsertStrategy加到mapping下
        mapping.add(updateExpression);// updateInsertExpression加到mapping下
        mapping.add(srcSourceInstance);// srcSourceInstance加到mapping下
        mapping.add(tarSourceInstance);// tarSourceInstance加到mapping下
        mapping.add(srcQualifierInstance);// srcQualifierInstance加到mapping下
        mapping.add(tarQualifierInstance);// tarQualifierInstance加到mapping下
        mapping.add(srcExpressionInstance);
        mapping.add(tarExpressionInstance);
        mapping.add(srcSorterInstance);
        mapping.add(tarSorterInstance);
        mapping.add(joinerInstance);
        mapping.add(routerInstance);
        mapping.add(insertExpressionInstance);
        mapping.add(deleteStrategyInstance);
        mapping.add(updateInsertStrategyInstance);
        mapping.add(updateInsertExpressionInstance);
        mapping.add(targetInsertInstance);
        mapping.add(targetDeleteInstance);
        mapping.add(targetUpdateInstance);

        // connector加到mapping下
        for (Iterator<Element> it = connectors.iterator(); it.hasNext();) {
            // System.out.println(it.next().asXML());
            mapping.add(it.next());
        }

        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                targetInsertInstance.attributeValue("NAME"));
        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                targetDeleteInstance.attributeValue("NAME"));

        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                targetUpdateInstance.attributeValue("NAME"));

        // variable
        String mappingVariables = infaProperty.getProperty("mapping.variable");
        if ((null != mappingVariables) && (!"".equals(mappingVariables))) {
            ArrayList<Element> variables = createMappingVariables(mappingVariables);
            for (Iterator<Element> it = variables.iterator(); it.hasNext();) {
                mapping.add(it.next());
            }
        }
        mapping.addElement("ERPINFO");
        return mapping;
    }

    private Element createZipperMapping(Element srcSource, Element tarSource, Element target) {
        swtNamePostfix = 1;// 每次新生成MAPPING时重置组件计数
        instanceNumber = 1;// 每次新生成MAPPING时重置instance计数
        String[] zipperCols = getZipperCols(srcSource, "SOURCEFIELD");// 拉链表的三个字段

        String mapname = infaProperty.getProperty("map.prefix", "M_") + srcSource.attributeValue("NAME")
                + infaProperty.getProperty("map.suffix", "_INC");
        Element mapping = DocumentHelper.createElement("MAPPING")
                .addAttribute("DESCRIPTION", srcSource.attributeValue("DESCRIPTION")).addAttribute("ISVALID", "YES")
                .addAttribute("NAME", mapname).addAttribute("OBJECTVERSION", "1").addAttribute("VERSIONNUMBER", "1");

        Element srcQualifier = copySourceAsQualifier(srcSource);// 开发源Source Qualifier

        Element tarQualifier = copySourceAsQualifier(tarSource);// 开发目标Source Qualifier
        tarQualifier.addAttribute("NAME", tarQualifier.attributeValue("NAME") + "1");// 防止名称冲突

        {

            String sqlFilter = infaProperty.getProperty("sql.filter");
            if (null != sqlFilter && !"".equals(sqlFilter)) {
                ((Element) srcQualifier.selectSingleNode("TABLEATTRIBUTE[@NAME='Source Filter']")).addAttribute("VALUE",
                        infaProperty.getProperty("sql.filter"));// Source Filter的查询SQL
                ((Element) tarQualifier.selectSingleNode("TABLEATTRIBUTE[@NAME='Source Filter']")).addAttribute("VALUE",
                        zipperCols[2] + "!='3' AND " + infaProperty.getProperty("sql.filter"));// Source Filter的查询SQL
            } else {
                ((Element) tarQualifier.selectSingleNode("TABLEATTRIBUTE[@NAME='Source Filter']")).addAttribute("VALUE",
                        zipperCols[2] + "!='3'");// Source Filter的查询SQL
            }
        }
        Element crc32Expressionfield = createCRC32Expressionfield(srcQualifier);
        Element srcExpression = copyQualifierAsExpression(srcQualifier, crc32Expressionfield);// 开发源Expression
        Element tarExpression = copyQualifierAsExpression(tarQualifier, crc32Expressionfield);// 开发目标Expression
        Element srcSorter = copyExpressionAsSorter(srcExpression, srcSource);// 开发源Sorter
        Element tarSorter = copyExpressionAsSorter(tarExpression, tarSource);// 开发目标Sorter
        Element joiner = copySorterAsJoiner(srcSorter, tarSorter);// 开发目标Joiner
        Element router = copyJoinerAsRouter(joiner, srcSource);// 开发目标router
        Element insertExpression = copyQualifierAsExpression(tarQualifier, null);// 开发目标insertExpression
        {
            // 需要将insertExpression中的拉链字段改成对应的值
            ((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + zipperCols[0] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "SYSDATE")
                    .addAttribute("PORTTYPE", "OUTPUT");
            ((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + zipperCols[1] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "").addAttribute("PORTTYPE", "OUTPUT");
            ((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + zipperCols[2] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "1")
                    .addAttribute("PORTTYPE", "OUTPUT");
        }
        Element deleteStrategy = copySourceAsDeleteStrategy(tarSource, "DELETE");// 开发deleteUpdateStrategy
        Element deleteExpression = copyQualifierAsExpression(deleteStrategy, null);// 开发目标deleteExpression
        {
            // 需要将deleteExpression中的增加拉链字段对应的值
            deleteExpression.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", "date/time")
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                    .addAttribute("EXPRESSION", "SYSDATE").addAttribute("EXPRESSIONTYPE", "GENERAL")
                    .addAttribute("NAME", zipperCols[1]).addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", "19").addAttribute("SCALE", "0");
            deleteExpression.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", "nstring")
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", "3")
                    .addAttribute("EXPRESSIONTYPE", "GENERAL").addAttribute("NAME", zipperCols[2])
                    .addAttribute("PICTURETEXT", "").addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", "1")
                    .addAttribute("SCALE", "0");
        }
        Element updateDeleteStrategy = copySourceAsDeleteStrategy(tarSource, "UPDATE_DELETE");// 开发updateUpdateStrategy
        Element updateDeleteExpression = copyQualifierAsExpression(updateDeleteStrategy, null);// 开发目标updateUpdateExpression
        {
            // 需要将updateUpdateExpression中的增加拉链字段对应的值
            updateDeleteExpression.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", "date/time")
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
                    .addAttribute("EXPRESSION", "SYSDATE").addAttribute("EXPRESSIONTYPE", "GENERAL")
                    .addAttribute("NAME", zipperCols[1]).addAttribute("PICTURETEXT", "")
                    .addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", "19").addAttribute("SCALE", "0");
            updateDeleteExpression.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", "nstring")
                    .addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", "3")
                    .addAttribute("EXPRESSIONTYPE", "GENERAL").addAttribute("NAME", zipperCols[2])
                    .addAttribute("PICTURETEXT", "").addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", "1")
                    .addAttribute("SCALE", "0");
        }
        Element updateInsertStrategy = copySourceAsUpdateStrategy(srcSource, "UPDATE_INSERT");// 开发updateInsertStrategy
        {
            // 修改策略为DD_INSERT
            ((Element) updateInsertStrategy.selectSingleNode("TABLEATTRIBUTE[@NAME='Update Strategy Expression']"))
                    .addAttribute("VALUE", "DD_INSERT");
        }

        Element updateInsertExpression = copyQualifierAsExpression(tarQualifier, null);// 开发updateInsertExpression
        {
            // 需要将updateInsertExpression中的增加拉链字段对应的值
            ((Element) updateInsertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + zipperCols[0] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "SYSDATE")
                    .addAttribute("PORTTYPE", "OUTPUT");
            ((Element) updateInsertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + zipperCols[1] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "").addAttribute("PORTTYPE", "OUTPUT");
            ((Element) updateInsertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='" + zipperCols[2] + "']"))
                    .addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "2")
                    .addAttribute("PORTTYPE", "OUTPUT");
        }

        Element srcSourceInstance = createSourceInstance(srcSource);
        Element tarSourceInstance = createSourceInstance(tarSource);
        Element srcQualifierInstance = createQualifierInstance(srcQualifier, srcSource);
        Element tarQualifierInstance = createQualifierInstance(tarQualifier, tarSource);
        Element srcExpressionInstance = createTransformInstance(srcExpression);
        Element tarExpressionInstance = createTransformInstance(tarExpression);
        Element srcSorterInstance = createTransformInstance(srcSorter);
        Element tarSorterInstance = createTransformInstance(tarSorter);
        Element joinerInstance = createTransformInstance(joiner);
        Element routerInstance = createTransformInstance(router);
        Element insertExpressionInstance = createTransformInstance(insertExpression);

        Element deleteStrategyInstance = createTransformInstance(deleteStrategy);
        Element deleteExpressionInstance = createTransformInstance(deleteExpression);

        Element updateDeleteStrategyInstance = createTransformInstance(updateDeleteStrategy);
        Element updateDeleteExpressionInstance = createTransformInstance(updateDeleteExpression);
        Element updateInsertStrategyInstance = createTransformInstance(updateInsertStrategy);
        Element updateInsertExpressionInstance = createTransformInstance(updateInsertExpression);

        Element targetInsertInstance = createTargetInstance(target, "INSERT_");
        Element targetDeleteInstance = createTargetInstance(target, "DELETE_");
        Element targetUpdateDeleteInstance = createTargetInstance(target, "UPDATE_DELETE_");
        Element targetUpdateInsertInstance = createTargetInstance(target, "UPDATE_INSERT_");

        ArrayList<Element> connectors = new ArrayList<>();
        connectors.addAll(createConnector(srcSourceInstance, srcSource, srcQualifierInstance, srcQualifier));
        connectors.addAll(createConnector(tarSourceInstance, tarSource, tarQualifierInstance, tarQualifier));
        connectors.addAll(createConnector(srcQualifierInstance, srcQualifier, srcExpressionInstance, srcExpression));
        connectors.addAll(createConnector(tarQualifierInstance, tarQualifier, tarExpressionInstance, tarExpression));
        connectors.addAll(createConnector(srcExpressionInstance, srcExpression, srcSorterInstance, srcSorter));
        connectors.addAll(createConnector(tarExpressionInstance, tarExpression, tarSorterInstance, tarSorter));
        connectors.addAll(createToJoinerConnector(srcSorterInstance, srcSorter, joinerInstance, joiner, "_SRC"));
        connectors.addAll(createToJoinerConnector(tarSorterInstance, tarSorter, joinerInstance, joiner, "_TAR"));
        connectors.addAll(createConnector(joinerInstance, joiner, routerInstance, router));

        // insert Connector
        connectors.addAll(
                createRouterToConnector(routerInstance, router, insertExpressionInstance, insertExpression, "_SRC_I"));
        connectors.addAll(createConnector(insertExpressionInstance, insertExpression, targetInsertInstance, target));

        // delete Connector
        connectors.addAll(
                createRouterToConnector(routerInstance, router, deleteStrategyInstance, deleteStrategy, "_TAR_D"));
        connectors.addAll(
                createConnector(deleteStrategyInstance, deleteStrategy, deleteExpressionInstance, deleteExpression));
        connectors.addAll(createConnector(deleteExpressionInstance, deleteExpression, targetDeleteInstance, target));
        // update Connector
        connectors.addAll(createRouterToConnector(routerInstance, router, updateDeleteStrategyInstance,
                updateDeleteStrategy, "_TAR_U"));
        connectors.addAll(createRouterToConnector(routerInstance, router, updateInsertStrategyInstance,
                updateInsertStrategy, "_SRC_U"));

        connectors.addAll(createConnector(updateDeleteStrategyInstance, updateDeleteStrategy,
                updateDeleteExpressionInstance, updateDeleteExpression));
        connectors.addAll(createConnector(updateInsertStrategyInstance, updateInsertStrategy,
                updateInsertExpressionInstance, updateInsertExpression));
        connectors.addAll(createConnector(updateDeleteExpressionInstance, updateDeleteExpression,
                targetUpdateDeleteInstance, target));
        connectors.addAll(createConnector(updateInsertExpressionInstance, updateInsertExpression,
                targetUpdateInsertInstance, target));

        mapping.add(srcQualifier);// Source Qualifier加到mapping下
        mapping.add(tarQualifier);// Source Qualifier加到mapping下
        mapping.add(srcExpression);// 源EXPTRANS加到mapping下
        mapping.add(tarExpression);// 目标EXPTRANS加到mapping下
        mapping.add(srcSorter);// 源Sorter加到mapping下
        mapping.add(tarSorter);// 目标Sorter加到mapping下
        mapping.add(joiner);// joiner加到mapping下
        mapping.add(router);// router加到mapping下
        mapping.add(insertExpression);// insertExpression加到mapping下
        mapping.add(deleteStrategy);// deleteUpdateStrategy加到mapping下
        mapping.add(deleteExpression);// deleteExpression加到mapping下
        mapping.add(updateDeleteStrategy);// updateDeleteStrategy加到mapping下
        mapping.add(updateDeleteExpression);// updateDeleteExpression加到mapping下
        mapping.add(updateInsertStrategy);// updateInsertStrategy加到mapping下
        mapping.add(updateInsertExpression);// updateInsertExpression加到mapping下
        mapping.add(srcSourceInstance);// srcSourceInstance加到mapping下
        mapping.add(tarSourceInstance);// tarSourceInstance加到mapping下
        mapping.add(srcQualifierInstance);// srcQualifierInstance加到mapping下
        mapping.add(tarQualifierInstance);// tarQualifierInstance加到mapping下
        mapping.add(srcExpressionInstance);
        mapping.add(tarExpressionInstance);
        mapping.add(srcSorterInstance);
        mapping.add(tarSorterInstance);
        mapping.add(joinerInstance);
        mapping.add(routerInstance);
        mapping.add(insertExpressionInstance);
        mapping.add(deleteStrategyInstance);
        mapping.add(deleteExpressionInstance);
        mapping.add(updateDeleteStrategyInstance);
        mapping.add(updateDeleteExpressionInstance);
        mapping.add(updateInsertStrategyInstance);
        mapping.add(updateInsertExpressionInstance);
        mapping.add(targetInsertInstance);
        mapping.add(targetDeleteInstance);
        mapping.add(targetUpdateDeleteInstance);
        mapping.add(targetUpdateInsertInstance);

        // connector加到mapping下
        for (Iterator<Element> it = connectors.iterator(); it.hasNext();) {
            // System.out.println(it.next().asXML());
            mapping.add(it.next());
        }

        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                targetInsertInstance.attributeValue("NAME"));
        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                targetDeleteInstance.attributeValue("NAME"));
        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                targetUpdateDeleteInstance.attributeValue("NAME"));
        mapping.addElement("TARGETLOADORDER").addAttribute("ORDER", "1").addAttribute("TARGETINSTANCE",
                targetUpdateInsertInstance.attributeValue("NAME"));

        // variable
        String mappingVariables = infaProperty.getProperty("mapping.variable");
        if ((null != mappingVariables) && (!"".equals(mappingVariables))) {
            ArrayList<Element> variables = createMappingVariables(mappingVariables);
            for (Iterator<Element> it = variables.iterator(); it.hasNext();) {
                mapping.add(it.next());
            }
        }
        mapping.addElement("ERPINFO");
        return mapping;
    }

    // 切换生成的XML文件
    public void xmlFileChange(int fileCount) {

        String fileName = infaProperty.getProperty("work.dir")
                + infaProperty.getProperty("xml.output", "gen.xml").toLowerCase().replace(".xml", fileCount + ".xml");
        // 初始化XML文件打印格式
        OutputFormat format = new OutputFormat("    ", true);
        format.setEncoding("GBK");
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
    }

    public void xmlFileWrite(String path) {
        String v_path = "//" + path;
        try {
            Element folder = ((Element) doc.selectSingleNode("//FOLDER"));
            if (folder.nodeCount() == 0) {
                return;
            }
            xmlWriter.write(doc.selectSingleNode(v_path));
            xmlWriter.flush();
        } catch (IOException e) {
            log.error("444"+e.getMessage());
        }
    }

    public void makeZipperXML(String tableName)
            throws NoPrimaryKeyException, CheckTableExistException, UnsupportedDatatypeException, SQLException {
        String username = infaProperty.getProperty("source.username").toUpperCase();
        String sourceTablename;
        String targetTablename;

        // 处理表名
        if (tableName.contains(".")) {
            owner = tableName.split("\\.")[0];
            sourceTablename = infaProperty.getProperty("source.prefix", "").toUpperCase() + tableName.split("\\.")[1];
            targetTablename = infaProperty.getProperty("target.prefix", "").toUpperCase() + tableName.split("\\.")[1];

        } else {
            owner = infaProperty.getProperty("source.owner", username).toUpperCase();
            sourceTablename = infaProperty.getProperty("source.prefix", "").toUpperCase() + tableName;
            targetTablename = infaProperty.getProperty("target.prefix", "").toUpperCase() + tableName;
        }

        // 表名长度大于30时
        if (sourceTablename.length() > 30) {
            sourceTablename = sourceTablename.substring(0, 30);
        }

        if (targetTablename.length() > 30) {
            targetTablename = targetTablename.substring(0, 30);
        }

        // 导入源表
        Element srcSource = createSourceWithPrimaryKey(owner, sourceTablename);

        // 导入目标表的源（注意复制后需要重命名为目标表）
        Element tarSource = copySourceAsZipperSource(srcSource);
        tarSource.attribute("NAME").setValue(targetTablename);
        // 复制目标表的目标
        Element target = copySourceAsTarget(tarSource);

        // 开发mapping
        Element mapping = createZipperMapping(srcSource, tarSource, target);
        // 设置CONFIG
        // Element sessionConfiguration = createSessionConfiguration();
        // 开发work flow
        Element workflow = createWorkflow(mapping);

        Element folder = ((Element) doc.selectSingleNode("//FOLDER"));
        folder.add(srcSource);// srcSource加到folder下，
        folder.add(tarSource);// 第二个源加到folder下, tar source加到folder下
        folder.add(target);// target加到folder下
        folder.add(mapping);// mapping加到folder下
        // folder.add(sessionConfiguration);// sessionConfiguration加到folder下

        // 是否多个配置文件生成多个工作流？
        String[] mutiParams = infaProperty.getProperty("workflow.muti-params", "-1").replaceAll("\n", "").split(";");
        if (!"-1".equals(mutiParams[0])) {
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
                ((Element) e.selectSingleNode("ATTRIBUTE[@NAME='Parameter Filename']")).addAttribute("VALUE", wfParam);
                folder.add(e);// workflow加到folder下
            }
        } else {
            folder.add(workflow);// workflow加到folder下
        }

    }

    public void makeAddXML(String tableName)
            throws NoPrimaryKeyException, CheckTableExistException, UnsupportedDatatypeException, SQLException {
        String username = infaProperty.getProperty("source.username").toUpperCase();
        String sourceTablename;
        String targetTablename;

        // 处理表名
        if (tableName.contains(".")) {
            owner = tableName.split("\\.")[0];
            sourceTablename = infaProperty.getProperty("source.prefix", "").toUpperCase() + tableName.split("\\.")[1];
            targetTablename = infaProperty.getProperty("target.prefix", "").toUpperCase() + tableName.split("\\.")[1];

        } else {
            owner = infaProperty.getProperty("source.owner", username).toUpperCase();
            sourceTablename = infaProperty.getProperty("source.prefix", "").toUpperCase() + tableName;
            targetTablename = infaProperty.getProperty("target.prefix", "").toUpperCase() + tableName;
        }

        // 表名长度大于30时
        if (sourceTablename.length() > 30) {
            sourceTablename = sourceTablename.substring(0, 30);
        }

        if (targetTablename.length() > 30) {
            targetTablename = targetTablename.substring(0, 30);
        }

        // 导入源表
        Element srcSource = createSourceWithPrimaryKey(owner, sourceTablename);
        // 导入目标表的源（注意复制后需要重命名为目标表）
        Element tarSource = copySourceAsAddSource(srcSource);

        // try {
        // OutputFormat format = new OutputFormat(" ", true);
        // new XMLWriter(format).write((tarSource));
        // } catch (IOException e) {
        //
        // e.printStackTrace();
        // }
        tarSource.attribute("NAME").setValue(targetTablename);
        // 复制目标表的目标
        Element target = copySourceAsTarget(tarSource);
        // 目标表主键设置
        Element e;
        for (@SuppressWarnings("unchecked") Iterator<Element> it = target.elementIterator(); it.hasNext();) {
            e = it.next();
            e.addAttribute("KEYTYPE", "NOT A KEY");
        }
        ((Element) target.selectSingleNode("TARGETFIELD[@NAME='HY_ID']")).addAttribute("KEYTYPE", "PRIMARY KEY");

        // 开发mapping
        Element mapping = createAddMapping(srcSource, tarSource, target);
        // 设置CONFIG
        // Element sessionConfiguration = createSessionConfiguration();
        // 开发work flow
        Element workflow = createWorkflow(mapping);

        Element folder = ((Element) doc.selectSingleNode("//FOLDER"));
        folder.add(srcSource);// srcSource加到folder下，
        folder.add(tarSource);// 第二个源加到folder下, tar source加到folder下
        folder.add(target);// target加到folder下
        folder.add(mapping);// mapping加到folder下
        // folder.add(sessionConfiguration);// sessionConfiguration加到folder下
        // 是否多个配置文件生成多个工作流？
        String[] mutiParams = infaProperty.getProperty("workflow.muti-params", "-1").replaceAll("\n", "").split(";");
        if (!"-1".equals(mutiParams[0])) {
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
                ((Element) e.selectSingleNode("ATTRIBUTE[@NAME='Parameter Filename']")).addAttribute("VALUE", wfParam);
                folder.add(e);// workflow加到folder下
            }
        } else {
            folder.add(workflow);// workflow加到folder下
        }

    }

    public void makeTruncateThenDeleteXML(String tableName)
            throws NoPrimaryKeyException, CheckTableExistException, UnsupportedDatatypeException, SQLException {
        // 获取需要生成的表名和所在用户参数
        String username = infaProperty.getProperty("source.username").toUpperCase();
        String sourceTablename;
        String targetTablename;

        // 处理表名
        if (tableName.contains(".")) {
            owner = tableName.split("\\.")[0];
            sourceTablename = infaProperty.getProperty("source.prefix", "").toUpperCase() + tableName.split("\\.")[1];
            targetTablename = infaProperty.getProperty("target.prefix", "").toUpperCase() + tableName.split("\\.")[1];

        } else {
            owner = infaProperty.getProperty("source.owner", username).toUpperCase();
            sourceTablename = infaProperty.getProperty("source.prefix", "").toUpperCase() + tableName;
            targetTablename = infaProperty.getProperty("target.prefix", "").toUpperCase() + tableName;
        }

        // 表名长度大于30时
        if (sourceTablename.length() > 30) {
            sourceTablename = sourceTablename.substring(0, 30);
        }

        if (targetTablename.length() > 30) {
            targetTablename = targetTablename.substring(0, 30);
        }
        // 导入源表
        Element srcSource = createSource(owner, sourceTablename);
        Element tarSource = copySourceAsAddSource(srcSource);

        // 复制源作为目标
        Element target = copySourceAsTarget(tarSource);
        target.attribute("NAME").setValue(targetTablename);

        // 开发mapping
        Element mapping = createTruncateThenDeleteMapping(srcSource, tarSource, target);
        // 设置CONFIG
        // Element sessionConfiguration = createSessionConfiguration();
        // 开发work flow
        Element workflow = createWorkflow(mapping);

        Element folder = ((Element) doc.selectSingleNode("//FOLDER"));
        folder.add(srcSource);// srcSource加到folder下，
        folder.add(target);// target加到folder下
        folder.add(mapping);// mapping加到folder下
        // folder.add(sessionConfiguration);// sessionConfiguration加到folder下
        // 是否多个配置文件生成多个工作流？
        String[] mutiParams = infaProperty.getProperty("workflow.muti-params", "-1").replaceAll("\n", "").split(";");
        if (!"-1".equals(mutiParams[0])) {
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
                ((Element) e.selectSingleNode("ATTRIBUTE[@NAME='Parameter Filename']")).addAttribute("VALUE", wfParam);
                folder.add(e);// workflow加到folder下
            }
        } else {
            folder.add(workflow);// workflow加到folder下
        }
    }

}
