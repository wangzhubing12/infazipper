package com.wzb.infa.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.properties.InfaProperty;

/***
 * 创建各种INFA对象组件类型如Source Qualifier，会创建对应的字段 创建各种内置的对象如TRANSFORMATION类型时，不含对应字段
 * 
 */

public class InfaUtils {

	private Logger log = Logger.getLogger(InfaUtils.class);
	private InfaProperty infaProperty = InfaProperty.getInstance();
	private int swtNamePostfix = 1; // EXPRESSION等组件后缀序列
	private int instanceNumber = 1; // mapping中instance个数顺序，放在description里面，建立sessionInstance时可以对应设置参数
	private final static HashMap<String, String> dataTypeO2I = new HashMap<String, String>() {
		/**
		 * 数据类型对应，将oracle的数据类型转对应成INFA的数据类型 此处写的不全，新的类型需要不断添加进来
		 */
		private static final long serialVersionUID = 1L;
		{
			put("blob", "binary");
			put("date", "date/time");
			put("long", "text");
			put("long row", "binary");
			put("nchar", "nstring");
			put("char", "string");
			put("nclob", "ntext");
			put("number", "double");
			put("number(p,s)", "decimal");
			put("nvarchar2", "nstring");
			put("row", "binary");
			put("timestamp", "date/time");
			put("varchar", "string");
			put("varchar2", "string");
			put("xmltype", "text");
		}
	};

	public boolean checkTableExist(Connection connect, String owner, String table) throws CheckTableExistException {
		PreparedStatement pstm = null;
		ResultSet rs = null;
		String sql = "select count(*) from all_tables a where a.owner = ? and a.table_name= ?";
		try {
			pstm = connect.prepareStatement(sql);
			pstm.setString(1, owner);
			pstm.setString(2, table);
			rs = pstm.executeQuery();
			rs.next();
			int res = rs.getInt(1);
			if (res == 1) {
				return true;
			} else {
				throw new CheckTableExistException(owner + "." + table + " not exist !");
			}
		} catch (SQLException e) {
			throw new CheckTableExistException(owner + "." + table + " check exist error!");
		} finally {
			try {
				rs.close();
				pstm.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}

	public Element importSource(Connection connect, String owner, String table) throws NoPrimaryKeyException {

		StringBuilder sqlsb = new StringBuilder();
		sqlsb.append("SELECT A.TABLE_NAME, A.COLUMN_NAME NAME,\n");
		sqlsb.append("       LOWER(DECODE(A.DATA_TYPE, 'NUMBER', 'NUMBER(P,S)', A.DATA_TYPE)) DATATYPE,\n");
		sqlsb.append("       TO_CHAR(DECODE(A.DATA_TYPE, 'DATE', '19', 'NUMBER',\n");
		sqlsb.append("                       nvl(A.DATA_PRECISION, 0), 0)) LENGTH,\n");
		sqlsb.append("       TO_CHAR(DECODE(A.DATA_TYPE, 'DATE', '19', 'NUMBER',\n");
		sqlsb.append("                       NVL(A.DATA_PRECISION, A.DATA_LENGTH), A.DATA_LENGTH)) PRECISION,\n");
		sqlsb.append("       TO_CHAR(NVL(A.DATA_SCALE, '0')) SCALE,\n");
		sqlsb.append("       DECODE(A.NULLABLE, 'N', 'NOTNULL', 'Y', 'NULL') NULLABLE,\n");
		sqlsb.append("       A.COLUMN_ID FIELDNUMBER,\n");
		sqlsb.append("       NVL(SUM(DECODE(A.DATA_TYPE, 'DATE', 19,\n");
		sqlsb.append("                       NVL(A.DATA_PRECISION, A.DATA_LENGTH)))\n");
		sqlsb.append("            OVER(ORDER BY A.COLUMN_ID ROWS BETWEEN UNBOUNDED PRECEDING AND 1\n");
		sqlsb.append("                 PRECEDING), 0) PHYSICALOFFSET,\n");
		sqlsb.append("       NVL(SUM(DECODE(A.DATA_TYPE, 'DATE', '19', 'NUMBER', A.DATA_PRECISION,\n");
		sqlsb.append("                       '0'))\n");
		sqlsb.append("            OVER(ORDER BY A.COLUMN_ID ROWS BETWEEN UNBOUNDED PRECEDING AND 1\n");
		sqlsb.append("                 PRECEDING), '0') OFFSET,\n");
		sqlsb.append("       TO_CHAR(DECODE(A.DATA_TYPE, 'DATE', '19', 'NUMBER',\n");
		sqlsb.append("                       NvL(A.DATA_PRECISION, 0), A.DATA_LENGTH)) PHYSICALLENGTH,\n");
		sqlsb.append("       NVL2((SELECT C.COLUMN_NAME\n");
		sqlsb.append("               FROM ALL_CONSTRAINTS B, ALL_CONS_COLUMNS C\n");
		sqlsb.append("              WHERE B.TABLE_NAME = A.TABLE_NAME\n");
		sqlsb.append("                AND B.TABLE_NAME = C.TABLE_NAME\n");
		sqlsb.append("                AND B.OWNER = C.OWNER\n");
		sqlsb.append("                AND B.CONSTRAINT_NAME = C.CONSTRAINT_NAME\n");
		sqlsb.append("                AND B.CONSTRAINT_TYPE = 'P'\n");
		sqlsb.append("                AND A.COLUMN_NAME = C.COLUMN_NAME\n");
		sqlsb.append("                AND A.OWNER = C.OWNER), 'PRIMARY KEY', 'NOT A KEY') KEYTYPE,\n");
		sqlsb.append("       (select b.COMMENTS\n");
		sqlsb.append("           from all_col_comments b\n");
		sqlsb.append("          where a.OWNER = b.OWNER\n");
		sqlsb.append("            and a.TABLE_NAME = b.TABLE_NAME\n");
		sqlsb.append("            and a.COLUMN_NAME = b.COLUMN_NAME) COMMENTS\n");
		sqlsb.append("  FROM ALL_TAB_COLUMNS A\n");
		sqlsb.append(" WHERE A.TABLE_NAME = ?\n");
		sqlsb.append("   AND A.OWNER = ?\n");
		sqlsb.append(" ORDER BY 8");
		String sql = sqlsb.toString();
		Element el = DocumentHelper.createElement("SOURCE").addAttribute("BUSINESSNAME", "")
				.addAttribute("DATABASETYPE", "Oracle")
				.addAttribute("DBDNAME", infaProperty.getProperty("source.dbname", "SOURCEDB"))
				.addAttribute("DESCRIPTION", getTableComent(connect, owner, table)).addAttribute("NAME", table)
				.addAttribute("OBJECTVERSION", "1").addAttribute("OWNERNAME", owner).addAttribute("VERSIONNUMBER", "1");
		try {
			PreparedStatement pstm = connect.prepareStatement(sql);
			pstm.setString(1, table);
			pstm.setString(2, owner);
			ResultSet rs = pstm.executeQuery();
			boolean hasPrimaryKey = false;
			while (rs.next()) {
				hasPrimaryKey = hasPrimaryKey || "PRIMARY KEY".equals(rs.getString("KEYTYPE"));
				el.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "")
						.addAttribute("DATATYPE", rs.getString("DATATYPE"))
						.addAttribute("DESCRIPTION", rs.getString("COMMENTS"))
						.addAttribute("FIELDNUMBER", rs.getString("FIELDNUMBER")).addAttribute("FIELDPROPERTY", "0")
						.addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO")
						.addAttribute("KEYTYPE", rs.getString("KEYTYPE")).addAttribute("LENGTH", rs.getString("LENGTH"))
						.addAttribute("LEVEL", "0").addAttribute("NAME", rs.getString("NAME"))
						.addAttribute("NULLABLE", rs.getString("NULLABLE")).addAttribute("OCCURS", "0")
						.addAttribute("OFFSET", rs.getString("OFFSET"))
						.addAttribute("PHYSICALLENGTH", rs.getString("PHYSICALLENGTH"))
						.addAttribute("PHYSICALOFFSET", rs.getString("PHYSICALOFFSET")).addAttribute("PICTURETEXT", "")
						.addAttribute("PRECISION", rs.getString("PRECISION"))
						.addAttribute("SCALE", rs.getString("SCALE")).addAttribute("USAGE_FLAGS", "");
			}
			if (!hasPrimaryKey) {
				throw new NoPrimaryKeyException(" no PrimaryKey");
			}
			return el;
		} catch (SQLException e) {
			log.error(sql);
			throw new NoPrimaryKeyException(e.getMessage());
		}
	}

	public String getTableComent(Connection connect, String owner, String table) {

		String sql = "select a.COMMENTS from all_tab_comments a where a.owner= ? and a.table_name= ?";
		try {
			PreparedStatement pstm = connect.prepareStatement(sql);
			pstm.setString(1, table);
			pstm.setString(2, owner);
			ResultSet rs = pstm.executeQuery();
			String res = null;
			while (rs.next()) {
				res = rs.getString(1);
			}
			rs.close();
			pstm.close();
			if (res != null) {
				return res;
			}
		} catch (SQLException e) {
			log.error(sql);
			log.error(e.getMessage());
		}
		return "";
	}

	public Element copySourceAsZipperSource(Element srcSource) {
		// 计算出下一个SOURCEFIELD字段的PHYSICALOFFSET值,为本SOURCEFIELD的PHYSICALOFFSET+PHYSICALLENGTH
		Element lastField = (Element) srcSource.selectSingleNode("SOURCEFIELD[last()]");
		String PHYSICALOFFSET = lastField.attribute("PHYSICALOFFSET").getValue();
		String PHYSICALLENGTH = lastField.attribute("PHYSICALLENGTH").getValue();
		Integer fieldNumber = Integer.parseInt(lastField.attribute("FIELDNUMBER").getValue()) + 1;
		Integer nextOffset = Integer.parseInt(PHYSICALOFFSET) + Integer.parseInt(PHYSICALLENGTH);
		// 复制源并增加拉链表的三个字段作为第二个源
		Element tarSource = srcSource.createCopy();
		tarSource.attribute("NAME")
				.setValue(infaProperty.getProperty("target.prefix") + tarSource.attribute("NAME").getValue());
		tarSource.attribute("DBDNAME").setValue(
				infaProperty.getProperty("target.dbname", "TARGETDB") + tarSource.attribute("DBDNAME").getValue());
		tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "date")
				.addAttribute("DESCRIPTION", "insert time").addAttribute("FIELDNUMBER", String.valueOf(fieldNumber))
				.addAttribute("FIELDPROPERTY", "0").addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO")
				.addAttribute("KEYTYPE", "PRIMARY KEY").addAttribute("LENGTH", "19").addAttribute("LEVEL", "0")
				.addAttribute("NAME", "QYSJ").addAttribute("NULLABLE", "NOTNULL").addAttribute("OCCURS", "0")
				.addAttribute("OFFSET", "19").addAttribute("PHYSICALLENGTH", "19")
				.addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset)).addAttribute("PICTURETEXT", "")
				.addAttribute("PRECISION", "19").addAttribute("SCALE", "0").addAttribute("USAGE_FLAGS", "");
		tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "date")
				.addAttribute("DESCRIPTION", "delete or update time")
				.addAttribute("FIELDNUMBER", String.valueOf(fieldNumber + 1)).addAttribute("FIELDPROPERTY", "0")
				.addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO").addAttribute("KEYTYPE", "NOT A KEY")
				.addAttribute("LENGTH", "19").addAttribute("LEVEL", "0").addAttribute("NAME", "SXSJ")
				.addAttribute("NULLABLE", "NULL").addAttribute("OCCURS", "0").addAttribute("OFFSET", "38")
				.addAttribute("PHYSICALLENGTH", "19").addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset + 19))
				.addAttribute("PICTURETEXT", "").addAttribute("PRECISION", "19").addAttribute("SCALE", "0")
				.addAttribute("USAGE_FLAGS", "");
		tarSource.addElement("SOURCEFIELD").addAttribute("BUSINESSNAME", "").addAttribute("DATATYPE", "varchar2")
				.addAttribute("DESCRIPTION", "data status").addAttribute("FIELDNUMBER", String.valueOf(fieldNumber + 2))
				.addAttribute("FIELDPROPERTY", "0").addAttribute("FIELDTYPE", "ELEMITEM").addAttribute("HIDDEN", "NO")
				.addAttribute("KEYTYPE", "NOT A KEY").addAttribute("LENGTH", "1").addAttribute("LEVEL", "0")
				.addAttribute("NAME", "JLZT").addAttribute("NULLABLE", "NULL").addAttribute("OCCURS", "0")
				.addAttribute("OFFSET", "57").addAttribute("PHYSICALLENGTH", "1")
				.addAttribute("PHYSICALOFFSET", String.valueOf(nextOffset + 38)).addAttribute("PICTURETEXT", "")
				.addAttribute("PRECISION", "1").addAttribute("SCALE", "0").addAttribute("USAGE_FLAGS", "");
		return tarSource;
	}

	@SuppressWarnings("unchecked")
	public Element copySourceAsTarget(Element tarSource) {
		Element target = tarSource.createCopy();
		target.setName("TARGET");
		target.addAttribute("CONSTRAINT", "");
		target.addAttribute("TABLEOPTIONS", "");
		target.remove(target.attribute("DBDNAME"));
		target.remove(target.attribute("OWNERNAME"));

		Element e = null;
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

	public Element createZipperMapping(Element srcSource, Element tarSource, Element target) {
		swtNamePostfix = 1;// 每次新生成MAPPING时重置组件计数
		instanceNumber = 1;// 每次新生成MAPPING时重置instance计数
		String mapname = infaProperty.getProperty("map.prefix") + srcSource.attributeValue("NAME")
				+ infaProperty.getProperty("map.postfix");
		Element mapping = DocumentHelper.createElement("MAPPING")
				.addAttribute("DESCRIPTION", srcSource.attributeValue("DESCRIPTION")).addAttribute("ISVALID", "YES")
				.addAttribute("NAME", mapname).addAttribute("OBJECTVERSION", "1").addAttribute("VERSIONNUMBER", "1");

		Element srcQualifier = copySourceAsQualifier(srcSource);// 开发源Source Qualifier

		Element tarQualifier = copySourceAsQualifier(tarSource);// 开发目标Source Qualifier
		{
			String sqlFilter = infaProperty.getProperty("sql.filter");
			if (null != sqlFilter && !"".equals(sqlFilter)) {
				((Element) srcQualifier.selectSingleNode("TABLEATTRIBUTE[@NAME='Source Filter']")).addAttribute("VALUE",
						infaProperty.getProperty("sql.filter"));// Source Filter的查询SQL
				((Element) tarQualifier.selectSingleNode("TABLEATTRIBUTE[@NAME='Source Filter']")).addAttribute("VALUE",
						"JLZT!='3' AND " + infaProperty.getProperty("sql.filter"));// Source Filter的查询SQL
			} else {
				((Element) tarQualifier.selectSingleNode("TABLEATTRIBUTE[@NAME='Source Filter']")).addAttribute("VALUE",
						"JLZT!='3'");// Source Filter的查询SQL
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
			((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='QYSJ']"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "SYSDATE")
					.addAttribute("PORTTYPE", "OUTPUT");
			((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='SXSJ']"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "").addAttribute("PORTTYPE", "OUTPUT");
			((Element) insertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='JLZT']"))
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
					.addAttribute("NAME", "SXSJ").addAttribute("PICTURETEXT", "").addAttribute("PORTTYPE", "OUTPUT")
					.addAttribute("PRECISION", "19").addAttribute("SCALE", "0");
			deleteExpression.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", "nstring")
					.addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", "3")
					.addAttribute("EXPRESSIONTYPE", "GENERAL").addAttribute("NAME", "JLZT")
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
					.addAttribute("NAME", "SXSJ").addAttribute("PICTURETEXT", "").addAttribute("PORTTYPE", "OUTPUT")
					.addAttribute("PRECISION", "19").addAttribute("SCALE", "0");
			updateDeleteExpression.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", "nstring")
					.addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("EXPRESSION", "3")
					.addAttribute("EXPRESSIONTYPE", "GENERAL").addAttribute("NAME", "JLZT")
					.addAttribute("PICTURETEXT", "").addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", "1")
					.addAttribute("SCALE", "0");
		}
		Element updateInsertStrategy = copySourceAsUpdateStrategy(tarSource);// 开发updateInsertStrategy
		Element updateInsertExpression = copyQualifierAsExpression(updateInsertStrategy, null);// 开发updateInsertExpression
		{
			// 需要将updateInsertExpression中的增加拉链字段对应的值
			((Element) updateInsertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='QYSJ']"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "SYSDATE")
					.addAttribute("PORTTYPE", "OUTPUT");
			((Element) updateInsertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='SXSJ']"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("EXPRESSION", "").addAttribute("PORTTYPE", "OUTPUT");
			((Element) updateInsertExpression.selectSingleNode("TRANSFORMFIELD[@NAME='JLZT']"))
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

		Element targetInsertInstance = createTargetInstance(target, "INSERT");
		Element targetDeleteInstance = createTargetInstance(target, "DELETE");
		Element targetUpdateDeleteInstance = createTargetInstance(target, "UPDATE_DELETE");
		Element targetUpdateInsertInstance = createTargetInstance(target, "UPDATE_INSERT");

		ArrayList<Element> connectors = new ArrayList<Element>();
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
				updateDeleteStrategy, "_SRC_U"));
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
		String mappingVariables = this.infaProperty.getProperty("mapping.variable");
		if ((null != mappingVariables) && (!"".equals(mappingVariables))) {
			ArrayList<Element> variables = createMappingVariables(mappingVariables);
			for (Iterator<Element> it = variables.iterator(); it.hasNext();) {
				mapping.add(it.next());
			}
		}
		mapping.addElement("ERPINFO");
		return mapping;
	}

	private ArrayList<Element> createMappingVariables(String mappingVariables) {
		String[] variablesSplit = mappingVariables.split(",");
		ArrayList<Element> els = new ArrayList<Element>();
		Element el = null;
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
		ArrayList<Element> connectors = new ArrayList<Element>();

		Element to = null;
		Element connector = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = toElement.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
			to = it.next();
			if (to.attributeValue("PORTTYPE").contains("INPUT")) {
				connector = createConnector(to.attributeValue("NAME") + groupPrefix, fromInstanceName, fromInstancetype,
						to.attributeValue("NAME"), toInstanceName, toInstancetype);
				// 更新时，QYSJ等字段需要从router的目标去连接，其他字段均可从源连接过来
				if (groupPrefix.contains("SRC") && ("QYSJ".equals(to.attributeValue("NAME"))
						|| "SXSJ".equals(to.attributeValue("NAME")) || "JLZT".equals(to.attributeValue("NAME")))) {
					connector.attribute("FROMFIELD")
							.setValue(connector.attributeValue("FROMFIELD").replace("_SRC_", "_TAR_"));
				}
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
		ArrayList<Element> connectors = new ArrayList<Element>();

		Element from = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = fromElement.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
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
				.addAttribute("NAME", srcQualifier.attributeValue("NAME")).addAttribute("REUSABLE", "NO")
				.addAttribute("TRANSFORMATION_NAME", srcQualifier.attributeValue("NAME"))
				.addAttribute("TRANSFORMATION_TYPE", "Source Qualifier").addAttribute("TYPE", "TRANSFORMATION");
		qualifierInstance.addElement("ASSOCIATED_SOURCE_INSTANCE").addAttribute("NAME",
				srcSource.attributeValue("NAME"));
		return qualifierInstance;
	}

	private Element createSourceInstance(Element srcSource) {
		Element sourceInstance = DocumentHelper.createElement("INSTANCE")
				.addAttribute("DBDNAME", srcSource.attributeValue("DBDNAME"))
				.addAttribute("DESCRIPTION", String.valueOf(instanceNumber++))
				.addAttribute("NAME", srcSource.attributeValue("NAME"))
				.addAttribute("TRANSFORMATION_NAME", srcSource.attributeValue("NAME"))
				.addAttribute("TRANSFORMATION_TYPE", "Source Definition").addAttribute("TYPE", "SOURCE");
		return sourceInstance;
	}

	private Element copySourceAsUpdateStrategy(Element tarSource) {
		Element updateStrategy = createTransformation("UPDATE_INSERT", "Update Strategy");
		// TRANSFORMFIELD
		Element e = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = tarSource.elementIterator("SOURCEFIELD"); it.hasNext();) {
			e = it.next();
			updateStrategy.addElement("TRANSFORMFIELD")
					.addAttribute("DATATYPE", dataTypeO2I.getOrDefault(e.attributeValue("DATATYPE"), "string"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INPUT")
					.addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
					.addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
					.addAttribute("SCALE", e.attributeValue("SCALE"));
		}
		updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Update Strategy Expression")
				.addAttribute("VALUE", "DD_INSERT");
		updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Forward Rejected Rows").addAttribute("VALUE",
				"YES");
		updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE",
				"Normal");
		return updateStrategy;
	}

	private Element copySourceAsDeleteStrategy(Element tarSource, String deleteStrategyName) {
		Element updateStrategy = createTransformation(deleteStrategyName, "Update Strategy");
		// TRANSFORMFIELD
		Element e = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = tarSource.elementIterator("SOURCEFIELD"); it.hasNext();) {
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

		Element e = null;
		// TRANSFORMFIELD INPUT
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
			e = it.next();
			router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INPUT")
					.addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
					.addAttribute("PORTTYPE", "INPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
					.addAttribute("SCALE", e.attributeValue("SCALE"));
		}
		// TRANSFORMFIELD INSERT
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
			e = it.next();
			router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "INSERT")
					.addAttribute("NAME", e.attributeValue("NAME") + "_I").addAttribute("PICTURETEXT", "")
					.addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
					.addAttribute("REF_FIELD", e.attributeValue("NAME"))
					.addAttribute("SCALE", e.attributeValue("SCALE"));
		}
		// TRANSFORMFIELD UPDATE
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
			e = it.next();
			router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "UPDATE")
					.addAttribute("NAME", e.attributeValue("NAME") + "_U").addAttribute("PICTURETEXT", "")
					.addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
					.addAttribute("REF_FIELD", e.attributeValue("NAME"))
					.addAttribute("SCALE", e.attributeValue("SCALE"));
		}
		// TRANSFORMFIELD DELETE
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
			e = it.next();
			router.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("GROUP", "DELETE")
					.addAttribute("NAME", e.attributeValue("NAME") + "_D").addAttribute("PICTURETEXT", "")
					.addAttribute("PORTTYPE", "OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
					.addAttribute("REF_FIELD", e.attributeValue("NAME"))
					.addAttribute("SCALE", e.attributeValue("SCALE"));
		}
		// TRANSFORMFIELD DEFAULT
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = joiner.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
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
		Element e = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = srcSorter.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
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

		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = tarSorter.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
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
		if (addTransformfield != null)
			exptrans.add(addTransformfield.createCopy());

		// TRANSFORMFIELD
		Element e = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = srcQualifier.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
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
		Element crc32Expressionfield = DocumentHelper.createElement("TRANSFORMFIELD")
				.addAttribute("DATATYPE", "nstring").addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "")
				.addAttribute("EXPRESSION", crc32).addAttribute("EXPRESSIONTYPE", "GENERAL")
				.addAttribute("NAME", "CRC32").addAttribute("PICTURETEXT", "").addAttribute("PORTTYPE", "OUTPUT")
				.addAttribute("PRECISION", "19").addAttribute("SCALE", "0");
		return crc32Expressionfield;
	}

	private Element copyExpressionAsSorter(Element expression, Element source) {
		Element sorter = createTransformation("SRTTRANS" + swtNamePostfix++, "Sorter");
		// TRANSFORMFIELD
		Element e = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = expression.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
			e = it.next();
			sorter.addElement("TRANSFORMFIELD").addAttribute("DATATYPE", e.attributeValue("DATATYPE"))
					.addAttribute("DEFAULTVALUE", "").addAttribute("DESCRIPTION", "").addAttribute("ISSORTKEY", "NO")
					.addAttribute("NAME", e.attributeValue("NAME")).addAttribute("PICTURETEXT", "")
					.addAttribute("PORTTYPE", "INPUT/OUTPUT").addAttribute("PRECISION", e.attributeValue("PRECISION"))
					.addAttribute("SCALE", e.attributeValue("SCALE")).addAttribute("SORTDIRECTION", "ASCENDING");
		}
		// 设置按照主键排序，从source中获取主键
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = source.elementIterator("SOURCEFIELD"); it.hasNext();) {
			e = it.next();
			if ("PRIMARY KEY".equals(e.attributeValue("KEYTYPE")))
				((Element) sorter.selectSingleNode("TRANSFORMFIELD[@NAME='" + e.attributeValue("NAME") + "']"))
						.attribute("ISSORTKEY").setValue("YES");
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
				infaProperty.getProperty("qualifier.prefix") + source.attributeValue("NAME"), "Source Qualifier");

		// TRANSFORMFIELD
		Element e = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = source.elementIterator(); it.hasNext();) {
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

	public Element createSessionConfiguration() {
		Element sessionConfiguration = DocumentHelper.createElement("CONFIG");
		return sessionConfiguration;
	}

	public Element createWorkflow(Element mapping) {
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
				.addAttribute("NAME", "启动").addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Start")
				.addAttribute("VERSIONNUMBER", "1");
		Element session = createSession(mapping);

		// TASKINSTANCE
		Element startTarskInstance = DocumentHelper.createElement("TASKINSTANCE").addAttribute("DESCRIPTION", "")
				.addAttribute("ISENABLED", "YES").addAttribute("NAME", "启动").addAttribute("REUSABLE", "NO")
				.addAttribute("TASKNAME", "启动").addAttribute("TASKTYPE", "Start");
		Element sessionInstance = DocumentHelper.createElement("TASKINSTANCE").addAttribute("DESCRIPTION", "")
				.addAttribute("FAIL_PARENT_IF_INSTANCE_DID_NOT_RUN", "NO")
				.addAttribute("FAIL_PARENT_IF_INSTANCE_FAILS", "YES").addAttribute("ISENABLED", "YES")
				.addAttribute("NAME", session.attributeValue("NAME")).addAttribute("REUSABLE", "NO")
				.addAttribute("TASKNAME", session.attributeValue("NAME")).addAttribute("TASKTYPE", "Session")
				.addAttribute("TREAT_INPUTLINK_AS_AND", "YES");
		// WORKFLOWLINK
		Element workflowLink = DocumentHelper.createElement("WORKFLOWLINK").addAttribute("CONDITION", "")
				.addAttribute("FROMTASK", "启动").addAttribute("TOTASK", session.attributeValue("NAME"));

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
				.addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$启动.StartTime")
				.addAttribute("USERDEFINED", "NO");
		workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "date/time").addAttribute("DEFAULTVALUE", "")
				.addAttribute("DESCRIPTION ", "The time this task completed").addAttribute("ISNULL", "NO")
				.addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$启动.EndTime")
				.addAttribute("USERDEFINED", "NO");
		workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
				.addAttribute("DESCRIPTION ", "Status of this task  execution").addAttribute("ISNULL", "NO")
				.addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$启动.Status")
				.addAttribute("USERDEFINED", "NO");
		workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
				.addAttribute("DESCRIPTION ", "Status of the previous task that is not disabled")
				.addAttribute("ISNULL", "NO").addAttribute("ISPERSISTENT", "NO")
				.addAttribute("NAME", "$启动.PrevTaskStatus").addAttribute("USERDEFINED", "NO");
		workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "integer").addAttribute("DEFAULTVALUE", "")
				.addAttribute("DESCRIPTION ", "Error code for this task s execution").addAttribute("ISNULL", "NO")
				.addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$启动.ErrorCode")
				.addAttribute("USERDEFINED", "NO");
		workflow.addElement("WORKFLOWVARIABLE").addAttribute("DATATYPE", "string").addAttribute("DEFAULTVALUE", "")
				.addAttribute("DESCRIPTION ", "Error message for this task s execution").addAttribute("ISNULL", "NO")
				.addAttribute("ISPERSISTENT", "NO").addAttribute("NAME", "$启动.ErrorMsg")
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
				this.infaProperty.getProperty("workflow.paramter.filename", ""));
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

	private Element createSession(Element mapping) {
		// TODO
		Element session = DocumentHelper.createElement("SESSION")
				.addAttribute("DESCRIPTION", mapping.attributeValue("DESCRIPTION")).addAttribute("ISVALID", "YES")
				.addAttribute("MAPPINGNAME", mapping.attributeValue("NAME"))
				.addAttribute("NAME", infaProperty.getProperty("session.prefix", "S_") + mapping.attributeValue("NAME"))
				.addAttribute("REUSABLE", "NO").addAttribute("SORTORDER", "Binary").addAttribute("VERSIONNUMBER", "1");

		// SESSTRANSFORMATIONINST
		Element e = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = mapping.elementIterator("INSTANCE"); it.hasNext();) {
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
		session.addElement("ATTRIBUTE").addAttribute("NAME", "Commit Type").addAttribute("VALUE", "Target");
		session.addElement("ATTRIBUTE").addAttribute("NAME", "Commit Interval").addAttribute("VALUE", "10000");
		session.addElement("ATTRIBUTE").addAttribute("NAME", "Commit On End Of File").addAttribute("VALUE", "YES");
		session.addElement("ATTRIBUTE").addAttribute("NAME", "Rollback Transactions on Errors").addAttribute("VALUE",
				"NO");
		session.addElement("ATTRIBUTE").addAttribute("NAME", "Recovery Strategy").addAttribute("VALUE",
				"Fail task and continue workflow");
		session.addElement("ATTRIBUTE").addAttribute("NAME", "Java Classpath").addAttribute("VALUE", "");
		session.addElement("ATTRIBUTE").addAttribute("NAME", "Performance").addAttribute("VALUE", "");
		session.addElement("ATTRIBUTE").addAttribute("NAME", "DTM buffer size").addAttribute("VALUE", "24000000");
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
		sessionextension.addElement("CONNECTIONREFERENCE").addAttribute("CNXREFNAME", "DB Connection")
				.addAttribute("CONNECTIONNAME", "").addAttribute("CONNECTIONNUMBER", "1")
				.addAttribute("CONNECTIONSUBTYPE", "").addAttribute("CONNECTIONTYPE", "Relational")
				.addAttribute("VARIABLE", "$DBConnectionSRC");
		return sessionextension;
	}

	private Element createSessTransformationInst(Element instance) {
		Element sessTransformationInst = DocumentHelper.createElement("SESSTRANSFORMATIONINST");
		if ("Source Definition".equals(instance.attributeValue("TRANSFORMATION_TYPE"))) {
			sessTransformationInst.addAttribute("ISREPARTITIONPOINT", "NO").addAttribute("PIPELINE", "0")
					.addAttribute("SINSTANCENAME", instance.attributeValue("NAME")).addAttribute("STAGE", "0")
					.addAttribute("TRANSFORMATIONNAME", instance.attributeValue("TRANSFORMATION_NAME"))
					.addAttribute("TRANSFORMATIONTYPE", instance.attributeValue("TRANSFORMATION_TYPE"));
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
		Element e = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = srcQualifier.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
			e = it.next();
			if ("date/time".equals(e.attributeValue("DATATYPE"))) {
				sb.append("to_char(");
				sb.append(e.attributeValue("NAME"));
				sb.append(",'YYYYMMDDHH24MISS')||");
			} else if ("nstring".equals(e.attributeValue("DATATYPE")) || "string".equals(e.attributeValue("DATATYPE"))
					|| "text".equals(e.attributeValue("DATATYPE")) || "ntext".equals(e.attributeValue("DATATYPE"))) {
				sb.append(e.attributeValue("NAME"));
				sb.append("||");
			} else {
				sb.append("to_char(");
				sb.append(e.attributeValue("NAME"));
				sb.append(")||");
			}
		}
		String crc32 = sb.substring(0, sb.length() - 2);
		return crc32;
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
		ArrayList<Element> connectors = new ArrayList<Element>();

		Element from = null;
		Element to = null;
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = fromElement.elementIterator("SOURCEFIELD"); it.hasNext();) {
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
		for (@SuppressWarnings("unchecked")
		Iterator<Element> it = fromElement.elementIterator("TRANSFORMFIELD"); it.hasNext();) {
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
}
