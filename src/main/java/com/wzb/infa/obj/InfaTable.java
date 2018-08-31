package com.wzb.infa.obj;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.wzb.infa.dbutils.DbUtil;
import com.wzb.infa.exceptions.CheckTableExistException;
import com.wzb.infa.exceptions.DuplicateColumnExceptiion;
import com.wzb.infa.exceptions.NoPrimaryKeyException;
import com.wzb.infa.exceptions.UnsupportedDatatypeException;

/**
 * InfaTable ，oracle原始表对象，从数据库获取对象的相关信息后，提供SOURCE等infa的对象
 * 
 */
public class InfaTable {

	/***
	 * 创建一个target对象
	 */
	public Element createTarget(String targetName, boolean createWithPrimaryKey) {
		Element target = DocumentHelper.createElement("TARGET").addAttribute("BUSINESSNAME", "")
				.addAttribute("DATABASETYPE", "Oracle").addAttribute("DESCRIPTION", "").addAttribute("NAME", targetName)
				.addAttribute("OBJECTVERSION", "1").addAttribute("VERSIONNUMBER", "1").addAttribute("CONSTRAINT", "")
				.addAttribute("TABLEOPTIONS", "");
		Element targetField;
		for (InfaCol col : cols) {
			targetField = col.createTargetField(createWithPrimaryKey);
			target.add(targetField);
		}

		return target;
	}

	/***
	 * 创建一个updateStrategy对象
	 */
	public Element createUpdateStrategy(String updateStrategyName, String updateStrategyValue, boolean primaryKeyOnly,
			InfaCol addCol) {
		Element updateStrategy = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", updateStrategyName).addAttribute("OBJECTVERSION", "1")
				.addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Update Strategy")
				.addAttribute("VERSIONNUMBER", "1");
		Element updateStrategyField;
		for (InfaCol col : cols) {
			if ("NOT A KEY".equals(col.getKeyType()) && primaryKeyOnly) {
				continue;
			}
			updateStrategyField = col.createUpdateStrategyField();
			updateStrategy.add(updateStrategyField);
		}
		if (addCol != null) {
			updateStrategy.add(addCol.createUpdateStrategyField());
		}
		// 默认TABLEATTRIBUTE
		updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Update Strategy Expression")
				.addAttribute("VALUE", updateStrategyValue);
		updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Forward Rejected Rows").addAttribute("VALUE",
				"YES");
		updateStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE",
				"Normal");
		return updateStrategy;
	}

	/***
	 * 创建一个updateStrategy对象
	 */
	public Element createUpdateStrategy(InfaCol pkcol) {
		Element deleteStrategy = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", "DEL_" + this.tableName).addAttribute("OBJECTVERSION", "1")
				.addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Update Strategy")
				.addAttribute("VERSIONNUMBER", "1");
		Element updateStrategyField;
		updateStrategyField = pkcol.createUpdateStrategyField();
		deleteStrategy.add(updateStrategyField);
		// 默认TABLEATTRIBUTE
		deleteStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Update Strategy Expression")
				.addAttribute("VALUE", "DD_DELETE");
		deleteStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Forward Rejected Rows").addAttribute("VALUE",
				"YES");
		deleteStrategy.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE",
				"Normal");
		return deleteStrategy;
	}

	/***
	 * 创建一个router对象
	 */
	public Element createRouter(Element joiner, Element source) {

		Element router = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", "RTR_" + this.tableName).addAttribute("OBJECTVERSION", "1")
				.addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Router").addAttribute("VERSIONNUMBER", "1");

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

	/***
	 * 创建一个Joiner对象
	 */
	public Element createJoiner(InfaTable joinTable) {

		Element joiner = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", "JNR_" + this.tableName).addAttribute("OBJECTVERSION", "1")
				.addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Joiner").addAttribute("VERSIONNUMBER", "1");

		Element joinerField;
		StringBuilder joinCondition = new StringBuilder();
		for (InfaCol col : cols) {
			joinerField = col.createJoinerField("_SRC", "INPUT/OUTPUT/MASTER");
			joiner.add(joinerField);
			if ("PRIMARY KEY".equals(col.getKeyType())) {
				joinCondition.append(col.getColumnName());
				joinCondition.append("_SRC");
				joinCondition.append(" = ");
				joinCondition.append(col.getColumnName());
				joinCondition.append("_TAR");
				joinCondition.append(" AND ");
			}

		}
		for (InfaCol col : joinTable.cols) {
			joinerField = col.createJoinerField("_TAR", "INPUT/OUTPUT");
			joiner.add(joinerField);
		}

		// 默认TABLEATTRIBUTE
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

	/***
	 * 创建一个Joiner对象
	 */
	public Element createJoiner(InfaTable joinTable, InfaCol addCol) {

		Element joiner = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", "JNR_" + this.tableName).addAttribute("OBJECTVERSION", "1")
				.addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Joiner").addAttribute("VERSIONNUMBER", "1");

		Element joinerField;
		StringBuilder joinCondition = new StringBuilder();
		for (InfaCol col : cols) {
			joinerField = col.createJoinerField("_SRC", "INPUT/OUTPUT/MASTER");
			joiner.add(joinerField);
			if ("PRIMARY KEY".equals(col.getKeyType())) {
				joinCondition.append(col.getColumnName());
				joinCondition.append("_SRC");
				joinCondition.append(" = ");
				joinCondition.append(col.getColumnName());
				joinCondition.append("_TAR");
				joinCondition.append(" AND ");
			}

		}
		joiner.add(addCol.createJoinerField("_SRC", "INPUT/OUTPUT/MASTER"));
		for (InfaCol col : joinTable.cols) {
			joinerField = col.createJoinerField("_TAR", "INPUT/OUTPUT");
			joiner.add(joinerField);
		}
		joiner.add(addCol.createJoinerField("_TAR", "INPUT/OUTPUT"));

		// 默认TABLEATTRIBUTE
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

	/***
	 * 创建一个Sorter对象
	 */
	public Element createSorter(String name) {

		Element sorter = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", name).addAttribute("OBJECTVERSION", "1").addAttribute("REUSABLE", "NO")
				.addAttribute("TYPE", "Sorter").addAttribute("VERSIONNUMBER", "1");

		Element sorterField;
		for (InfaCol col : cols) {
			sorterField = col.createSorterField();
			sorter.add(sorterField);
		}
		// 默认TABLEATTRIBUTE
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

	/***
	 * 创建一个Sorter对象
	 */
	public Element createSorter(String name, InfaCol addCol) {

		Element sorter = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", name).addAttribute("OBJECTVERSION", "1").addAttribute("REUSABLE", "NO")
				.addAttribute("TYPE", "Sorter").addAttribute("VERSIONNUMBER", "1");

		Element sorterField;
		for (InfaCol col : cols) {
			sorterField = col.createSorterField();
			sorter.add(sorterField);
		}
		sorter.add(addCol.createSorterField());
		// 默认TABLEATTRIBUTE
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

	/***
	 * 创建一个Expression对象
	 */
	public Element createExpression(String expressionName, boolean primaryKeyOnly) {

		Element expression = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", expressionName).addAttribute("OBJECTVERSION", "1").addAttribute("REUSABLE", "NO")
				.addAttribute("TYPE", "Expression").addAttribute("VERSIONNUMBER", "1");

		Element expressionField;
		for (InfaCol col : cols) {
			if ("NOT A KEY".equals(col.getKeyType()) && primaryKeyOnly) {
				continue;
			}
			expressionField = col.createExpressionField();
			expression.add(expressionField);
		}

		return expression;

	}

	/***
	 * 创建一个Expression对象
	 */
	public Element createExpression(String expressionName, InfaCol addCol, String colExpresion) {

		Element expression = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", expressionName).addAttribute("OBJECTVERSION", "1").addAttribute("REUSABLE", "NO")
				.addAttribute("TYPE", "Expression").addAttribute("VERSIONNUMBER", "1");

		Element expressionField;
		for (InfaCol col : cols) {
			expressionField = col.createExpressionField();
			expression.add(expressionField);
		}
		if (addCol != null)
			expression.add(addCol.createExpressionField(colExpresion));
		return expression;

	}

	/***
	 * 创建一个SourceQualifier对象
	 */
	public Element createSourceQualifier(String sourceQualifierName, String sqlFilter) {

		Element sourceQualifier = DocumentHelper.createElement("TRANSFORMATION").addAttribute("DESCRIPTION", "")
				.addAttribute("NAME", sourceQualifierName).addAttribute("OBJECTVERSION", "1")
				.addAttribute("REUSABLE", "NO").addAttribute("TYPE", "Source Qualifier")
				.addAttribute("VERSIONNUMBER", "1");

		Element sourceQualifierField;
		for (InfaCol col : cols) {
			sourceQualifierField = col.createQualifierField();
			sourceQualifier.add(sourceQualifierField);
		}

		// 默认TABLEATTRIBUTE
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Sql Query").addAttribute("VALUE", "");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "User Defined Join").addAttribute("VALUE",
				"");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Source Filter").addAttribute("VALUE",
				sqlFilter);
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Number Of Sorted Ports")
				.addAttribute("VALUE", "0");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Tracing Level").addAttribute("VALUE",
				"Normal");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Select Distinct").addAttribute("VALUE",
				"NO");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Is Partitionable").addAttribute("VALUE",
				"NO");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Pre SQL").addAttribute("VALUE", "");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Post SQL").addAttribute("VALUE", "");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Output is deterministic")
				.addAttribute("VALUE", "No");
		sourceQualifier.addElement("TABLEATTRIBUTE").addAttribute("NAME", "Output is repeatable").addAttribute("VALUE",
				"Never");
		return sourceQualifier;

	}

	/***
	 * 从头开始，创建一个source对象
	 */
	public Element createSource(String sourceName, String dbName) throws UnsupportedDatatypeException {

		Element source = DocumentHelper.createElement("SOURCE").addAttribute("BUSINESSNAME", "")
				.addAttribute("DATABASETYPE", "Oracle").addAttribute("DBDNAME", dbName)
				.addAttribute("DESCRIPTION", comment).addAttribute("NAME", tableName).addAttribute("OBJECTVERSION", "1")
				.addAttribute("OWNERNAME", owner).addAttribute("VERSIONNUMBER", "1");

		int prePhysicalLength = 0;
		int prePhysicalOffset = 0;
		int preLength = 0;
		int preOffset = 0;
		Element sourceField;
		for (InfaCol col : cols) {

			sourceField = col.createSourceField(prePhysicalLength, prePhysicalOffset, preLength, preOffset);
			source.add(sourceField);
			prePhysicalOffset = prePhysicalLength + prePhysicalOffset;
			preOffset = preLength + preOffset;
			prePhysicalLength = Integer.parseInt(col.getPhysicalLength());
			preLength = Integer.parseInt(col.getStringLength());

		}

		return source;

	}

	/***
	 * 以下代码是成员变量及其getter和setter
	 */
	public static Logger logger = Logger.getLogger(InfaTable.class);
	private String owner;
	private String tableName;
	private String comment;
	private boolean hasPk = false;
	private ArrayList<InfaCol> cols = new ArrayList<>();

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getName() {
		return tableName;
	}

	public void setName(String name) {
		this.tableName = name;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public ArrayList<InfaCol> getCols() {
		return cols;
	}

	public void setCols(ArrayList<InfaCol> cols) {
		this.cols = cols;
	}

	public boolean isHasPk() {
		return hasPk;
	}

	public void setHasPk(boolean hasPk) {
		this.hasPk = hasPk;
	}

	/*****
	 * 调用dbUtil，传入用户、表名构造InfaTable
	 * 
	 * @throws SQLException
	 * @throws CheckTableExistException
	 */
	public InfaTable(String owner, String tableName) throws SQLException, CheckTableExistException {
		super();
		logger.debug("begin InfaTable:" + tableName);
		this.owner = owner;
		this.tableName = tableName;
		InfaCol col;
		DbUtil dbUtil = DbUtil.getInstance();
		if (!dbUtil.isTabExist(owner, tableName)) {
			throw new CheckTableExistException(owner + "." + tableName + " not exist!!!");
		}
		this.comment = dbUtil.getTabComments(owner, tableName);

		ResultSet rsCols = dbUtil.getTabCols(owner, tableName);

		String columnName;
		String dataType;
		String dataLength;
		String dataPrecision;
		String dataScale;
		String nullable;
		String columnId;
		while (rsCols.next()) {
			columnName = rsCols.getString("NAME");
			dataType = rsCols.getString("DATATYPE");
			dataLength = rsCols.getString("LENGTH");
			dataPrecision = rsCols.getString("PRECISION");
			dataScale = rsCols.getString("SCALE");
			nullable = rsCols.getString("NULLABLE");
			columnId = rsCols.getString("FIELDNUMBER");
			col = new InfaCol(columnName, "", dataType, dataLength, dataPrecision, dataScale, nullable, columnId,
					"NOT A KEY");
			this.cols.add(col);

		}

		ResultSet rsPrimaryCols = dbUtil.getPrimaryCols(owner, tableName);

		while (rsPrimaryCols.next()) {
			logger.debug("find primary key:" + tableName);
			hasPk = true;
			columnName = rsPrimaryCols.getString("COLUMN_NAME");
			for (InfaCol ketCol : this.cols) {
				if (columnName.equals(ketCol.getColumnName())) {
					ketCol.setKeyType("PRIMARY KEY");
				}
			}
		}

		logger.debug("end InfaTable:" + tableName);
	}

	// 无参构造函数隐藏
	private InfaTable() {
	}

	@SuppressWarnings("unchecked")
	public InfaTable copy(String targetName) {
		InfaTable infaTable = new InfaTable();
		infaTable.owner = this.owner;
		infaTable.tableName = targetName;
		infaTable.comment = this.comment;
		infaTable.hasPk = this.hasPk;

		// infaTable.Cols只需要复制一份本身，底层的col无需复制多份，使用浅复制即可
		infaTable.cols = (ArrayList<InfaCol>) this.cols.clone();

		return infaTable;
	}

	public String getCRC32String() {
		StringBuilder sb = new StringBuilder();
		sb.append("CRC32(");
		for (InfaCol col : this.cols) {
			try {
				if (col.getDataType().equals("timestamp") || col.getDataType().equals("date")) {
					sb.append("to_char(");
					sb.append(col.getColumnName());
					sb.append(",'YYYYMMDDHH24MISS')||");
				} else if (col.getDataType().equals("number(p,s)")) {
					sb.append("to_char(");
					sb.append(col.getColumnName());
					sb.append(")||");
				} else if (col.getDataType().contains("lob") || col.getDataType().contains("raw")
						|| col.getDataType().contains("long")) {

				} else {
					sb.append(col.getColumnName());
					sb.append("||");
				}

			} catch (UnsupportedDatatypeException e) {
				e.printStackTrace();
			}
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append(")");
		return sb.toString();
	}

	public String getPkString() throws NoPrimaryKeyException {
		StringBuilder sb = new StringBuilder();
		for (InfaCol col : this.cols) {
			if ("NOT A KEY".equals(col.getKeyType())) {
				continue;
			}
			try {
				if (col.getDataType().equals("timestamp") || col.getDataType().equals("date")) {
					sb.append("to_char(");
					sb.append(col.getColumnName());
					sb.append(",'YYYYMMDDHH24MISS')||'_'||");
				} else if (col.getDataType().equals("number(p,s)")) {
					sb.append("to_char(");
					sb.append(col.getColumnName());
					sb.append(")||'_'||");
				} else if (col.getDataType().contains("lob") || col.getDataType().contains("raw")
						|| col.getDataType().contains("long")) {

				} else {
					sb.append(col.getColumnName());
					sb.append("||'_'||");
				}

			} catch (UnsupportedDatatypeException e) {
				logger.error("UnsupportedDatatypeException:" + e.getMessage());
			}
		}
		if (sb.length() == 0) {
			throw new NoPrimaryKeyException(this.owner + "." + this.tableName + " NoPrimaryKey!");
		}
		sb.delete(sb.length() - 7, sb.length());
		return sb.toString();
	}

	public boolean hasCol(String colname) {
		for (InfaCol col : this.cols) {
			if (colname.equals(col.getColumnName())) {
				return true;
			}
		}
		return false;
	}

	public InfaTable addCol(InfaCol colToAdd) throws DuplicateColumnExceptiion {

		for (InfaCol col : this.cols) {
			if (col.getColumnName().equals(colToAdd.getColumnName())) {
				throw new DuplicateColumnExceptiion(this.tableName + "." + colToAdd.getColumnName());
			}
		}
		this.cols.add(colToAdd);
		return this;
	}
}
