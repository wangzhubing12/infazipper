package com.wzb.infa.dbutils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.wzb.infa.properties.InfaProperty;

//保存Connection、SQL，以及各个SQL的STAMETN对象、单例模式1
public class DbUtil {

	public ResultSet getTabCols(String owner, String table) throws SQLException {
		pstmTabCols.setString(1, table);
		pstmTabCols.setString(2, owner);
		return pstmTabCols.executeQuery();
	}

	public ResultSet getPrimaryCols(String owner, String table) throws SQLException {
		pstmPrimaryCols.setString(1, owner);
		pstmPrimaryCols.setString(2, table);
		pstmPrimaryCols.setString(3, owner);
		pstmPrimaryCols.setString(4, table);
		return pstmPrimaryCols.executeQuery();
	}

	public String getTabComments(String owner, String table) throws SQLException {
		pstmTabComments.setString(1, owner);
		pstmTabComments.setString(2, table);
		String res;
		try (ResultSet rs = pstmTabComments.executeQuery()) {
			res = null;
			while (rs.next()) {
				res = rs.getString(1);
			}
		}
		if (res != null) {
			return res;
		} else {
			logger.warn(owner + "." + table + "[No Comments]");
			return "";
		}
	}

	public boolean isTabExist(String owner, String table) throws SQLException {
		pstmTabExist.setString(1, owner);
		pstmTabExist.setString(2, table);
		int res;
		try (ResultSet rs = pstmTabExist.executeQuery()) {
			rs.next();
			res = rs.getInt(1);
		}
		return res == 1;
	}

	public void release() {
		if (pstmTabCols != null) {
			try {
				pstmTabCols.close();
			} catch (SQLException e) {
			}
		}
		if (pstmPrimaryCols != null) {
			try {
				pstmPrimaryCols.close();
			} catch (SQLException e) {
			}
		}
		if (pstmTabComments != null) {
			try {
				pstmTabComments.close();
			} catch (SQLException e) {
			}
		}
		if (pstmTabExist != null) {
			try {
				pstmTabExist.close();
			} catch (SQLException e) {
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
	}

	public static Logger logger = Logger.getLogger(DbUtil.class);
	public static DbUtil dbUtil = null;
	private Connection conn = null;
	private InfaProperty infaProperty = null;
	private PreparedStatement pstmTabCols = null;
	private PreparedStatement pstmPrimaryCols = null;
	private PreparedStatement pstmTabComments = null;
	private PreparedStatement pstmTabExist = null;

	public static DbUtil getInstance() {
		if (dbUtil == null) {
			dbUtil = new DbUtil();
		}
		return dbUtil;
	}

	private DbUtil() {
		try {
			infaProperty = InfaProperty.getInstance();
			Class.forName(infaProperty.getProperty("source.driver"));
			String url = infaProperty.getProperty("source.url");
			String username = infaProperty.getProperty("source.username");
			String password = infaProperty.getProperty("source.password");
			conn = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException | SQLException e) {
			logger.error(e.getMessage());

		}
		StringBuilder sqlTabCols = new StringBuilder();

		// 初始化SQL语句
		sqlTabCols.append("SELECT A.TABLE_NAME,\n");
		sqlTabCols.append("       A.COLUMN_NAME NAME,\n");
		sqlTabCols.append("       LOWER(A.DATA_TYPE) DATATYPE,\n");
		sqlTabCols.append("       TO_CHAR(NVL(a.DATA_LENGTH, 0)) LENGTH,\n");
		sqlTabCols.append("       TO_CHAR(NVL(A.DATA_PRECISION, 0)) PRECISION,\n");
		sqlTabCols.append("       TO_CHAR(NVL(A.DATA_SCALE, '0')) SCALE,\n");
		sqlTabCols.append("       DECODE(A.NULLABLE, 'N', 'NOTNULL', 'Y', 'NULL') NULLABLE,\n");
		sqlTabCols.append("       A.COLUMN_ID FIELDNUMBER\n");
		sqlTabCols.append("  FROM ");
		sqlTabCols.append(infaProperty.getProperty("dictionary.tabcolumns", "ALL_TAB_COLUMNS"));
		sqlTabCols.append(" A\n");
		sqlTabCols.append(" WHERE A.TABLE_NAME = ?\n");
		sqlTabCols.append("   AND A.OWNER = ?\n");
		sqlTabCols.append(" ORDER BY 8");
		StringBuilder sqlPrimaryCols = new StringBuilder();

		// sqlPrimaryCols.append("SELECT C.COLUMN_NAME\n");
		// sqlPrimaryCols.append(" FROM ");
		// sqlPrimaryCols.append(infaProperty.getProperty("dictionary.constraints",
		// "ALL_CONSTRAINTS"));
		// sqlPrimaryCols.append(" B, ");
		// sqlPrimaryCols.append(infaProperty.getProperty("dictionary.conscolumns",
		// "ALL_CONS_COLUMNS"));
		// sqlPrimaryCols.append(" C\n");
		// sqlPrimaryCols.append(" WHERE B.TABLE_NAME = ?\n");
		// sqlPrimaryCols.append(" AND C.TABLE_NAME = ?\n");
		// sqlPrimaryCols.append(" AND B.OWNER = ?\n");
		// sqlPrimaryCols.append(" AND C.OWNER = ?\n");
		// sqlPrimaryCols.append(" AND B.CONSTRAINT_NAME =
		// C.CONSTRAINT_NAME\n");
		// sqlPrimaryCols.append(" AND B.CONSTRAINT_TYPE = 'P'\n");
		sqlPrimaryCols.append("SELECT B.COLUMN_NAME\n");
		sqlPrimaryCols.append("  FROM ");
		sqlPrimaryCols.append(infaProperty.getProperty("dictionary.indcolumns", "ALL_IND_COLUMNS"));
		sqlPrimaryCols.append(" B\n");
		sqlPrimaryCols.append(" WHERE B.TABLE_OWNER = ?\n");
		sqlPrimaryCols.append("   AND B.TABLE_NAME = ?\n");
		sqlPrimaryCols.append(
				"   AND (CASE WHEN INSTR(B.INDEX_NAME,'PK')> 0 THEN 'ZZ'||B.INDEX_NAME ELSE B.INDEX_NAME END) = (SELECT MAX(CASE WHEN INSTR(A.INDEX_NAME,'PK')> 0 THEN 'ZZ'||A.INDEX_NAME ELSE A.INDEX_NAME END)\n");
		sqlPrimaryCols.append("                         FROM ");
		sqlPrimaryCols.append(infaProperty.getProperty("dictionary.indexes", "ALL_INDEXES"));
		sqlPrimaryCols.append(" A\n");
		sqlPrimaryCols.append("                        WHERE A.TABLE_OWNER = ?\n");
		sqlPrimaryCols.append("                          AND A.TABLE_NAME = ?\n");
		sqlPrimaryCols.append("                          AND A.UNIQUENESS = 'UNIQUE')");

		String sqlTabComments = "SELECT A.COMMENTS FROM "
				+ infaProperty.getProperty("dictionary.tabcomments", "ALL_TAB_COMMENTS")
				+ " A WHERE A.OWNER= ? AND A.TABLE_NAME= ?";

		String sqlTabExist = "SELECT COUNT(*) FROM " + infaProperty.getProperty("dictionary.tables", "ALL_TABLES")
				+ " A WHERE A.OWNER = ? AND A.TABLE_NAME= ?";

		try {
			pstmTabCols = conn.prepareStatement(sqlTabCols.toString());
			pstmPrimaryCols = conn.prepareStatement(sqlPrimaryCols.toString());
			pstmTabComments = conn.prepareStatement(sqlTabComments);
			pstmTabExist = conn.prepareStatement(sqlTabExist);

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

	}

}
