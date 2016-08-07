package org.wowtools.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;
import org.wowtools.common.utils.ResourcesReader;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * jdbc工具类
 * 
 * @author liuyu
 *
 */
public class JdbcUtil {

	private static JdbcUtil defaultInstance;

	private static final HashMap<String, JdbcUtil> instances = new HashMap<String, JdbcUtil>(1);

	private ComboPooledDataSource dataSource;

	private String dataSourceName;

	@FunctionalInterface
	public static interface JdbcResultVisitor {
		default public void afterNoNext() {
		}

		public void visit(ResultSet rs) throws Exception;
		
		default public void visitWithException(ResultSet rs){
			try {
				visit(rs);
			} catch (SQLException e) {
				throw new DaoRuntimeException(e);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
	};

	public synchronized static JdbcUtil getDefaultInstance() {
		if (null == defaultInstance) {
			defaultInstance = getOrInitInstance("jdbccfg-default.json");
		}
		return defaultInstance;
	}

	public synchronized static JdbcUtil getOrInitInstance(String cfgPath) {
		String cfg = ResourcesReader.read(JdbcUtil.class, "jdbccfg-default.json");
		JSONObject jsonCfg = new JSONObject(cfg);
		return getOrInitInstance(jsonCfg);
	}

	public synchronized static JdbcUtil getOrInitInstance(JSONObject jsonCfg) {
		String dataSourceName = jsonCfg.getString("dataSourceName");
		JdbcUtil old = instances.get(dataSourceName);
		if (null != old) {
			return old;
		}
		return new JdbcUtil(jsonCfg, dataSourceName);
	}

	/**
	 * 
	 * @param jsonCfg
	 *            json格式的配置信息
	 */
	private JdbcUtil(JSONObject jsonCfg, String dataSourceName) {
		try {
			dataSource = new ComboPooledDataSource();
			dataSource.setDataSourceName(dataSourceName);
			dataSource.setUser(jsonCfg.getString("user"));
			dataSource.setPassword(jsonCfg.getString("password"));
			dataSource.setJdbcUrl(jsonCfg.getString("jdbcUrl"));
			dataSource.setDriverClass(jsonCfg.getString("driverClass"));
			dataSource.setMinPoolSize(jsonCfg.getInt("minPoolSize"));
			dataSource.setMaxPoolSize(jsonCfg.getInt("maxPoolSize"));

			try {
				dataSource.setInitialPoolSize(jsonCfg.getInt("initialPoolSize"));
			} catch (Exception e) {
			}
			try {
				dataSource.setMaxStatements(jsonCfg.getInt("maxStatements"));
			} catch (Exception e) {
			}
			try {
				dataSource.setMaxIdleTime(jsonCfg.getInt("maxIdleTime"));
			} catch (Exception e) {
			}
		} catch (Exception e) {
			throw new DaoRuntimeException("初始化JdbcUtil异常", e);
		}
	}

	public int[] batchExecute(String sql, List<Object[]> batchArgs) {
		Connection conn = getConnection();
		PreparedStatement pstm = null;
		try {
			conn.setAutoCommit(false);
			pstm = conn.prepareStatement(sql);
			for (Object[] args : batchArgs) {
				int i = 1;
				for (Object arg : args) {
					pstm.setObject(i, arg);
					i++;
				}
				pstm.addBatch();
			}
			int[] res = pstm.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			return res;
		} catch (Exception e) {
			throw new DaoRuntimeException(e);
		} finally {
			if (null != pstm) {
				try {
					pstm.close();
				} catch (Exception e) {
				}
			}
			if (null != conn) {
				try {
					releaseConnection(conn);
				} catch (Exception e) {
				}
			}
		}
	}

	public int execute(String sql, Object... args) {
		Connection conn = getConnection();
		PreparedStatement pstm = null;
		try {
			pstm = conn.prepareStatement(sql);
			int i = 1;
			for (Object arg : args) {
				pstm.setObject(i, arg);
				i++;
			}
			return pstm.executeUpdate();
		} catch (Exception e) {
			throw new DaoRuntimeException(e);
		} finally {
			if (null != pstm) {
				try {
					pstm.close();
				} catch (Exception e) {
				}
			}
			if (null != conn) {
				try {
					releaseConnection(conn);
				} catch (Exception e) {
				}
			}
		}
	}

	public void query(JdbcResultVisitor rsVisitor, String sql, Object... args) {
		Connection conn = getConnection();
		PreparedStatement pstm = null;
		ResultSet rs = null;
		try {
			pstm = conn.prepareStatement(sql);
			int i = 1;
			for (Object arg : args) {
				pstm.setObject(i, arg);
				i++;
			}
			rs = pstm.executeQuery();
			while (rs.next()) {
				rsVisitor.visitWithException(rs);
			}
		} catch (Exception e) {
			throw new DaoRuntimeException(e);
		} finally {
			try {
				rsVisitor.afterNoNext();
			} catch (Exception e) {
			}
			if (null != rs) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (null != pstm) {
				try {
					pstm.close();
				} catch (Exception e) {
				}
			}
			if (null != conn) {
				try {
					releaseConnection(conn);
				} catch (Exception e) {
				}
			}
		}
	}

	public Connection getConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new DaoRuntimeException(e);
		}
	}

	public void releaseConnection(Connection conn) throws SQLException {
		conn.close();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof JdbcUtil) {
			JdbcUtil other = (JdbcUtil) obj;
			return dataSourceName.equals(other.dataSource);
		}
		return super.equals(obj);
	}

}
