package org.bigdatatechcir.warehouse.datageneration.business_code.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DbUtil {
    private static final Logger logger = LoggerFactory.getLogger(DbUtil.class);
    private static HikariDataSource dataSource;
    private static boolean isConnected = false;

    static {
        try {
            initDataSource();
            testConnection();
            isConnected = true;
        } catch (Exception e) {
            logger.error("数据库初始化失败: {}", e.getMessage());
            isConnected = false;
        }
    }

    private static void initDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://hadoop102:3306/gmall?useUnicode=true&characterEncoding=utf-8&useSSL=false");
        config.setUsername("root");
        config.setPassword("000000");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(5000); // 设置连接超时为5秒
        dataSource = new HikariDataSource(config);
    }

    private static void testConnection() {
        try (Connection conn = dataSource.getConnection()) {
            logger.info("数据库连接测试成功");
        } catch (SQLException e) {
            throw new RuntimeException("数据库连接测试失败", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        if (!isConnected) {
            throw new SQLException("数据库未连接");
        }
        return dataSource.getConnection();
    }

    public static void batchInsert(String sql, List<Object[]> params) {
        if (!isConnected) {
            printToConsole(sql, params);
            return;
        }

        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            
            for (Object[] param : params) {
                for (int i = 0; i < param.length; i++) {
                    ps.setObject(i + 1, param[i]);
                }
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            logger.error("执行批量插入失败: {}", e.getMessage());
            printToConsole(sql, params);
        } finally {
            closeResources(conn, ps);
        }
    }

    private static void printToConsole(String sql, List<Object[]> params) {
        System.out.println("\n=== 模拟SQL执行（数据库连接失败时的打印输出）===");
        System.out.println("SQL: " + sql);
        System.out.println("数据:");
        int count = 0;
        for (Object[] param : params) {
            if (count++ > 10) {
                System.out.println("... (还有 " + (params.size() - 10) + " 条数据)");
                break;
            }
            StringBuilder row = new StringBuilder("(");
            for (int i = 0; i < param.length; i++) {
                row.append(param[i]);
                if (i < param.length - 1) {
                    row.append(", ");
                }
            }
            row.append(")");
            System.out.println(row.toString());
        }
        System.out.println("=== 模拟SQL执行结束 ===\n");
    }

    private static void closeResources(Connection conn, PreparedStatement ps) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                logger.error("关闭PreparedStatement失败", e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("关闭Connection失败", e);
            }
        }
    }

    public static void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
} 