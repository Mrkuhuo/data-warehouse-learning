package org.bigdatatechcir.warehouse.datageneration.business_code.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class DbUtil {
    private static final Logger logger = LoggerFactory.getLogger(DbUtil.class);
    private HikariDataSource dataSource;
    private boolean isConnected = false;

    @Value("${spring.datasource.url}")
    private String url;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${spring.datasource.hikari.maximum-pool-size:10}")
    private int maximumPoolSize;

    @Value("${spring.datasource.hikari.minimum-idle:5}")
    private int minimumIdle;

    @Value("${spring.datasource.hikari.connection-timeout:5000}")
    private long connectionTimeout;

    @Value("${spring.datasource.hikari.idle-timeout:300000}")
    private long idleTimeout;

    @Value("${spring.datasource.hikari.max-lifetime:1200000}")
    private long maxLifetime;

    @PostConstruct
    public void init() {
        try {
            initDataSource();
            testConnection();
            isConnected = true;
        } catch (Exception e) {
            logger.error("数据库初始化失败: {}", e.getMessage());
            isConnected = false;
        }
    }

    private void initDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        config.setConnectionTimeout(connectionTimeout);
        config.setIdleTimeout(idleTimeout);
        config.setMaxLifetime(maxLifetime);
        dataSource = new HikariDataSource(config);
    }

    private void testConnection() {
        try (Connection conn = dataSource.getConnection()) {
            logger.info("数据库连接测试成功");
        } catch (SQLException e) {
            throw new RuntimeException("数据库连接测试失败", e);
        }
    }

    public Connection getConnection() throws SQLException {
        if (!isConnected) {
            throw new SQLException("数据库未连接");
        }
        return dataSource.getConnection();
    }

    public void batchInsert(String sql, List<Object[]> params) {
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

    private void printToConsole(String sql, List<Object[]> params) {
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

    private void closeResources(Connection conn, PreparedStatement ps) {
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

    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    public int queryForInt(String sql) {
        if (!isConnected) {
            return 0;
        }
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            logger.error("查询整数值失败: {}", e.getMessage());
            return 0;
        }
    }
} 