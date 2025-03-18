package common;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class MysqlDruidConnectionPool {
    private transient static DruidDataSource dataSource = null;
    static {
        String driverClassName = "com.mysql.jdbc.Driver";
        String dbUrl = "jdbc:mysql://192.168.208.207:3306/data_engine?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8&allowMultiQueries=true&rewriteBatchedStatements=true";
        String dbUserName = "";
        String dbPassword = "";

        try {
            dataSource = new DruidDataSource(); // 创建Druid连接池
            dataSource.setDriverClassName(driverClassName); // 设置连接池的数据库驱动
            dataSource.setUrl(dbUrl); // 设置数据库的连接地址
            dataSource.setUsername(dbUserName); // 设置数据库的用户名
            dataSource.setPassword(dbPassword); // 设置数据库的密码

            // 连接池配置
            dataSource.setInitialSize(5);   // 设置连接池的初始大小
            dataSource.setMinIdle(2);       // 设置连接池大小的下限
            dataSource.setMaxActive(20);    // 设置连接池大小的上限
            dataSource.setMaxWait(60000);          // 获取连接的最大等待时间（毫秒）

            // 连接有效性检查
            dataSource.setTestWhileIdle(true);     // 检查空闲连接的有效性
            dataSource.setTestOnBorrow(false);     // 获取连接时不测试
            dataSource.setTestOnReturn(false);     // 归还连接时不测试

            // 连接保活
            dataSource.setTimeBetweenEvictionRunsMillis(60000); // 检查空闲连接的间隔时间（60 秒）
            dataSource.setMinEvictableIdleTimeMillis(300000);   // 连接的最小空闲时间（5 分钟）

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void closeDataSource() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    private MysqlDruidConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void rollbackConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();  // 回滚
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();  // 关闭连接并将其返回到连接池中
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
