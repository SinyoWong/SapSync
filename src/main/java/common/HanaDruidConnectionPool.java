package common;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class HanaDruidConnectionPool {
    private transient static DruidDataSource dataSource = null;

    static {
        String driverClassName = "com.sap.db.jdbc.Driver";
        String dbUrl = "jdbc:sap://:30015/HD002";
        String dbUserName = "";
        String dbPassword = "";
        try {
            dataSource = new DruidDataSource(); // 创建Druid连接池
            dataSource.setDriverClassName(driverClassName); // 设置连接池的数据库驱动
            dataSource.setUrl(dbUrl); // 设置数据库的连接地址
            dataSource.setUsername(dbUserName); // 设置数据库的用户名
            dataSource.setPassword(dbPassword); // 设置数据库的密码
            dataSource.setInitialSize(5); // 设置连接池的初始大小
            dataSource.setMinIdle(2); // 设置连接池大小的下限
            dataSource.setMaxActive(20); // 设置连接池大小的上限
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private HanaDruidConnectionPool() {
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
