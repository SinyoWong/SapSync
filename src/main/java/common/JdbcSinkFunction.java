package common;

import constant.Constant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;


public class JdbcSinkFunction extends RichSinkFunction<String> {
    private static final Logger logger = LogManager.getLogger(JdbcSinkFunction.class);
    private final Constant constant;
    private Connection hanaConnection = null;
    private Connection mysqlConnection = null;
    private Statement hanaStmt = null;
    private Statement mysqlStmt = null;
    static boolean runTag = true;

    Set<Long> localPrimaryKeySet = new HashSet<>();

    public JdbcSinkFunction(Constant constant) {
        this.constant = constant;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hanaConnection = HanaDruidConnectionPool.getConnection();
        hanaConnection.setAutoCommit(false);
        mysqlConnection = MysqlDruidConnectionPool.getConnection();
        mysqlConnection.setAutoCommit(false);

        hanaStmt = hanaConnection.createStatement();
        mysqlStmt = mysqlConnection.createStatement();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        super.invoke(value, context);
        logger.info("value: " + value);

        runTag = true;
        localPrimaryKeySet.clear();

        String batchNum = value.split(":")[1];

//        String hanaSelectSql = "SELECT * FROM \"HD002\".\"CLOUD_CANAL_TRIGGER_DATA_3\" LIMIT 10000";
        String hanaSelectSql = constant.hanaSelectSqlTop10000;

        int insertCdc2Nums = 0;
        int deleteCdc1Nums = 0;

        // 写入HanaDB
        try {
            if (hanaConnection == null ) {
                hanaConnection = HanaDruidConnectionPool.getConnection();
                hanaConnection.setAutoCommit(false);
            }
            if (mysqlConnection == null) {
                mysqlConnection = MysqlDruidConnectionPool.getConnection();
                mysqlConnection.setAutoCommit(false);
            }
            if (hanaStmt == null) {
                hanaStmt = hanaConnection.createStatement();
            }
            if (mysqlStmt == null) {
                mysqlStmt = mysqlConnection.createStatement();
            }

            ResultSet resultSet = hanaStmt.executeQuery(hanaSelectSql);

            while (resultSet.next()) {
                Long transId = resultSet.getLong("TRANSACTION_ID");
                localPrimaryKeySet.add(transId);
            }

            logger.info("TRANSACTION_ID: " + localPrimaryKeySet);

            if (localPrimaryKeySet.size() > 0) {
//                String insertSql = "INSERT INTO \"HD002\".\"CDC2\" SELECT " + batchNum + " AS BATCH_NUM,* FROM \"HD002\".\"CDC1\" WHERE TRANSACTION_ID IN ";
                String insertSql = constant.hanaInsertSql.replace("${batchNum}",batchNum);
                {
                    String condition = "";
                    for (Long pk : localPrimaryKeySet) {
                        condition = condition + pk.toString() + ",";
                    }
                    condition = condition.substring(0, condition.length() - 1);
                    insertSql = insertSql + "(" + condition + ")";
                    insertCdc2Nums = hanaStmt.executeUpdate(insertSql);

                    logger.info("Insert Lines: " + insertCdc2Nums);
                    logger.info("insertSql: " + insertSql);
                }

//                String deleteSql = "DELETE FROM \"HD002\".\"CLOUD_CANAL_TRIGGER_DATA_3\" WHERE TRANSACTION_ID IN ";
                String deleteSql = constant.hanaDeleteSql;
                String condition = "";
                for (Long pk : localPrimaryKeySet) {
                    condition = condition + pk.toString() + ",";
                }
                condition = condition.substring(0, condition.length() - 1);
                deleteSql = deleteSql + "(" + condition + ")";
                deleteCdc1Nums = hanaStmt.executeUpdate(deleteSql);

                logger.info("Delete Lines: " + deleteCdc1Nums);
                logger.info("deleteSql: " + deleteSql);
            }
            logger.info("CDC1 -> CDC2 SUCCEED.");
            localPrimaryKeySet.clear();
        } catch (Exception e) {
            runTag = false;
            logger.info("CDC1 -> CDC2 FAILED.");
            HanaDruidConnectionPool.rollbackConnection(hanaConnection);
            HanaDruidConnectionPool.closeConnection(hanaConnection);
            e.printStackTrace();
        }

        // 写入MySQL
        try {
            String mysqlSql = constant.mysqlSql.replace("${hanaBatchNum}",batchNum)
                    .replace("${hanaBatchNumCount1}",insertCdc2Nums+"")
                    .replace("${hanaBatchNumCount2}",deleteCdc1Nums+"");
            logger.info("mysqlSql: " + mysqlSql);
            mysqlStmt.executeUpdate(mysqlSql);
            logger.info("Insert mysql scheduler table SUCCEED.");
        } catch (Exception e) {
            runTag = false;
            logger.info("Insert mysql scheduler table FAILED.");
            MysqlDruidConnectionPool.rollbackConnection(mysqlConnection);
            MysqlDruidConnectionPool.closeConnection(mysqlConnection);
            e.printStackTrace();
        }

        try {
            hanaConnection.commit();
            mysqlConnection.commit();
            logger.info("Commit SUCCEED.");
        } catch (SQLException e) {
            runTag = false;
            logger.info("Commit FAILED.");
            if (hanaStmt != null) {
                hanaStmt.close();
            }
            if (mysqlStmt != null) {
                mysqlStmt.close();
            }
            HanaDruidConnectionPool.rollbackConnection(hanaConnection);
            HanaDruidConnectionPool.closeConnection(hanaConnection);
            MysqlDruidConnectionPool.rollbackConnection(mysqlConnection);
            MysqlDruidConnectionPool.closeConnection(mysqlConnection);
            e.printStackTrace();
        }

        if(!runTag){
            throw new SQLException("Error execute sql transactions!");
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        HanaDruidConnectionPool.closeConnection(hanaConnection);
        MysqlDruidConnectionPool.closeConnection(mysqlConnection);

        if (hanaStmt != null) {
            hanaStmt.close();
        }
        if (mysqlStmt != null) {
            mysqlStmt.close();
        }
    }

}