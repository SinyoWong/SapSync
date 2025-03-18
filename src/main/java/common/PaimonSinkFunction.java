package common;

import com.alibaba.fastjson2.JSONObject;
import constant.Constant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class PaimonSinkFunction extends RichSinkFunction<String> {
    private static final Logger logger = LogManager.getLogger(PaimonSinkFunction.class);
    private final Constant constant;
    private Connection hanaConnection = null;
    private Connection mysqlConnection = null;
    private Statement hanaStmt = null;
    private Statement mysqlStmt = null;
    private StreamTableWrite write;
    private StreamWriteBuilder writeBuilder;
    String batchNum = "";
    long commitIdentifier = 0;

    public PaimonSinkFunction(Constant constant) {
        this.constant = constant;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hanaConnection = HanaDruidConnectionPool.getConnection();
        mysqlConnection = MysqlDruidConnectionPool.getConnection();
//        mysqlConnection.setAutoCommit(false);

        hanaStmt = hanaConnection.createStatement();
        hanaStmt.setFetchSize(1000);
        mysqlStmt = mysqlConnection.createStatement();

        // Paimon
        // 1. Create a WriteBuilder (Serializable)
        Table table = GetTable.getTable("Linux", constant.paimonDbName, constant.paimonTableName, constant.rowColumn);
//        Table table = GetTable.getTable("Windows", constant.paimonDbName, constant.paimonTableName, constant.rowColumn);
        writeBuilder = table.newStreamWriteBuilder();
        // 2. Write records in distributed tasks
        write = writeBuilder.newWrite();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        super.invoke(value, context);
        logger.info("Paimon value: " + value);
        if (hanaStmt == null) {
            hanaStmt = hanaConnection.createStatement();
        }
        if (mysqlStmt == null) {
            mysqlStmt = mysqlConnection.createStatement();
        }
        if (write == null) {
            write = writeBuilder.newWrite();
        }

        try {
            boolean runningTag = false;
            String checkRunningSql = constant.checkRunningSql;

            ResultSet runningResultSet = mysqlStmt.executeQuery(checkRunningSql);
            while (runningResultSet.next()) {
                runningTag = true;
                logger.info("Schedule is running! JUMP!");
            }
            // 判断该表调度是否正在RUNNING
            if (!runningTag) {
                boolean readyTag = false;
                int insertLines = 0;

                String checkReadySql = constant.checkReadySql;
                ResultSet checkReadySet = mysqlStmt.executeQuery(checkReadySql);
                while (checkReadySet.next()) {
                    readyTag = true;
                    insertLines = checkReadySet.getInt("insertLines");
                    batchNum = checkReadySet.getString("batchNum");
                }
                String updateFinishSql = constant.updateFinishSql.replace("${batchNum}", batchNum);
                logger.info("updateFinishSql: " + updateFinishSql);
                // 该批次号如果条数为0 直接更新调度表状态为finish
                if (insertLines == 0 && batchNum != null && !batchNum.equals("")) {
                    mysqlStmt.executeUpdate(updateFinishSql);
                } else if (readyTag && batchNum != null && !batchNum.equals("")) {
                    // 判断该表调度是否Ready 是否存在批次号
                    String updateRunningSql = constant.updateRunningSql.replace("${batchNum}", batchNum);

                    mysqlStmt.executeUpdate(updateRunningSql);

                    String hanaRunSql = constant.hanaRunSql.replace("${batchNum}", batchNum);
                    ResultSet hanaResultSet = hanaStmt.executeQuery(hanaRunSql);

                    while (hanaResultSet.next()) {
                        Long rowBatchNum = hanaResultSet.getLong("BATCH_NUM");
                        Long transId = hanaResultSet.getLong("TRANSACTION_ID");
                        String eventType = hanaResultSet.getString("EVENT_TYPE");
                        String rowData = hanaResultSet.getString("ROW_DATA");
//                        Long dataId = hanaResultSet.getLong("DATA_ID");
//                        Integer triggerId = hanaResultSet.getInt("TRIGGER_ID");
//                        String catalogName = hanaResultSet.getString("CATALOG_NAME");
//                        String schemaName = hanaResultSet.getString("SCHEMA_NAME");
//                        String tableName = hanaResultSet.getString("TABLE_NAME");
//                        String pkData = hanaResultSet.getString("PK_DATA");
//                        String oldData = hanaResultSet.getString("OLD_DATA");
//                        String transCreateTime = hanaResultSet.getString("CREATE_TIME");

                        JSONObject json = JSONObject.parseObject(rowData);

                        if (!eventType.equals("D")) {
                            GenericRow record1 = new GenericRow(constant.rowColumn.size() + 2);
                            for (int i = 0; i < constant.rowColumn.size() + 2; ++i) {
                                if (i == constant.rowColumn.size()) {
                                    record1.setField(i, rowBatchNum);
                                } else if (i == constant.rowColumn.size() + 1) {
                                    record1.setField(i, transId);
                                } else {
                                    BinaryString bs = BinaryString.fromString((String) json.get(constant.rowColumn.get(i)));
                                    record1.setField(i, bs);
                                }
                            }
                            write.write(record1);
                        }
                    }

                    List<CommitMessage> messages = write.prepareCommit(false, commitIdentifier);
                    commitIdentifier++;
                    StreamTableCommit commit = writeBuilder.newCommit();

                    // 提交 Paimon 数据
//                    Map<Long, List<CommitMessage>> commitIdentifiersAndMessages = new HashMap<>();
//                    commitIdentifiersAndMessages.put(commitIdentifier, messages);
//                    commit.filterAndCommit(commitIdentifiersAndMessages);
                    commit.commit(commitIdentifier, messages);

                    logger.info("Paimon Commit Succeed...");
                    mysqlStmt.executeUpdate(updateFinishSql);
                }
            }

        } catch (InterruptedException | SQLException e) {
            String updateErrorSql = constant.updateErrorSql.replace("${batchNum}",batchNum);
            try {
                mysqlStmt.executeUpdate(updateErrorSql);
            } catch (SQLException ex) {
                ex.printStackTrace();
                throw new SQLException("Error execute updated sql!");
            } finally {
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
            }
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