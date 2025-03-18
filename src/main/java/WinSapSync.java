import common.GetTable;
import common.HanaDruidConnectionPool;
import common.JdbcSinkFunction;
import common.MysqlDruidConnectionPool;
import common.PaimonSinkFunction;
import constant.Constant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
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
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WinSapSync {
    private static final Logger logger = LogManager.getLogger(WinSapSync.class.getName());
//    static String paimonDbName;
//    static String paimonTableName;
//    static String sapTableName;
//    static String CDC1;
//    static String CDC2;
//    static JSONArray rowColumn;
    static Constant constant;

    public static void main(String[] args) throws Exception {
        String inputPara = "{\"taskId\":12345,\"period1\":120,\"period2\":60,\"CheckpointCleanup\":\"DELETE_ON_CANCELLATION\",\"TolerableCheckpointFailureNumber\":50,\"CheckpointTimeout\":600000,\"MinPauseBetweenCheckpoints\":5000,\"CheckpointingMode\":\"EXACTLY_ONCE\",\"Parallelism\":1,\"StateBackendPath\":\"hdfs://DATALAKE/user/flink/checkpoint\",\"CheckpointingTime\":60000,\"scheduleTable\":\"data_engine.sap_scheduler\",\"paimonDbName\":\"my_db\",\"paimonTableName\":\"my_table\",\"sapTableName\":\"TEST1\",\"rowColumn\":[\"ID\",\"TYPES\",\"TS\"],\"CDC1\":\"\\\"HD002\\\".\\\"CLOUD_CANAL_TRIGGER_DATA\\\"\",\"CDC2\":\"\\\"HD002\\\".\\\"CLOUD_CANAL_TRIGGER_DATA_3\\\"\"}";
        constant = new Constant(inputPara);

//        JSONObject json = JSONObject.parseObject(inputPara);
//        paimonDbName= (String) json.get("paimonDbName");
//        paimonTableName= (String) json.get("paimonTableName");
//        sapTableName= (String) json.get("sapTableName");
//        rowColumn= (JSONArray) json.get("rowColumn");
//        CDC1 = (String) json.get("CDC1");
//        CDC2 = (String) json.get("CDC2");

        System.out.println(constant.paimonDbName + " "+constant.paimonTableName+" "+constant.sapTableName+" "+ constant.rowColumn +" "+constant.CDC1+" "+constant.CDC2);
        System.out.println(constant.updateFinishSql.replace("${batchNum}","12345"));
        long mysqlMaxBatchNum = 1230L;
        System.out.println(mysqlMaxBatchNum+"");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置状态后端为 EmbeddedRocksDBStateBackend
        env.setStateBackend(new HashMapStateBackend());
        env.setParallelism(1);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        boolean initialTag = true;
        boolean initialTag2 = false;

        env.addSource(new WinSapSync.HeartBeatSource(constant,initialTag))
                .addSink(new JdbcSinkFunction(constant));

        env.addSource(new WinSapSync.HeartBeatSource(constant,initialTag2))
                .addSink(new PaimonSinkFunction(constant));

        env.execute("Flink Two-Phase Commit Sink Example");
    }


    public static class HeartBeatSource extends RichSourceFunction<String> {
        private transient ExecutorService executorService;
        private transient AtomicBoolean isBackRunning;
        private volatile boolean isRunning = true;
        private final Constant constant;
        private boolean initialTag = true;

        public HeartBeatSource(Constant constant, boolean initialTag) {

            this.constant = constant;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (initialTag) {
                initialMysqlScheduler();
            }
//            isBackRunning = new AtomicBoolean(true);
//            executorService = Executors.newSingleThreadExecutor();
//            executorService.submit(new WinSapSync.HeartBeatSource.BackgroundTask());
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                String s = getStringDate();
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect("heartBeat:" + s);
                }
                Thread.sleep(10000);
            }
        }


        private class BackgroundTask implements Runnable {
            transient Connection hanaConnection;
            transient Connection mysqlConnection;
            transient Statement hanaStmt;
            transient Statement mysqlStmt;
            transient StreamTableWrite write;
            transient StreamWriteBuilder writeBuilder;
            transient ScheduledExecutorService scheduler;
            String batchNum = "";
            long commitIdentifier = 0;

            private BackgroundTask() throws SQLException, Catalog.TableNotExistException {
                this.hanaConnection = HanaDruidConnectionPool.getConnection();
                this.mysqlConnection = MysqlDruidConnectionPool.getConnection();
                this.hanaStmt = hanaConnection.createStatement();
                this.mysqlStmt = mysqlConnection.createStatement();
                // 创建调度器
                this.scheduler = Executors.newScheduledThreadPool(1);
                // 1. Create a WriteBuilder (Serializable)
                Table table = GetTable.getTable("Windows",constant.paimonDbName, constant.paimonTableName,constant.rowColumn);
                this.writeBuilder = table.newStreamWriteBuilder();
                // 2. Write records in distributed tasks
                this.write = writeBuilder.newWrite();
            }

            @Override
            public void run() {
                while (isBackRunning.get()) {
                    try {
                        boolean runningTag = false;
                        String checkRunningSql = constant.checkRunningSql;

                        ResultSet runningResultSet = mysqlStmt.executeQuery(checkRunningSql);
                        while (runningResultSet.next()) {
                            runningTag = true;
                            System.out.println("Schedule is running! JUMP!");
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
                            String updateFinishSql = constant.updateFinishSql.replace("${batchNum}",batchNum);
                            System.out.println("updateFinishSql: " + updateFinishSql);

                            // 该批次号如果条数为0 直接更新调度表状态为finish
                            if (insertLines == 0 && batchNum != null && !batchNum.equals("")) {
                                mysqlStmt.executeUpdate(updateFinishSql);
                            } else if (readyTag && batchNum != null && !batchNum.equals("")) {
                                // 判断该表调度是否Ready 是否存在批次号
                                String updateRunningSql = constant.updateRunningSql.replace("${batchNum}",batchNum);

                                mysqlStmt.executeUpdate(updateRunningSql);

                                String hanaRunSql = constant.hanaRunSql.replace("${batchNum}",batchNum);
                                ResultSet hanaResultSet = hanaStmt.executeQuery(hanaRunSql);

                                while (hanaResultSet.next()) {
                                    Long rowBatchNum = hanaResultSet.getLong("BATCH_NUM");
                                    Long dataId = hanaResultSet.getLong("DATA_ID");
                                    Long transId = hanaResultSet.getLong("TRANSACTION_ID");
                                    Integer triggerId = hanaResultSet.getInt("TRIGGER_ID");
                                    String catalogName = hanaResultSet.getString("CATALOG_NAME");
                                    String schemaName = hanaResultSet.getString("SCHEMA_NAME");
                                    String tableName = hanaResultSet.getString("TABLE_NAME");
                                    String eventType = hanaResultSet.getString("EVENT_TYPE");
                                    String rowData = hanaResultSet.getString("ROW_DATA");
                                    String pkData = hanaResultSet.getString("PK_DATA");
                                    String oldData = hanaResultSet.getString("OLD_DATA");
                                    String transCreateTime = hanaResultSet.getString("CREATE_TIME");

//                                    JSONObject json = JSONObject.parseObject(rowData);
//                                    BinaryString rowId = BinaryString.fromString((String) json.get("ID"));
//                                    BinaryString rowTypes = BinaryString.fromString((String) json.get("TYPES"));
//                                    BinaryString rowTs = BinaryString.fromString((String) json.get("TS"));
//                                    RichCdcRecord rr = RichCdcRecord.builder(INSERT)
//                                            .field("PID", DataTypes.BIGINT(), String.valueOf(rowId))
//                                            .field("DATA_ID", DataTypes.BIGINT(), String.valueOf(dataId))
//                                            .field("TRANSACTION_ID", DataTypes.BIGINT(), String.valueOf(transId))
//                                            .field("ROW_DATA", DataTypes.BIGINT(), String.valueOf(rowData))
//                                            .build();
//                                    GenericRow record1 = GenericRow.of(rowBatchNum, transId, rowId, rowTypes, rowTs);
                                    if (!eventType.equals("D")) {
//                                        String[] values = (String[]) constant.rowColumn.toArray();
                                        GenericRow record1 = new GenericRow(constant.rowColumn.size() + 2);
                                        for (int i = 0; i < constant.rowColumn.size() + 2; ++i) {
                                            if (i == 0) {
                                                record1.setField(i, rowBatchNum);
                                            } else if (i == 1) {
                                                record1.setField(i, transId);
                                            } else {
                                                BinaryString bs = BinaryString.fromString((String) constant.rowColumn.get(i-2));
                                                record1.setField(i, bs);
                                            }
                                        }
                                        write.write(record1);
                                    }
                                }

                                List<CommitMessage> messages = write.prepareCommit(false, commitIdentifier);
                                commitIdentifier++;
                                StreamTableCommit commit = writeBuilder.newCommit();
                                commit.commit(commitIdentifier, messages);
                                System.out.println("Paimon Commit Succeed...");
                                mysqlStmt.executeUpdate(updateFinishSql);
                            }

                        }
                        // 执行后台任务
                        System.out.println("Running background task...");
                        Thread.sleep(7000); // 7s循环读一次调度表写一次Paimon
                    } catch (InterruptedException | SQLException e) {
                        String updateErrorSql = constant.updateErrorSql.replace("${batchNum}",batchNum);
                        try {
                            mysqlStmt.executeUpdate(updateErrorSql);
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void close() throws Exception {
            isBackRunning.set(false);
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
            super.close();
        }
    }


    public static String getStringDate() {
        Calendar cal = Calendar.getInstance();
        int iYear = cal.get(Calendar.YEAR);
        int iMonth = cal.get(Calendar.MONTH)+1;
        int iDate = cal.get(Calendar.DATE);
        int iHour = cal.get(Calendar.HOUR_OF_DAY);
        int iMinute = cal.get(Calendar.MINUTE);
        int iSecond = cal.get(Calendar.SECOND);

        String sYear = repair(iYear);
        String sMonth = repair(iMonth);
        String sDate = repair(iDate);
        String sHour = repair(iHour);
        String sMinute = repair(iMinute);
        String sSecond = repair(iSecond);

        String stringDate = sYear + sMonth + sDate + sHour + sMinute + sSecond;

        return stringDate;
    }

    public static String repair(int num) {
        String sNum = String.valueOf(num);
        if (num < 10) {
            sNum = '0' + sNum;
        }
        return sNum;
    }

    public static void initialMysqlScheduler() throws SQLException {
        String hanaSelectSql = constant.hanaSelectSql;
        Connection hanaConnection = HanaDruidConnectionPool.getConnection();
        Statement hanaStmt = hanaConnection.createStatement();
        ResultSet hanaResultSet = hanaStmt.executeQuery(hanaSelectSql);
        long hanaMaxBatchNum = 0L;
        while (hanaResultSet.next()) {
            hanaMaxBatchNum = hanaResultSet.getLong("hanaMaxBatchNum");
        }

        String mysqlSelectSql = constant.mysqlSelectSql;
        Connection mysqlConnection = MysqlDruidConnectionPool.getConnection();
        Statement mysqlStmt = mysqlConnection.createStatement();
        ResultSet mysqlResultSet = mysqlStmt.executeQuery(mysqlSelectSql);
        long mysqlMaxBatchNum = 0L;
        while (mysqlResultSet.next()) {
            mysqlMaxBatchNum = mysqlResultSet.getLong("mysqlMaxBatchNum");
        }

        if (hanaMaxBatchNum > mysqlMaxBatchNum) {
            String hanaSelectSqlNew = constant.hanaSelectSqlNew.replace("${mysqlMaxBatchNum}",mysqlMaxBatchNum+"");
            hanaResultSet = hanaStmt.executeQuery(hanaSelectSqlNew);

            while (hanaResultSet.next()) {
                long hanaBatchNum = hanaResultSet.getLong("BATCH_NUM");
                int hanaBatchNumCount = hanaResultSet.getInt("BATCH_NUM_COUNT");
                String mysqlSql = constant.mysqlSql.replace("${hanaBatchNum}",hanaBatchNum+"")
                        .replace("${hanaBatchNumCount1}",hanaBatchNumCount+"")
                        .replace("${hanaBatchNumCount2}",hanaBatchNumCount+"");
                System.out.println("mysqlSql: " + mysqlSql);
                mysqlStmt.executeUpdate(mysqlSql);
            }
        }

        // update running to ready
        String updateRunningToReadySql = constant.updateRunningToReadySql;
        mysqlStmt.executeUpdate(updateRunningToReadySql);

        // update error to ready
        String updateErrorToReadySql = constant.updateErrorToReadySql;
        mysqlStmt.executeUpdate(updateErrorToReadySql);

        hanaStmt.close();
        hanaConnection.close();
        mysqlStmt.close();
        mysqlConnection.close();
    }


}
