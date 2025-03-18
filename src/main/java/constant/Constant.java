package constant;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.io.Serializable;
import java.util.Map;

public class Constant implements Serializable {
    private static final long serialVersionUID = -78411919677615684L;
    public String scheduleTable;
    public String paimonDbName;
    public String paimonTableName;
    public String sapTableName;
    public String CDC1;
    public String CDC2;
    public JSONArray rowColumn;
    public String checkRunningSql;
    public String checkReadySql;
    public String updateFinishSql;
    public String updateRunningSql;
    public String hanaRunSql;
    public String updateErrorSql;
    public String hanaSelectSql;
    public String mysqlSelectSql;
    public String hanaSelectSqlNew;
    public String mysqlSql;
    public String updateRunningToReadySql;
    public String updateErrorToReadySql;
    public String hanaSelectSqlTop10000;
    public String hanaInsertSql;
    public String hanaDeleteSql;
    public String CheckpointCleanup;
    public String CheckpointingModeTmp;
    public String StateBackendPath;
    public int period1;
    public int period2;
    public int TolerableCheckpointFailureNumber;
    public int Parallelism;
    public long taskId;
    public long CheckpointTimeout;
    public long MinPauseBetweenCheckpoints;
    public long CheckpointingTime;

    public static void main(String[] args) {
        String inputPara = "{\"taskId\":12345,\"period1\":120,\"period2\":60,\"CheckpointCleanup\":\"DELETE_ON_CANCELLATION\",\"TolerableCheckpointFailureNumber\":50,\"CheckpointTimeout\":600000,\"MinPauseBetweenCheckpoints\":5000,\"CheckpointingMode\":\"EXACTLY_ONCE\",\"Parallelism\":1,\"StateBackendPath\":\"hdfs://DATALAKE/user/flink/checkpoint\",\"CheckpointingTime\":60000,\"scheduleTable\":\"data_engine.sap_scheduler\",\"paimonDbName\":\"my_db\",\"paimonTableName\":\"my_table\",\"sapTableName\":\"TEST2\",\"rowColumn\":[\"ID\",\"TYPES\",\"TS\"],\"CDC1\":\"\\\"HD002\\\".\\\"CLOUD_CANAL_TRIGGER_DATA\\\"\",\"CDC2\":\"\\\"HD002\\\".\\\"CLOUD_CANAL_TRIGGER_DATA_3\\\"\"}";

        Constant constant = new Constant(inputPara);

        for (int i = 0; i < constant.rowColumn.size() + 2; ++i) {
            System.out.println(constant.rowColumn.get(i));
        }
    }

    public Constant(String inputPara){

//        String inputPara = "{\"scheduleTable\":\"test.sap_scheduler\",\"paimonDbName\":\"ods_wj\",\"paimonTableName\":\"my_table\",\"sapTableName\":\"TEST1\",\"rowColumn\":[\"ID\",\"TYPES\",\"TS\"],\"CDC1\":\"\\\"HD002\\\".\\\"CLOUD_CANAL_TRIGGER_DATA_3\\\"\",\"CDC2\":\"\\\"HD002\\\".\\\"CLOUD_CANAL_TRIGGER_DATA_2\\\"\"}";
        JSONObject jsonOri = JSONObject.parseObject(inputPara);
        JSONObject json = new JSONObject();
        transformJsonObject(jsonOri,json);
        scheduleTable= (String) json.get("SCHEDULETABLE");
        paimonDbName= (String) json.get("PAIMONDBNAME");
        paimonTableName= (String) json.get("PAIMONTABLENAME");
        sapTableName= (String) json.get("SAPTABLENAME");
        rowColumn= (JSONArray) json.get("ROWCOLUMN");
        CDC1 = (String) json.get("CDC1");
        CDC2 = (String) json.get("CDC2");
        CheckpointCleanup = (String) json.get("CHECKPOINTCLEANUP");
        TolerableCheckpointFailureNumber = (int) json.get("TOLERABLECHECKPOINTFAILURENUMBER");
        CheckpointTimeout = Long.parseLong(json.get("CHECKPOINTTIMEOUT").toString());
        MinPauseBetweenCheckpoints = Long.parseLong(json.get("MINPAUSEBETWEENCHECKPOINTS").toString());
        CheckpointingModeTmp = (String) json.get("CHECKPOINTINGMODE");
        Parallelism = (int) json.get("PARALLELISM");
        StateBackendPath = (String) json.get("STATEBACKENDPATH");
        CheckpointingTime = Long.parseLong(json.get("CHECKPOINTINGTIME").toString());
        taskId = Long.parseLong(json.get("TASKID").toString());
        period1 = (int) json.get("PERIOD1");
        period2 = (int) json.get("PERIOD2");

        checkRunningSql = "SELECT id FROM " + scheduleTable + " WHERE paimonStatus = 'RUNNING' and sapTableName='" + sapTableName + "' and taskId="+ taskId;
        checkReadySql = "SELECT batchNum,insertLines FROM " + scheduleTable + " WHERE paimonStatus='READY' and sapTableName='" + sapTableName + "' and taskId="+ taskId +" ORDER BY batchNum LIMIT 1";
        updateFinishSql = "UPDATE " + scheduleTable + " SET paimonStatus='FINISHED' WHERE sapTableName='" + sapTableName + "' and batchNum=${batchNum} and taskId="+ taskId;
        updateRunningSql = "UPDATE " + scheduleTable + " SET paimonStatus='RUNNING' WHERE sapTableName='" + sapTableName + "' and batchNum=${batchNum} and taskId="+ taskId;
        hanaRunSql = "SELECT * FROM " + CDC2 + " where BATCH_NUM=${batchNum}";
        updateErrorSql = "UPDATE " + scheduleTable + " SET paimonStatus='ERROR' WHERE sapTableName='" + sapTableName + "' and batchNum=${batchNum} and taskId="+ taskId;
        hanaSelectSql = "SELECT MAX(BATCH_NUM) as hanaMaxBatchNum FROM " + CDC2;
        mysqlSelectSql = "SELECT MAX(batchNum) as mysqlMaxBatchNum FROM " + scheduleTable + " WHERE sapTableName = '"+sapTableName+"' and taskId="+ taskId;
        hanaSelectSqlNew = "SELECT BATCH_NUM,COUNT(1) AS BATCH_NUM_COUNT FROM " + CDC2 + " WHERE BATCH_NUM > ${mysqlMaxBatchNum} GROUP BY BATCH_NUM ORDER BY BATCH_NUM";
        mysqlSql = "insert into " + scheduleTable + "(sapTableName,batchNum,insertLines,deleteLines,cdc2Status,paimonStatus,createTime,updateTime,taskId) values ('" + sapTableName + "', ${hanaBatchNum} , ${hanaBatchNumCount1} , ${hanaBatchNumCount2} ,'FINISHED','READY',now(),now(),"+taskId+")";
        updateRunningToReadySql = "UPDATE " + scheduleTable + " SET paimonStatus='READY' WHERE sapTableName='" + sapTableName + "' AND paimonStatus='RUNNING' and taskId="+ taskId;
        updateErrorToReadySql = "UPDATE " + scheduleTable + " SET paimonStatus='READY' WHERE sapTableName='" + sapTableName + "' AND paimonStatus='ERROR' and taskId="+ taskId;
        hanaSelectSqlTop10000 = "SELECT * FROM " + CDC1 + " LIMIT 10000";
        hanaInsertSql = "INSERT INTO " + CDC2 + " SELECT ${batchNum} AS BATCH_NUM,* FROM " + CDC1 + " WHERE TRANSACTION_ID IN ";
        hanaDeleteSql = "DELETE FROM " + CDC1 + " WHERE TRANSACTION_ID IN ";

        System.out.println("checkRunningSql: "+checkRunningSql);
        System.out.println("checkReadySql: "+checkReadySql);
        System.out.println("updateFinishSql: "+updateFinishSql);
        System.out.println("updateRunningSql: "+updateRunningSql);
        System.out.println("hanaRunSql: "+hanaRunSql);
        System.out.println("updateErrorSql: "+updateErrorSql);
        System.out.println("hanaSelectSql: "+hanaSelectSql);
        System.out.println("mysqlSelectSql: "+mysqlSelectSql);
        System.out.println("hanaSelectSqlNew: "+hanaSelectSqlNew);
        System.out.println("mysqlSql: "+mysqlSql);
        System.out.println("updateRunningToReadySql: "+updateRunningToReadySql);
        System.out.println("updateErrorToReadySql: "+updateErrorToReadySql);
        System.out.println("hanaSelectSqlTop10000: "+hanaSelectSqlTop10000);
        System.out.println("hanaInsertSql: "+hanaInsertSql);
        System.out.println("hanaDeleteSql: "+hanaDeleteSql);
    }

    private static void transformJsonObject(JSONObject from, JSONObject to) {
        for (Map.Entry<String, Object> entry : from.entrySet()) {
            String key = entry.getKey().toUpperCase();
            Object value = entry.getValue();
            to.put(key, value);
        }
    }
}
