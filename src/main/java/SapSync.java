import common.HeartBeatSource;
import common.JdbcSinkFunction;
import common.PaimonSinkFunction;
import constant.Constant;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class SapSync {
    private static final Logger logger = LogManager.getLogger(SapSync.class);

    public static void main(String[] args) throws Exception {
//        String inputPara = "{\"CheckpointCleanup\":\"DELETE_ON_CANCELLATION\",\"TolerableCheckpointFailureNumber\":50,\"CheckpointTimeout\":600000,\"MinPauseBetweenCheckpoints\":5000,\"CheckpointingMode\":\"EXACTLY_ONCE\",\"Parallelism\":1,\"StateBackendPath\":\"hdfs://DATALAKE/user/flink/checkpoint\",\"CheckpointingTime\":60000,\"scheduleTable\":\"test.sap_scheduler2\",\"paimonDbName\":\"my_db\",\"paimonTableName\":\"my_table\",\"sapTableName\":\"TEST1\",\"rowColumn\":[\"ID\",\"TYPES\",\"TS\"],\"CDC1\":\"\\\"HD002\\\".\\\"CLOUD_CANAL_TRIGGER_DATA_3\\\"\",\"CDC2\":\"\\\"HD002\\\".\\\"CLOUD_CANAL_TRIGGER_DATA_2\\\"\"}";
        String inputPara =args[0];
        logger.info("args[0]: "+args[0]);
        Constant constant = new Constant(inputPara);
        logger.info(constant.paimonDbName + " "+constant.paimonTableName+" "+constant.sapTableName+" "+ constant.rowColumn +" "+constant.CDC1+" "+constant.CDC2);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果设置了checkpoint
        if (constant.CheckpointingTime>0L) {
            if (constant.CheckpointingModeTmp.equalsIgnoreCase("exactly_once")) {
                env.enableCheckpointing(constant.CheckpointingTime, CheckpointingMode.EXACTLY_ONCE);
            } else {
                env.enableCheckpointing(constant.CheckpointingTime, CheckpointingMode.AT_LEAST_ONCE);
            }
            if (constant.CheckpointCleanup.equalsIgnoreCase("delete_on_cancellation")) {
                env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
            } else {
                env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            }
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(constant.TolerableCheckpointFailureNumber);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(constant.MinPauseBetweenCheckpoints);
            env.getCheckpointConfig().setCheckpointTimeout(constant.CheckpointTimeout);
            // 配置状态后端为 EmbeddedRocksDBStateBackend
            EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
            // 设置checkpoint存储
            FileSystemCheckpointStorage checkpointStorage = new FileSystemCheckpointStorage(constant.StateBackendPath);
            env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);
            env.setStateBackend(stateBackend);
        }
        boolean initialTag = true;
        boolean initialTag2 = false;

        env.setParallelism(constant.Parallelism);

        env.addSource(new HeartBeatSource(constant,constant.period1, initialTag))
                .addSink(new JdbcSinkFunction(constant));

        env.addSource(new HeartBeatSource(constant,constant.period2, initialTag2))
                .addSink(new PaimonSinkFunction(constant));

        env.execute("Flink Two-Phase Commit Sink Example");
    }

}
