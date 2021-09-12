package com.james.flink.app.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Created by James on 21-9-10 上午12:52
 */
public class FlinkTableWriteSample2 {
    public static void main(String[] args) {
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.enableCheckpointing(10000);
//
//        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//
//        tenv.useCatalog("hive_catalog");
//
//        tenv.useDatabase("iceberg_db");
//
//        tenv.executeSql(
//                "INSERT INTO sample VALUES (10, 'a')");


        /**
         *  第2种实现方式
         */
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String catalog            = "hive_catalog";
        String database = "iceberg_db1";
        String hiveConfDir = "/home/james/install/hive/apache-hive-2.3.7-bin/conf";

        HiveCatalog hive = new HiveCatalog(catalog, database, hiveConfDir);
        tableEnv.registerCatalog(catalog, hive);
        // 使用注册的catalog
        tableEnv.useCatalog(catalog);
    }


}
