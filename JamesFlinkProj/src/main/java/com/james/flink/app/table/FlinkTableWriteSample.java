package com.james.flink.app.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by James on 21-9-10 上午12:52
 */
public class FlinkTableWriteSample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE CATALOG hive_catalog2 WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive'," +
                //"  'hive-site-path'='hdfs://localhost/data/flink/conf/hive-site.xml'" +
                "  'hive-site-path'='/home/james/install/hive/apache-hive-2.3.7-bin/conf/hive-site.xml'" +
                ")");

        tenv.useCatalog("hive_catalog2");


        tenv.executeSql("CREATE DATABASE iceberg_db");
        tenv.useDatabase("iceberg_db");

        tenv.executeSql("CREATE TABLE sample (\n" +
                " userid int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.userid.kind'='random',\n" +
                " 'fields.userid.min'='1',\n" +
                " 'fields.userid.max'='100',\n" +
                "'fields.f_random_str.length'='10'\n" +
                ")");

        tenv.executeSql(
                "INSERT INTO hive_catalog2.iceberg_db.sample VALUES (1, 'a')");
    }


}
