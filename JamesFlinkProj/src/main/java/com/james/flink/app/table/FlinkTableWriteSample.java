package com.james.flink.app.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;


/**
 * Created by James on 21-9-10 上午12:52
 */
public class FlinkTableWriteSample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String catalog = "hive_catalog";
        String database = "iceberg_db_20211017";
//        String hiveConfDir = "/home/james/install/apache-hive-3.1.2-bin/conf";
        String hiveConfDir = "/home/james/install/hive-2.3.5/conf";
        String srcTableName = "iceberg_table_src";
        String dstTableName = "iceberg_table_dst";

        HiveCatalog hiveCatalog = new HiveCatalog(catalog, null, hiveConfDir);
        tableEnv.registerCatalog(catalog, hiveCatalog);

        tableEnv.useCatalog(catalog);
        tableEnv.executeSql("CREATE DATABASE if not exists " + database);
        tableEnv.useDatabase(database);

        tableEnv.executeSql("CREATE TABLE if not exists " + srcTableName + " (\n" +
                " user_id int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.user_id.kind'='random',\n" +
                " 'fields.user_id.min'='1',\n" +
                " 'fields.user_id.max'='100',\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")");


        tableEnv.executeSql(String.format("drop table if exists %s.%s.%s", catalog, database, dstTableName));
        tableEnv.executeSql(String.format("CREATE TABLE %s.%s.%s ( user_id int, f_random_str STRING) WITH ('connector' = 'filesystem', 'path' = '/home/james/temp/flink_data/iceberg_table_dst', 'format' = 'parquet')", catalog, database, dstTableName));

        tableEnv.executeSql(String.format("INSERT INTO %s select * from %s", dstTableName, srcTableName));
        tableEnv.executeSql(String.format("select * from %s", dstTableName)).print();

//        tableEnv.executeSql(
//                String.format("SELECT user_id, f_random_str FROM %s.%s.%s", catalog, database, srcTableName)).print();
    }
}
