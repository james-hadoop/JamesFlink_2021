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
        String database = "iceberg_db1";
        String hiveConfDir = "/home/james/install/hive-2.3.5/conf";

        HiveCatalog hiveCatalog = new HiveCatalog(catalog, database, hiveConfDir);
        tableEnv.registerCatalog(catalog, hiveCatalog);

        tableEnv.useCatalog("hive_catalog");
        tableEnv.executeSql("CREATE DATABASE if not exists iceberg_db10");
        tableEnv.useDatabase("iceberg_db10");

        tableEnv.executeSql("CREATE TABLE if not exists sample10 (\n" +
                " userid int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.userid.kind'='random',\n" +
                " 'fields.userid.min'='1',\n" +
                " 'fields.userid.max'='100',\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")");


        tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://localhost:9000/user/hive/warehouse/iceberg/iceberg_db10',\n" +
                "  'property-version'='1'\n" +
                ")");

        // change catalog
        tableEnv.useCatalog("hadoop_catalog");
        tableEnv.executeSql("CREATE DATABASE if not exists iceberg_hadoop_db");
        tableEnv.useDatabase("iceberg_hadoop_db");
        // create iceberg result table
        tableEnv.executeSql("drop table if exists hadoop_catalog.iceberg_hadoop_db.iceberg_002");
        tableEnv.executeSql("CREATE TABLE  hadoop_catalog.iceberg_hadoop_db.iceberg_002 (\n" +
                "    userid int,\n" +
                "    f_random_str STRING\n" +
                ")");

//        tableEnv.executeSql(
//                "INSERT INTO  hadoop_catalog.iceberg_hadoop_db.iceberg_002 " +
//                        " SELECT userid, f_random_str FROM hive_catalog.iceberg_db10.sample10");

        tableEnv.executeSql(
                "SELECT userid, f_random_str FROM hive_catalog.iceberg_db10.sample10").print();

//        Configuration hadoopConf = new Configuration();
//
//        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://localhost:9000/user/hive/warehouse/iceberg/iceberg_hadoop_db/iceberg_002", hadoopConf);
//
//        DataStream<RowData> stream = FlinkSource.forRowData()
//                .env(env)
//                .tableLoader(tableLoader)
//                .streaming(true)
//                .build();
//
//// Print all records to stdout.
//        stream.print();
    }


}
