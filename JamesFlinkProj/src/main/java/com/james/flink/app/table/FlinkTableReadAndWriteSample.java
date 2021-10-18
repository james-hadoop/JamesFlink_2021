package com.james.flink.app.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Created by James on 21-9-10 上午12:52iceberg_db
 */
public class FlinkTableReadAndWriteSample {
    public static void main(String[] args) {
//        hadoop fs -ls /user/hive/warehouse/iceberg/hive_catalog/iceberg_db1.db

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String catalog = "hive_catalog";
        String database = "iceberg_db_20211017_night";
//        String hiveConfDir = "/home/james/install/apache-hive-3.1.2-bin/conf";
        String hiveConfDir = "/home/james/install/hive-2.3.5/conf";

        HiveCatalog hive = new HiveCatalog(catalog, null, hiveConfDir);
        tableEnv.registerCatalog(catalog, hive);
        // 使用注册的catalog
        tableEnv.useCatalog(catalog);

        tableEnv.useCatalog(catalog);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + database);
        tableEnv.useDatabase(database);

        String[] tableList = tableEnv.listTables();
        for (String table : tableList) {
            System.out.println(table);
        }

        try {
            tableEnv.executeSql("CREATE TABLE t_src (\n" +
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
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } //try

        try {
            tableEnv.executeSql("drop TABLE t_dst");
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } //try

        try {
            tableEnv.executeSql("CREATE TABLE t_dst (\n" +
                    " userid int,\n" +
                    " f_random_str STRING\n" +
                    ") WITH (\n" +
                    " 'connector' = 'print'\n" +
                    ")");
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } //try


        System.out.println("--------------------------------");

        tableEnv.executeSql("INSERT INTO t_dst select * from t_src");
    }
}
