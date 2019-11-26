package com.refinitiv.ejvqa.service;

import com.refinitiv.ejvqa.util.CommonUtil;
import com.refinitiv.ejvqa.util.MssqlConvertUtil;
import com.refinitiv.ejvqa.util.OracleConvertUtil;
import com.refinitiv.ejvqa.util.SybaseConvertUtil;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class ViewFileGenerationService {

    public boolean generateView(String DBTag, String ip_port, String databaseName1, String databaseName2, String username, String password, String tableNamePath, String schemaPattern, String extractNum, String fileDestPath) {
        boolean flag = true;
        Connection connection = null;
        DatabaseMetaData databaseMetaData = null;
        Statement statement = null;
        LinkedHashSet<String> tableNameSet = null;
        String primaryKey = null;
        String convertField = null;
        String createViewSql = "";
        String selectViewSql = "";
        try {
            connection = CommonUtil.createConnection(DBTag, connection, ip_port, databaseName1, username, password);
            databaseMetaData = CommonUtil.generateDatabaseMetaData(connection);
            statement = connection.createStatement();


            if ("All".equalsIgnoreCase(tableNamePath)) {
                tableNameSet = CommonUtil.generateTableNames(databaseMetaData, schemaPattern);
            } else if (tableNamePath == null) {
                tableNamePath = System.getProperty("user.dir") + "/src/data/TableName.xlsx";
                tableNameSet = CommonUtil.generateTableNameFromExcel(tableNamePath);
            } else if (tableNamePath.endsWith(".xlsx")) {
                tableNameSet = CommonUtil.generateTableNameFromExcel(tableNamePath);
            } else {
                tableNameSet = new LinkedHashSet<String>();
                tableNameSet.add(tableNamePath);
            }

            for (String tableName : tableNameSet) {
                String viewName = tableName + "_ViewJ";
                LinkedHashSet<String> indexColumnNameSet = CommonUtil.generateIndexColumnNames(databaseMetaData, tableName);
                LinkedHashMap<String, String> columnMap = CommonUtil.generateColumnInfo(databaseMetaData, tableName);
                if ("sybase".equalsIgnoreCase(DBTag)) {
                    primaryKey = SybaseConvertUtil.getSybasePrimaryKeyStr(indexColumnNameSet, columnMap);
                    convertField = SybaseConvertUtil.getSybaseConvertFieldStr(columnMap);
                    createViewSql = "create view " + viewName + " as select " + primaryKey + "," + convertField + " from " + databaseName1 + ".." + tableName;
                    if (extractNum == null || "".equalsIgnoreCase(extractNum)) {
                        System.out.println("请输入*或正整数!!!");
                    } else if ("*".equalsIgnoreCase(extractNum)) {
                        selectViewSql = "select * from " + viewName + " order by primary_key";
                    } else {
                        selectViewSql = "select top " + extractNum + " * from " + viewName + " order by primary_key";
                    }
                } else if ("mssql".equalsIgnoreCase(DBTag)) {
                    primaryKey = MssqlConvertUtil.getSqlserverPrimaryKeyStr(indexColumnNameSet, columnMap);
                    convertField = MssqlConvertUtil.getSqlserverConvertFieldStr(columnMap);
                    createViewSql = "create view " + viewName + " as select " + primaryKey + "," + convertField + " from " + databaseName1 + ".." + tableName;
                    if (extractNum == null || "".equalsIgnoreCase(extractNum)) {
                        System.out.println("请输入*或正整数!!!");
                    } else if ("*".equalsIgnoreCase(extractNum)) {
                        selectViewSql = "select * from " + viewName + " order by primary_key";
                    } else {
                        selectViewSql = "select top " + extractNum + " * from " + viewName + " order by primary_key";
                    }
                } else if ("oracle".equalsIgnoreCase(DBTag)) {
                    primaryKey = OracleConvertUtil.getOraclePrimaryKeyStr(indexColumnNameSet, columnMap);
                    convertField = OracleConvertUtil.getOracleConvertFieldStr(columnMap);
                    createViewSql = "create view " + viewName + " as select " + primaryKey + "," + convertField + " from " + tableName;
                    if (extractNum == null || "".equalsIgnoreCase(extractNum)) {
                        System.out.println("请输入*或正整数!!!");
                    } else if ("*".equalsIgnoreCase(extractNum)) {
                        selectViewSql = "select * from " + viewName + " order by primary_key";
                    } else {
                        selectViewSql = "select * from (select * from " + viewName + " order by primary_key) where rownum<=" + extractNum;
                    }
                } else {
                    System.out.println("The database is not currently supported!");
                }
                if (databaseName1 != null && databaseName2 != null) {
                    connection = CommonUtil.createConnection(DBTag, connection, ip_port, databaseName2, username, password);
                    databaseMetaData = CommonUtil.generateDatabaseMetaData(connection);
                    statement = connection.createStatement();
                }
                CommonUtil.generateViewNames(databaseMetaData, schemaPattern);
                CommonUtil.deleteView(statement, viewName);
                CommonUtil.createViewFromDatabase(statement, viewName, createViewSql);
                if (fileDestPath.equalsIgnoreCase("local")) {
                    fileDestPath = System.getProperty("user.dir") + "/output/";
                }
                CommonUtil.extractDataFromView(DBTag, statement, selectViewSql, viewName, fileDestPath);
                CommonUtil.deleteFile(fileDestPath);
                connection = CommonUtil.createConnection(DBTag, connection, ip_port, databaseName1, username, password);
                databaseMetaData = CommonUtil.generateDatabaseMetaData(connection);
                statement = connection.createStatement();
                System.out.println();
            }
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            flag = false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            flag = false;
        }
        return flag;
    }
}
