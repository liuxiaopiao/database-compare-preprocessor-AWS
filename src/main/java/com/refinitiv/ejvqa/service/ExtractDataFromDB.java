package com.refinitiv.ejvqa.service;

import com.refinitiv.ejvqa.util.CommonUtil;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.LinkedList;

public class ExtractDataFromDB {

    public void extractDataFromDb(String DBTag, String ip_port, String databaseName1, String username, String password, String tableNamePath, String schemaPattern, String extractNum, String fileDestPath){

        Connection connection=null;
        DatabaseMetaData databaseMetaData=null;
        Statement statement=null;
        LinkedHashSet<String> tableNameSet=null;
        LinkedList<String> primaryKeyList=null;
        FileOutputStream fileOutputStream=null;
        if (fileDestPath.equalsIgnoreCase("local")) {
            fileDestPath = System.getProperty("user.dir") + "/output/";
        }
        try {
            connection = CommonUtil.createConnection(DBTag, connection, ip_port, databaseName1, username, password);
            databaseMetaData = CommonUtil.generateDatabaseMetaData(connection);

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
                System.out.println(tableName);
                connection = CommonUtil.createConnection(DBTag, connection, ip_port, databaseName1, username, password);
                databaseMetaData = CommonUtil.generateDatabaseMetaData(connection);
                primaryKeyList=CommonUtil.generatePrimaryKeys(databaseMetaData,schemaPattern,tableName);
                StringBuilder primaryKeys=new StringBuilder();
                String sql=null;
                statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
                statement.setFetchSize(1000);
                String path=fileDestPath+schemaPattern+"_"+tableName;

                if ("oracle".equalsIgnoreCase(DBTag)) {
                    if(primaryKeyList.size()!=0){
                        for(String primaryKey:primaryKeyList){
                            primaryKeys.append(primaryKey+",");
                        }
                        primaryKeys.deleteCharAt(primaryKeys.length()-1);
                        System.out.println(primaryKeys.toString());

                        if(!"*".equalsIgnoreCase(extractNum)) {
                            sql = "select * from (select * from " + schemaPattern + "." + tableName + " order by " + primaryKeys + ") where rownum <=" + extractNum;
                        }else{
                            sql ="select * from "+schemaPattern+"."+tableName+" order by "+primaryKeys;
                        }
                    }
//                    else {
//                        if(!"*".equalsIgnoreCase(extractNum)) {
//                            sql = "select * from (select * from " + schemaPattern + "." + tableName + " order by OBJECTPERMID) where  rownum <=" + extractNum;
//                        }else{
//                            sql="select * from " + schemaPattern + "." + tableName + " order by OBJECTPERMID";
//                        }
//                    }
                    System.out.println(sql);

                    CommonUtil.extractDataBySQL(statement,sql,path);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
