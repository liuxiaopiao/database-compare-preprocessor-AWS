package com.refinitiv.ejvqa.service;

import com.refinitiv.ejvqa.util.CommonUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;

public class ExtractDataFromDB {

    public void extractDataFromDb(String DBTag, String ip_port, String databaseName1, String username, String password, String tableNamePath, String schemaPattern, String extractNum, String fileDestPath){

        Connection connection=null;
        DatabaseMetaData databaseMetaData=null;
        Statement statement=null;
        LinkedHashSet<String> tableNameSet=new LinkedHashSet<>();
        LinkedHashMap<String,String> tableNameToPrimaryKeyMap=new LinkedHashMap<>();
        LinkedHashMap<String,String> tableNameToColumnLabelMap=new LinkedHashMap<>();
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
                File file=new File(tableNamePath);
                CommonUtil.getSQLInfoFromExcel(file,tableNameSet,tableNameToPrimaryKeyMap,tableNameToColumnLabelMap);
            } else {
                tableNameSet = new LinkedHashSet<String>();
                tableNameSet.add(tableNamePath);
            }

            for (String tableName : tableNameSet) {
                System.out.println();
                System.out.println(tableName);
                LinkedList<String> primaryKeyList=new LinkedList<>();
                connection = CommonUtil.createConnection(DBTag, connection, ip_port, databaseName1, username, password);
                databaseMetaData = CommonUtil.generateDatabaseMetaData(connection);
                if(tableNamePath.endsWith(".xlsx")){
                    String[] primarykeys=tableNameToPrimaryKeyMap.get(tableName).split(",");
                    for(int i=0;i<primarykeys.length;i++) {
                        primaryKeyList.add(primarykeys[i]);
                    }
                }else {
                    primaryKeyList = CommonUtil.generatePrimaryKeys(databaseMetaData, schemaPattern, tableName);
                }
                StringBuilder primaryKeys=new StringBuilder();
                String sql=null;
                statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
                statement.setFetchSize(1000);
                String path=fileDestPath+schemaPattern+"_"+tableName;
                File file=new File(path);
                if(file.exists()){
                    file.delete();
                    System.out.println("Delete Old File!");
                }

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
                    else {
                        if(!"*".equalsIgnoreCase(extractNum)) {
                            if(tableName.contains("COMMENT")) {
                                sql = "select * from (select * from " + schemaPattern + "." + tableName + " order by OBJECTID,EFFECTIVETO,EFFECTIVEFROM) where  rownum <=" + extractNum;
                            }else if(tableName.contains("OBJECTLIST")){
                                sql = "select * from (select * from " + schemaPattern + "." + tableName + " order by OBJECTPATH) where  rownum <=" + extractNum;
                            }
                        }else{
                            if(tableName.contains("COMMENT")) {
                                sql = "select * from " + schemaPattern + "." + tableName + " order by OBJECTID,EFFECTIVETO,EFFECTIVEFROM";
                            }else if(tableName.contains("OBJECTLIST")){
                                sql = "select * from " + schemaPattern + "." + tableName + " order by OBJECTPATH";
                            }
                        }
                    }
                    System.out.println(sql);

                    long start=System.currentTimeMillis();
                    CommonUtil.extractDataBySQL(statement,sql,path,primaryKeyList);
                    CommonUtil.gzipFile(path,schemaPattern,tableName);
                    long end=System.currentTimeMillis();
                    System.out.println("\r\nTotal Timeuse is: "+(end-start)+"ms");
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
