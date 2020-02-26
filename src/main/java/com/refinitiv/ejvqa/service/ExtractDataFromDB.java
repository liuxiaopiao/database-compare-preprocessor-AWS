package com.refinitiv.ejvqa.service;

import com.refinitiv.ejvqa.util.CommonUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.*;

public class ExtractDataFromDB {

    public void extractDataFromDb(String DBTag, String ip_port, String databaseName1, String username, String password, String tableNamePath, String schemaPattern, String filter,String extractNum, String fileDestPath){

        Connection connection=null;
        DatabaseMetaData databaseMetaData=null;
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
                int startNum=Integer.parseInt(extractNum);
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
                String selectContent="*";
                if(tableNameToColumnLabelMap.get(tableName)!=null||!"".equals(tableNameToColumnLabelMap.get(tableName))){
                    selectContent=tableNameToColumnLabelMap.get(tableName);
                }

                String sql=null;
                String path=fileDestPath+schemaPattern+"_"+tableName;
                File file=new File(path);
                if(file.exists()){
                    file.delete();
                    System.out.println("Delete Old File!");
                }

                if ("oracle".equalsIgnoreCase(DBTag)) {
                    long start = System.currentTimeMillis();
                    boolean flag=true;
                    while(flag) {
                        flag=false;
                        StringBuilder primaryKeys=new StringBuilder();
                        if (primaryKeyList.size() != 0) {
                            for (String primaryKey : primaryKeyList) {
                                primaryKeys.append(primaryKey + ",");
                            }
                            primaryKeys.deleteCharAt(primaryKeys.length() - 1);
                            System.out.println(primaryKeys.toString());


                            if (!"*".equalsIgnoreCase(extractNum)) {
                                sql = "select * from (select a.*,rownum rn from (select "+selectContent+" from " + schemaPattern + "." + tableName +filter+ " order by " + primaryKeys + ") a where rownum <" + (startNum + 10000000) + ") where rn >=" + startNum;
                            } else {
                                sql = "select "+selectContent+" from " + schemaPattern + "." + tableName +filter+ " order by " + primaryKeys;
                            }
                        } else {
                            if (!"*".equalsIgnoreCase(extractNum)) {
                                if (tableName.contains("COMMENT")) {
                                    sql = "select * from (select a.*,rownum rn from (select "+selectContent+" from " + schemaPattern + "." + tableName +filter+ " order by OBJECTID,EFFECTIVETO,EFFECTIVEFROM) a where rownum <" + (startNum + 10000000) + ") where rn >=" + startNum;
                                } else if (tableName.contains("OBJECTLIST")) {
                                    sql = "select * from (select "+selectContent+" from " + schemaPattern + "." + tableName +filter+ " order by OBJECTPATH) where  rownum <=" + extractNum;
                                }
                            } else {
                                if (tableName.contains("COMMENT")) {
                                    sql = "select "+selectContent+" from " + schemaPattern + "." + tableName +filter+ " order by OBJECTID,EFFECTIVETO,EFFECTIVEFROM";
                                } else if (tableName.contains("OBJECTLIST")) {
                                    sql = "select "+selectContent+" from " + schemaPattern + "." + tableName +filter+ " order by OBJECTPATH";
                                }
                            }
                        }
                        System.out.println(sql);
                        long dbstart=System.currentTimeMillis();
                        PreparedStatement preparedStatement= connection.prepareStatement(sql);
                        preparedStatement.setFetchSize(10000);
                        ResultSet resultSet=preparedStatement.executeQuery();
                        long dbend=System.currentTimeMillis();
                        System.out.println("Time to interact with the database is: "+(dbend-dbstart)+"ms");
//                        resultSet.last();
//                        int lastIndex=resultSet.getRow();
//                        System.out.println("The Extract number of records is: "+lastIndex);
//                        resultSet.beforeFirst();
                        ResultSetMetaData resultSetMetaData=resultSet.getMetaData();

                        long begin=System.currentTimeMillis();
                        long loopBegin=begin;
                        long timeuse=0;
                        System.out.println("Starting extract data......");

                        StringBuilder rowData=new StringBuilder();
                        List<String> pageData=new LinkedList<>();

                        int j=0;
                        int columnCount=resultSetMetaData.getColumnCount();

                        if(primaryKeyList.size()!=0){
                            rowData.append("PrimaryKey|");
                        }
                        for(int i=1;i<=columnCount;i++){
                            rowData.append(resultSetMetaData.getColumnLabel(i)+"|");
                        }
                        rowData.append("\r\n");

                        while(resultSet.next()){
                            flag=true;
                            startNum++;
                            j++;
                            if(primaryKeyList.size()!=0){
                                for(String primaryKey:primaryKeyList){
                                    rowData.append(resultSet.getString(primaryKey));
                                }
                                rowData.append("|");
                            }
                            for(int i=1;i<=columnCount;i++){
                                rowData.append(resultSet.getString(i)+"|");
                            }
                            rowData.append("\r\n");

                            pageData.add(rowData.toString());
                            rowData.setLength(0);

                            if(j%500000==0){
                                CommonUtil.writeFile(pageData,path);
                                pageData.clear();
                                timeuse=System.currentTimeMillis();
                                System.out.println("Output "+j+" rows,Timeuse: "+(timeuse-loopBegin)+"ms");
                                loopBegin=timeuse;
                            }
                        }

                        if(j%500000!=0){
                            CommonUtil.writeFile(pageData,path);
                        }

                        long end=System.currentTimeMillis();
                        System.out.println("Extraction total timeuse is: "+(end-begin)+"ms");
                        resultSet.close();
                        preparedStatement.close();
                    }

                    connection.close();

                    CommonUtil.gzipFile(path, schemaPattern, tableName);
                    long end = System.currentTimeMillis();
                    System.out.println("\r\nTotal Timeuse is: " + (end - start) + "ms");
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
