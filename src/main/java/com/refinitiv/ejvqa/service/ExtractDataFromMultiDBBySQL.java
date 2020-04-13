package com.refinitiv.ejvqa.service;

import com.refinitiv.ejvqa.util.CommonUtil;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

public class ExtractDataFromMultiDBBySQL {

    public void extractDataFromMultiDataBaseBySQL(String DBTag, String ip_port, String username, String password, String excelPath, String destPath, int startNum, int increment, String separator, String isExtractAll, String append) {

        LinkedHashSet<String> tableNameSet = new LinkedHashSet<>();
        LinkedHashMap<String, String> tableName2SQL = new LinkedHashMap<>();

        Connection connection = null;

        if (destPath.equalsIgnoreCase("local")) {
            destPath = System.getProperty("user.dir") + "/output/";
        }

        if (excelPath.endsWith(".xlsx")) {
            File excelFile = new File(excelPath);
            CommonUtil.getSQLFromExcel(excelFile, tableNameSet, tableName2SQL);
        }

        try {
            for (String tableName : tableNameSet) {
                System.out.println();
                System.out.println(tableName);
                String schema = tableName.split("_")[0];
                System.out.println(schema);
                String folder = tableName.replaceAll(schema+"_", "");
                System.out.println(folder);
                connection = CommonUtil.createConnection(DBTag, connection, ip_port, schema, username, password);

                String sql = tableName2SQL.get(tableName);
                String fileName = tableName + System.currentTimeMillis();
                String filePath = null;
                if(Boolean.valueOf(append)) {
                    filePath=destPath;
                }else {
                    filePath=destPath + fileName;
                }
                int beginNum=startNum;

                long start = System.currentTimeMillis();
                boolean flag = true;
                while (flag) {
                    flag = false;
                    int j = 0;
                    String query=null;
                    if("oracle".equalsIgnoreCase(DBTag)) {
                       query = "select * from (select a.*,rownum rn from (" + sql + ") a where rownum<" + (beginNum + increment) + ") where rn>=" + beginNum;
                    }else if("mysql".equalsIgnoreCase(DBTag)){
                        query=sql+" limit "+beginNum+","+(beginNum + increment);
                    }else if("mssql".equalsIgnoreCase(DBTag)){
                        query="select top "+increment+" * from (select *,row_number() over(order by 1 asc) as rownumber from ("+sql+")) a where rownumber>"+beginNum;
                    }else if("sybase".equalsIgnoreCase(DBTag)){
                        query="select * from ("+sql+") a where a.1 between "+beginNum+" and "+(beginNum+increment);
                    }else{
                        System.out.println("The database is not currently supported!");
                    }
                    System.out.println(query);

                    long dbbegin = System.currentTimeMillis();
                    PreparedStatement preparedStatement = connection.prepareStatement(query);
                    preparedStatement.setFetchSize(10000);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                    int columnCount = resultSetMetaData.getColumnCount();
                    long dbend = System.currentTimeMillis();
                    System.out.println("Time to interact with the database is: " + (dbend - dbbegin) + "ms");

                    long begin = System.currentTimeMillis();
                    long loopBegin = begin;
                    long loopEnd = 0;
                    System.out.println("Starting extract data......");

                    StringBuilder rowData = new StringBuilder();
                    List<String> pageData = new LinkedList<>();

                    for (int i = 1; i < columnCount; i++) {
                        rowData.append(resultSetMetaData.getColumnLabel(i) + separator);
                    }
                    rowData.append("\r\n");

                    while (resultSet.next()) {
                        flag = true;
                        j++;

                        for (int i = 1; i < columnCount; i++) {
                            if (resultSet.getString(i) != null) {
                                rowData.append((resultSet.getString(i)).replaceAll("\r", "").replaceAll("\n", "").replaceAll("\r\n", "") + separator);
                            } else {
                                rowData.append(resultSet.getString(i) + separator);
                            }
                        }
                        rowData.append("\r\n");

                        pageData.add(rowData.toString());
                        rowData.setLength(0);

                        if (j % 500000 == 0) {
                            CommonUtil.writeFile(pageData, filePath);
                            pageData.clear();
                            loopEnd = System.currentTimeMillis();
                            System.out.println("Output " + j + " rows,Timeuse: " + (loopEnd - loopBegin) + "ms");
                            loopBegin = loopEnd;
                        }
                    }

                    if (j % 500000 != 0) {
                        CommonUtil.writeFile(pageData, filePath);
                        pageData.clear();
                        loopEnd = System.currentTimeMillis();
                        System.out.println("Output " + j + " rows,Timeuse: " + (loopEnd - loopBegin) + "ms");
                    }

                    long end = System.currentTimeMillis();
                    System.out.println("Extraction PageData Timeuse: " + (end - begin) + "ms");
                    resultSet.close();
                    preparedStatement.close();

                    beginNum=beginNum+increment;
                    if (Boolean.valueOf(isExtractAll)) {

                    } else {
                        flag = false;
                    }
                }

                connection.close();

                CommonUtil.gzipFile(filePath, folder, fileName);
                long fin = System.currentTimeMillis();
                System.out.println("\r\nTotal Timeuse: " + (fin - start) + "ms");

            }

            CommonUtil.deleteOnlyFile(destPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
