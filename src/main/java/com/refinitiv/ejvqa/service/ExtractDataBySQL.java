package com.refinitiv.ejvqa.service;

import com.refinitiv.ejvqa.util.CommonUtil;

import java.io.File;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

public class ExtractDataBySQL {

    public void extractDataBySQL(String DBTag,String ip_port,String username,String password,String excelPath,String destPath){

        LinkedHashSet<String> tableNameSet=new LinkedHashSet<>();
        LinkedHashMap<String,String> tableName2SQL=new LinkedHashMap<>();

        Connection connection=null;

        if(destPath.equalsIgnoreCase("local")){
            destPath=System.getProperty("user.dir")+"/output/";
        }

        if(excelPath.endsWith(".xlsx")){
            File excelFile=new File(excelPath);
            CommonUtil.getSQLFromExcel(excelFile,tableNameSet,tableName2SQL);
        }

        try {
            for (String tableName : tableNameSet) {
                System.out.println();
                System.out.println(tableName);
                String schema = tableName.split("_")[0];
                System.out.println(schema);
                String folder=tableName.replaceAll("CCPASDI_","").replaceAll("CIQM_","").replaceAll("CCPOLLSDI","");
                System.out.println(folder);
                connection = CommonUtil.createConnection(DBTag, connection, ip_port, schema, username, password);

                int startNum = 1;
                String sql = tableName2SQL.get(tableName);
                String fileName = tableName + System.currentTimeMillis();
                String filePath = destPath + fileName;

                if ("oracle".equalsIgnoreCase(DBTag)) {
                    long start = System.currentTimeMillis();
                    boolean flag = true;
                    while (flag) {
                        flag = false;
                        int j = 0;

                        String query = "select * from (select a.*,rownum rn from (" + sql + ") a where rownum<" + (startNum + 10000000) + ") where rn>=" + startNum;
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
                            rowData.append(resultSetMetaData.getColumnLabel(i) + "|");
                        }
                        rowData.append("\r\n");

                        while (resultSet.next()) {
                            flag = true;
                            startNum++;
                            j++;

                            for (int i = 1; i < columnCount; i++) {
                                if (resultSet.getString(i) != null) {
                                    rowData.append((resultSet.getString(i)).replaceAll("\r", "").replaceAll("\n", "").replaceAll("\r\n", "") + "|");
                                } else {
                                    rowData.append(resultSet.getString(i) + "|");
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
                    }

                    connection.close();

                    CommonUtil.gzipFile(filePath, folder, fileName);
                    long fin = System.currentTimeMillis();
                    System.out.println("\r\nTotal Timeuse: " + (fin - start) + "ms");
                }
            }

            CommonUtil.deleteOnlyFile(destPath);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
