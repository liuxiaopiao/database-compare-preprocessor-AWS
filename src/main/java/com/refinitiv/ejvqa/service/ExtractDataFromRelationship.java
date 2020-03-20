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

public class ExtractDataFromRelationship {


    public void extractDataBySQL(String DBTag,String ip_port,String username,String password,String excelPath,String destPath,int startNum,int increment,String separator){

        LinkedHashMap<String,String> ecpId2RelaTypeName=new LinkedHashMap<>();
        LinkedHashMap<String,String> ecpId2RelaTypeId=new LinkedHashMap<>();

        ecpId2RelaTypeName.put("ecp:5-e530701d-8ced-436a-a912-e6dfa3039b66","IsIssuedBy");
        ecpId2RelaTypeId.put("ecp:5-e530701d-8ced-436a-a912-e6dfa3039b66","310120");

        ecpId2RelaTypeName.put("ecp:5-f96cdf17-f915-4016-8118-66ea34106ac5","HasTransparencyWaiverFrom");
        ecpId2RelaTypeId.put("ecp:5-f96cdf17-f915-4016-8118-66ea34106ac5","1003436032");

        ecpId2RelaTypeName.put("ecp:5-d7ee3f5a-caff-45a5-95f6-1dc5ca454d74","IsEligibleToTradeOn");
        ecpId2RelaTypeId.put("ecp:5-d7ee3f5a-caff-45a5-95f6-1dc5ca454d74","1003204957");

        ecpId2RelaTypeName.put("ecp:5-6322bca6-01f5-4b4e-a718-4c3067ec4cf3","IsEligibleToTradeOn");
        ecpId2RelaTypeId.put("ecp:5-6322bca6-01f5-4b4e-a718-4c3067ec4cf3","1003436402");

        ecpId2RelaTypeName.put("ecp:5-4dfbf215-a6fa-4d13-a9cc-d532e6ebc7e8","HasReferenceObligationOf");
        ecpId2RelaTypeId.put("ecp:5-4dfbf215-a6fa-4d13-a9cc-d532e6ebc7e8","1003442910");

        ecpId2RelaTypeName.put("ecp:5-c2de14a0-6832-43e8-bab3-877d8a997537","HasFloatIndexOf");
        ecpId2RelaTypeId.put("ecp:5-c2de14a0-6832-43e8-bab3-877d8a997537","1003442908");

        ecpId2RelaTypeName.put("ecp:5-8044a35f-f0ef-442c-9676-9514e24a5b7b","HasAddedInfoIn");
        ecpId2RelaTypeId.put("ecp:5-8044a35f-f0ef-442c-9676-9514e24a5b7b","1003240168");

        ecpId2RelaTypeName.put("ecp:5-8da02f61-1de0-45d4-b99b-b28929815652","IsQuoteOf");
        ecpId2RelaTypeId.put("ecp:5-8da02f61-1de0-45d4-b99b-b28929815652","310080");

        ecpId2RelaTypeName.put("ecp:5-72589c8b-61f8-4d02-a6ed-984aeec94fa7","IsQuotedBy");
        ecpId2RelaTypeId.put("ecp:5-72589c8b-61f8-4d02-a6ed-984aeec94fa7","1003204953");

        ecpId2RelaTypeName.put("ecp:5-e382f5ec-4e56-4e22-871d-60a8165261c0","IsSourcedFrom");
        ecpId2RelaTypeId.put("ecp:5-e382f5ec-4e56-4e22-871d-60a8165261c0","1000276408");

        ecpId2RelaTypeName.put("ecp:5-895cebac-14d9-486f-903f-02874364eae9","IsDuplicateOf");
        ecpId2RelaTypeId.put("ecp:5-895cebac-14d9-486f-903f-02874364eae9","310027");

        ecpId2RelaTypeName.put("ecp:5-93dfcb2f-dac1-4e23-a4e9-16ec364fec16","IsDuplicateOf");
        ecpId2RelaTypeId.put("ecp:5-93dfcb2f-dac1-4e23-a4e9-16ec364fec16","310027");

        ecpId2RelaTypeName.put("ecp:5-fa2003eb-fb97-4942-971e-2e191a830588","Instrument");
        ecpId2RelaTypeId.put("ecp:5-fa2003eb-fb97-4942-971e-2e191a830588","404016");

        ecpId2RelaTypeName.put("ecp:5-07f838b8-d9a6-4fe1-9de9-dcabc02d21ce","ecpdataset");
        ecpId2RelaTypeId.put("ecp:5-07f838b8-d9a6-4fe1-9de9-dcabc02d21ce",null);

        ecpId2RelaTypeName.put("ecp:5-184be383-481a-4b87-a03f-2dbfd8adba51","MarketAttributableSource");
        ecpId2RelaTypeId.put("ecp:5-184be383-481a-4b87-a03f-2dbfd8adba51","1001988382");

        ecpId2RelaTypeName.put("ecp:5-63196068-b053-4407-bb98-fa37229e541c","Organization");
        ecpId2RelaTypeId.put("ecp:5-63196068-b053-4407-bb98-fa37229e541c","404010");

        ecpId2RelaTypeName.put("ecp:5-1a8ab289-d526-48c5-a061-07ca7d253db4","Quote");
        ecpId2RelaTypeId.put("ecp:5-1a8ab289-d526-48c5-a061-07ca7d253db4","404019");

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
                String folder=tableName.replaceAll("CCPASDI_","").replaceAll("CIQM_","").replaceAll("CCPOLLSDI_","");
                System.out.println(folder);
                connection = CommonUtil.createConnection(DBTag, connection, ip_port, schema, username, password);


                String sql = tableName2SQL.get(tableName);
                String fileName = tableName + System.currentTimeMillis();
                String filePath = destPath + fileName;

                if ("oracle".equalsIgnoreCase(DBTag)) {
                    long start = System.currentTimeMillis();
                    boolean flag = true;
                    while (flag) {
                        flag = false;
                        int j = 0;

                        String query = "select * from (select a.*,rownum rn from (" + sql + ") a where rownum<" + (startNum + increment) + ") where rn>=" + startNum;
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
                            startNum++;
                            j++;

                            for (int i = 1; i < columnCount; i++) {
                                if (resultSet.getString(i) != null) {
                                    if(resultSetMetaData.getColumnLabel(i).equalsIgnoreCase("RELATIONSHIPTYPEID")||resultSetMetaData.getColumnLabel(i).equalsIgnoreCase("RELATIONOBJECTTYPEID")||resultSetMetaData.getColumnLabel(i).equalsIgnoreCase("RELATEDOBJECTTYPEID")){
                                        String id=ecpId2RelaTypeId.get(resultSet.getString(i));
                                        rowData.append(id+separator);
                                    }else {
                                        rowData.append((resultSet.getString(i)).replaceAll("\r", "").replaceAll("\n", "").replaceAll("\r\n", "") + separator);
                                    }
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
                        if(startNum==1&&increment==10000000){

                        }else {
                            flag=false;
                        }
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
