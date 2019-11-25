package com.refinitiv.ejvqa.util;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class OracleConvertUtil {

    public static String getOraclePrimaryKeyStr(LinkedHashSet<String> indexColumNameSet, LinkedHashMap<String,String> columnMap) {
        StringBuilder str = new StringBuilder();
        Set<String> columnNameSet = columnMap.keySet();
        for (String columnName : columnNameSet) {
            if (indexColumNameSet.contains(columnName)) {
                if ("FLOAT".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "to_char(" + columnName + ",'999999999990D9999999999999')||";
                } else if (columnMap.get(columnName).contains("TIMESTAMP")) {
                    columnName = "to_char(" + columnName + ",'yyyy-MM-DD HH24:MI:SS.FF3')||";
                } else {
                    columnName = columnName + "||";
                }
                str.append(columnName);
            }
        }
        String primaryKey = str.substring(0, str.length() - 2) + " as primary_key";
        System.out.println(primaryKey);
        return primaryKey;
    }

    public static String getOracleConvertFieldStr(LinkedHashMap<String,String> columnMap) {
        StringBuilder sb = new StringBuilder();
        Set<String> fcolumnnameSet = columnMap.keySet();
        for (String columnName : fcolumnnameSet) {
            if ("FLOAT".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "to_char(" + columnName + ",'999999999990D9999999999999') as " + columnName + ",";
            }
//            else if (columnMap.get(columnName).contains("TIMESTAMP")) {
//                columnName = "to_char(" + columnName + ",'dd-MON-yy HH:MI:SS:FF9 AM') as " + columnName + ",";
//            }
            else if (columnMap.get(columnName).contains("TIMESTAMP")) {
                columnName = "to_char(" + columnName + ",'yyyy-MM-DD HH24:MI:SS.FF3') as " + columnName + ",";
            } else {
                columnName = columnName + ",";
            }
            sb.append(columnName);
        }
        String fieldconvertstr = sb.substring(0, sb.length() - 1);
        System.out.println(fieldconvertstr);
        return fieldconvertstr;
    }
}
