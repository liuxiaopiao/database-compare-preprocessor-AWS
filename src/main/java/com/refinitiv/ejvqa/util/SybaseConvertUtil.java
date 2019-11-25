package com.refinitiv.ejvqa.util;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class SybaseConvertUtil {

    public static String getSybasePrimaryKeyStr(LinkedHashSet<String> indexColumNameSet,LinkedHashMap<String,String> columnMap) {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> columnNameSet = columnMap.keySet();
        for (String columnName : columnNameSet) {
            if (indexColumNameSet.contains(columnName)) {
                if ("binary".equalsIgnoreCase(columnMap.get(columnName)) || "id_TY".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "'0x'+bintostr(" + columnName + ")+";
                } else if ("int".equalsIgnoreCase(columnMap.get(columnName)) || "smallint".equalsIgnoreCase(columnMap.get(columnName)) || "permid_num_TY".equalsIgnoreCase(columnMap.get(columnName)) || "numeric".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "convert(varchar," + columnName + ")+";
                } else if ("datetime".equalsIgnoreCase(columnMap.get(columnName)) || "smalldatetime".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "convert(varchar," + columnName + ")+";
                } else if ("date".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "convert(varchar(10)," + columnName + ",23)+";
                } else if ("float".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "str(" + columnName + ",20,6)+";
                } else if ("char".equalsIgnoreCase(columnMap.get(columnName)) || "varchar".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ltrim(rtrim(" + columnName + "))+";
                } else {
                    columnName = columnName + "+";
                }
                stringBuilder.append(columnName);
            }
        }
        String primaryKey = stringBuilder.substring(0, stringBuilder.length() - 1) + " as primary_key";
        System.out.println(primaryKey);
        return primaryKey;
    }

    public static String getSybaseConvertFieldStr(LinkedHashMap<String,String> columnMap) {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> columnNameSet = columnMap.keySet();
        for (String columnName : columnNameSet) {
            if ("binary".equalsIgnoreCase(columnMap.get(columnName)) || "id_TY".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "'0x'+bintostr(" + columnName + ") as " + columnName + ",";
            } else if ("int".equalsIgnoreCase(columnMap.get(columnName)) || "smallint".equalsIgnoreCase(columnMap.get(columnName)) || "permid_num_TY".equalsIgnoreCase(columnMap.get(columnName)) || "numeric".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "convert(varchar," + columnName + ") as " + columnName + ",";
            } else if ("date".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "convert(varchar(10)," + columnName + ",23) as " + columnName + ",";
            } else if ("datetime".equalsIgnoreCase(columnMap.get(columnName)) || "smalldatetime".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "convert(varchar," + columnName + ") as " + columnName + ",";
            } else if ("char".equalsIgnoreCase(columnMap.get(columnName)) || "varchar".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "ltrim(rtrim(" + columnName + ")) as " + columnName + ",";
            } else if ("text".equalsIgnoreCase(columnMap.get(columnName))){
                columnName = "str_replace(cast("+columnName+" as varchar(8000)),char(10),'') as "+columnName+",";
            } else {
                columnName = columnName + ",";
            }
            stringBuilder.append(columnName);
        }
        String convertFieldStr = stringBuilder.substring(0, stringBuilder.length() - 1);
        System.out.println(convertFieldStr);
        return convertFieldStr;
    }
}
