package com.refinitiv.ejvqa.util;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class MssqlConvertUtil {

    public static String getSqlserverPrimaryKeyStr(LinkedHashSet<String> indexColumNameSet, LinkedHashMap<String,String> columnMap) {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> columnNameSet = columnMap.keySet();
        for (String columnName : columnNameSet) {
            if (indexColumNameSet.contains(columnName)) {
                if ("binary".equalsIgnoreCase(columnMap.get(columnName)) || "id_TY".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ISNULL(master.dbo.fn_varbintohexstr(" + columnName + "),'0x')+";
                } else if ("int".equalsIgnoreCase(columnMap.get(columnName)) || "smallint".equalsIgnoreCase(columnMap.get(columnName)) || "permid_num_TY".equalsIgnoreCase(columnMap.get(columnName)) || "numeric".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ISNULL(convert(varchar," + columnName + "),'')+";
                } else if ("datetime".equalsIgnoreCase(columnMap.get(columnName)) || "smalldatetime".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ISNULL(convert(varchar," + columnName + "),'')+";
                } else if ("float".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ISNULL(str(" + columnName + ",20,6),'')+";
                } else if ("char".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ISNULL(ltrim(rtrim(" + columnName + ")),'')+";
                } else {
                    columnName = "ISNULL(" + columnName + ",'')+";
                }
                stringBuilder.append(columnName);
            }

        }
        String primaryKey = stringBuilder.substring(0, stringBuilder.length() - 1) + " as primary_key";
        System.out.println(primaryKey);
        return primaryKey;
    }

    public static String getSqlserverConvertFieldStr(LinkedHashMap<String,String> columnMap) {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> columnNameSet = columnMap.keySet();
        for (String columnName : columnNameSet) {
            if ("binary".equalsIgnoreCase(columnMap.get(columnName)) || "id_TY".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "ISNULL(master.dbo.fn_varbintohexstr(" + columnName + "),'0x') as " + columnName + ",";
            } else if ("int".equalsIgnoreCase(columnMap.get(columnName)) || "smallint".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "convert(varchar," + columnName + ") as " + columnName + ",";
            } else if ("datetime".equalsIgnoreCase(columnMap.get(columnName)) || "smalldatetime".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "convert(varchar," + columnName + ") as " + columnName + ",";
            } else if ("float".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "str(" + columnName + ",20,6) as " + columnName + ",";
            } else if ("char".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "ltrim(rtrim(" + columnName + ")) as " + columnName + ",";
            } else {
                columnName = columnName + ",";
            }
            stringBuilder.append(columnName);
        }
        String convertFieldStr = stringBuilder.substring(0, stringBuilder.length() - 1);
        System.out.println(convertFieldStr);
        return convertFieldStr;
    }

    public static String getMssqlserverPrimaryKeyStr(LinkedHashSet<String> indexColumNameSet, LinkedHashMap<String,String> columnMap) {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> columnNameSet = columnMap.keySet();
        for (String columnName : columnNameSet) {
            if (indexColumNameSet.contains(columnName)) {
                if ("datetime".equalsIgnoreCase(columnMap.get(columnName)) || "smalldatetime".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ISNULL(convert(varchar," + columnName + ",121),'')+";
                }
                else if ("int".equalsIgnoreCase(columnMap.get(columnName)) || "smallint".equalsIgnoreCase(columnMap.get(columnName)) || "tinyint".equalsIgnoreCase(columnMap.get(columnName)) || "bigint".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ISNULL(convert(varchar," + columnName + "),'')+";
                }
//                else if ("binary".equalsIgnoreCase(columnMap.get(columnName)) || "id_TY".equalsIgnoreCase(columnMap.get(columnName))) {
//                    columnName = "ISNULL(master.dbo.fn_varbintohexstr(" + columnName + "),'0x')+";
//                }
                else if ("float".equalsIgnoreCase(columnMap.get(columnName))) {
                    columnName = "ISNULL(str(" + columnName + ",27,13),'')+";
                }
//                else if ("char".equalsIgnoreCase(columnMap.get(columnName))) {
//                    columnName = "ISNULL(ltrim(rtrim(" + columnName + ")),'')+";
//                }
                else {
                    columnName = "ISNULL(" + columnName + ",'')+";
                }
                stringBuilder.append(columnName);
            }

        }
        String primaryKey = stringBuilder.substring(0, stringBuilder.length() - 1) + " as primary_key";
        System.out.println(primaryKey);
        return primaryKey;
    }

    public static String getMssqlserverConvertFieldStr(LinkedHashMap<String,String> columnMap) {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> columnNameSet = columnMap.keySet();
        for (String columnName : columnNameSet) {
            if ("datetime".equalsIgnoreCase(columnMap.get(columnName)) || "smalldatetime".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "convert(varchar," + columnName + ",121) as " + columnName + ",";
            }
            else if ("int".equalsIgnoreCase(columnMap.get(columnName)) || "smallint".equalsIgnoreCase(columnMap.get(columnName))||"tinyint".equalsIgnoreCase(columnMap.get(columnName)) || "bigint".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "convert(varchar," + columnName + ") as " + columnName + ",";
            }
//            else if ("binary".equalsIgnoreCase(columnMap.get(columnName)) || "id_TY".equalsIgnoreCase(columnMap.get(columnName))) {
//                columnName = "ISNULL(master.dbo.fn_varbintohexstr(" + columnName + "),'0x') as " + columnName + ",";
//            }
            else if ("float".equalsIgnoreCase(columnMap.get(columnName))) {
                columnName = "str(" + columnName + ",27,13) as " + columnName + ",";
            }
//            else if ("char".equalsIgnoreCase(columnMap.get(columnName))) {
//                columnName = "ltrim(rtrim(" + columnName + ")) as " + columnName + ",";
//            }
            else {
                columnName = columnName + ",";
            }
            stringBuilder.append(columnName);
        }
        String convertFieldStr = stringBuilder.substring(0, stringBuilder.length() - 1);
        System.out.println(convertFieldStr);
        return convertFieldStr;
    }

}
