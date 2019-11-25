package com.refinitiv.ejvqa.util;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.zip.GZIPOutputStream;

/**
 * Common util for create View
 */
public class CommonUtil {

    public static LinkedHashSet<String> tablNameSet;
    public static LinkedHashSet<String> viewNameSet;
    public static LinkedHashSet<String> indexColumNameSet;
    public static LinkedHashMap<String, String> columnMap;
    public static final String DatabaseName1 = "scratch";
    public static final String DatabaseName2 = "data_creditds";

    public static Connection createConnection(String DBTag, Connection conn, String ip_port, String databaseName, String username, String password) throws SQLException, ClassNotFoundException {
        String url = "";
        if ("mysql".equalsIgnoreCase(DBTag)) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            url = "jdbc:mysql://" + ip_port + "/" + databaseName;
        } else if ("sybase".equalsIgnoreCase(DBTag)) {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            url = "jdbc:jtds:sybase://" + ip_port + "/" + databaseName;
        } else if ("mssql".equalsIgnoreCase(DBTag)) {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            url = "jdbc:sqlserver://" + ip_port + ";databaseName=" + databaseName;
        } else if ("oracle".equalsIgnoreCase(DBTag)) {
            Class.forName("oracle.jdbc.OracleDriver");
            url = "jdbc:oracle:thin:@" + ip_port + ":orcl";
        } else {
            System.out.println("The database is not currently supported!");
        }

        conn = DriverManager.getConnection(url, username, password);
        if (conn != null) {
            System.out.println("Connect successfully!");
            return conn;
        }
        System.out.println("Connect failed!");
        return null;
    }

    public static DatabaseMetaData generateDatabaseMetaData(Connection conn) throws SQLException {
        DatabaseMetaData dbmd = conn.getMetaData();
        if (dbmd != null) {
            System.out.println("Get DatabaseMetaData successfully!");
            return dbmd;
        }
        System.out.println("Fail to get DatabaseMetaData!");
        return null;
    }

    public static LinkedHashSet<String> generateTableNames(DatabaseMetaData databaseMetaData, String schemaPattern) throws SQLException {
        tablNameSet = new LinkedHashSet<String>();
        ResultSet tableSet = databaseMetaData.getTables(null, schemaPattern, "%", new String[]{"TABLE"});
        while (tableSet.next()) {
            tablNameSet.add(tableSet.getString("TABLE_NAME"));
        }
        System.out.println(tablNameSet);
        return tablNameSet;
    }

    public static LinkedHashSet<String> generateTableNameFromExcel(String filepath) {
        LinkedHashSet tableNameSet = new LinkedHashSet();
        File file = new File(filepath);
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            if (file.exists() && file.getName().contains(".xlsx")) {
                XSSFWorkbook wb = new XSSFWorkbook(fis);
                XSSFSheet sheet = wb.getSheetAt(0);
                for (int i = 1; i < sheet.getPhysicalNumberOfRows(); i++) {
                    Row oneRow = sheet.getRow(i);
                    Cell tableName = oneRow.getCell(0);
                    if (tableName != null) {
                        tableNameSet.add(tableName.getStringCellValue());
                    }

                }
                wb.close();
            } else {
                throw new FileNotFoundException("The file not Found");
            }
            System.out.println(tableNameSet);
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tableNameSet;
    }

    public static LinkedHashSet<String> generateViewNames(DatabaseMetaData databaseMetaData, String schemaPattern) throws SQLException {
        viewNameSet = new LinkedHashSet<String>();
        ResultSet viewSet = databaseMetaData.getTables(null, schemaPattern, "%", new String[]{"VIEW"});
        while (viewSet.next()) {
            viewNameSet.add(viewSet.getString("TABLE_NAME"));
        }
        System.out.println(viewNameSet);
        return viewNameSet;
    }

    public static LinkedHashSet<String> generateIndexColumnNames(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        indexColumNameSet = new LinkedHashSet<String>();
        ResultSet indexSet = databaseMetaData.getIndexInfo(null, null, tableName, true, true);
        while (indexSet.next()) {
            if (indexSet.getString("COLUMN_NAME") != null) {
                indexColumNameSet.add(indexSet.getString("COLUMN_NAME"));
            }
        }
        System.out.println(indexColumNameSet);
        return indexColumNameSet;
    }

    public static LinkedHashMap<String, String> generateColumnInfo(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        columnMap = new LinkedHashMap<String, String>();
        ResultSet columnSet = databaseMetaData.getColumns(null, null, tableName, "%");
        while (columnSet.next()) {
            columnMap.put(columnSet.getString("COLUMN_NAME"), columnSet.getString("TYPE_NAME"));
        }
        System.out.println(columnMap);
        return columnMap;
    }

    public static void createViewFromDatabase(Statement statement, String viewName, String sql) throws SQLException {
        System.out.println(sql);
        if (statement.executeUpdate(sql) == 0) {
            System.out.println("create " + viewName + " successfully!");
        } else {
            System.out.println("fail to create " + viewName + "!");
        }
    }

    public static void deleteView(Statement statement, String viewName) throws SQLException {
        if (viewNameSet.contains(viewName)) {
            String sql = "drop view " + viewName;
            System.out.println(sql);
            if (statement.executeUpdate(sql) == 0) {
                System.out.println("delete " + viewName + " successfully!");
            } else {
                System.out.println("fail to delete " + viewName + "!");
            }
        }
    }

    public static void extractDataFromView(String databaseName, Statement statement, String sql, String viewName, String parentPath) throws SQLException {
        if (viewNameSet.contains(viewName)) {
            System.out.println("Start extracting Data to file...");
            long starttime = System.currentTimeMillis();
            System.out.println(sql);
            ResultSet resultSet = statement.executeQuery(sql);
            StringBuilder sb = new StringBuilder();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            String dirName = viewName.replaceAll("_V.{3}J", "");
            String fileName = null;
            if ("sybase".equalsIgnoreCase(databaseName)) {
                fileName = "sb_" + viewName.replaceAll("_V.{3}J", "");
            } else if ("mssql".equalsIgnoreCase(databaseName)) {
                fileName = "ss_" + viewName.replaceAll("_V.{3}J", "");
            } else if ("oracle".equalsIgnoreCase(databaseName)) {
                fileName = "or_" + viewName.replaceAll("_V.{3}J", "");
            } else if ("mysql".equalsIgnoreCase(databaseName)) {
                fileName = "my_" + viewName.replaceAll("_V.{3}J", "");
            }
            File file = new File(parentPath + fileName);
            try {
                if (!file.exists()) {
                    new File(file.getParent()).mkdirs();
                    file.createNewFile();
                }
                FileWriter fileWriter = new FileWriter(file);
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    sb.append(resultSetMetaData.getColumnLabel(i) + "|");
                }
                sb.append("\r\n");
                while (resultSet.next()) {
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        sb.append(resultSet.getString(i) + "|");
                    }
                    sb.append("\r\n");
                    fileWriter.write(sb.toString());
                    fileWriter.flush();
                    sb=new StringBuilder();
                }
                fileWriter.close();
                long endtime = System.currentTimeMillis();
                System.out.println("Extraction Complete!!! Timeuse:" + (endtime - starttime) + "ms");
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                File fileout = new File(parentPath, dirName);
                if (!fileout.exists()) {
                    fileout.mkdir();
                }
                BufferedOutputStream bos = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(fileout + "\\" + fileName + ".gz")));
                System.out.println("Start writing the Gzip file...");
                int c;
                while ((c = br.read()) != -1) {
                    bos.write(String.valueOf((char) c).getBytes());
                    bos.flush();
                }
                br.close();
                bos.close();
                long end = System.currentTimeMillis();
                System.out.println("Compress Complete!!! Timeuse:" + (end - endtime) + "ms");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void deleteFile(String fileDestPath){
        File file=new File(fileDestPath);
        File[] files=file.listFiles();
        for(File subFile:files){
            if(subFile.isFile()){
                subFile.delete();
            }
        }
    }

}
