package com.refinitiv.ejvqa.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Common util for create View
 */
public class CommonUtil {

    public static LinkedHashSet<String> tablNameSet;
    public static LinkedHashSet<String> viewNameSet;
    public static LinkedHashSet<String> indexColumNameSet;
    public static LinkedList<String> primaryKeyList;
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
            url = "jdbc:oracle:thin:@" + ip_port;
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
            if(!tableSet.getString("TABLE_NAME").contains("_BKP")) {
                tablNameSet.add(tableSet.getString("TABLE_NAME"));
            }
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

    public static void getSQLInfoFromExcel(File file,LinkedHashSet<String> tableNameSet,LinkedHashMap<String,String> tableNameToPrimaryKey,LinkedHashMap<String,String> tableNameToColumnLabel,LinkedHashMap<String,String> tableNameToFilter){
        FileInputStream fis=null;
        try {
            fis = new FileInputStream(file);
            if (file.exists() && file.getName().endsWith(".xlsx")) {
                XSSFWorkbook xssfWb = new XSSFWorkbook(fis);
                XSSFSheet xsfSheet = xssfWb.getSheetAt(0);
                for (int i = 1; i < xsfSheet.getPhysicalNumberOfRows(); i++) {
                    Row oneRow = xsfSheet.getRow(i);
                    Cell tableName = oneRow.getCell(0);
                    if (tableName != null) {
                        tableNameSet.add(tableName.getStringCellValue());
                    }
                    Cell primaryKey = oneRow.getCell(1);
                    if (tableName != null && primaryKey != null) {
                        tableNameToPrimaryKey.put(tableName.getStringCellValue(), primaryKey.getStringCellValue());
                    }
                    Cell columnLabel = oneRow.getCell(2);
                    if (tableName != null && columnLabel != null) {
                        tableNameToColumnLabel.put(tableName.getStringCellValue(), columnLabel.getStringCellValue());
                    }
                    Cell filter=oneRow.getCell(3);
                    if(tableName!=null&&filter!=null){
                        tableNameToFilter.put(tableName.getStringCellValue(),filter.getStringCellValue());
                    }

                }
                xssfWb.close();
                ;
            }
            System.out.println(tableNameSet);
            System.out.println(tableNameToPrimaryKey);
            System.out.println(tableNameToColumnLabel);
        }catch(IOException e){
            e.printStackTrace();
        }finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void getSQLFromExcel(File file,LinkedHashSet<String> tableNameSet,LinkedHashMap<String,String> tableNameToSQL){
        FileInputStream fis=null;
        try {
            fis = new FileInputStream(file);
            if (file.exists() && file.getName().endsWith(".xlsx")) {
                XSSFWorkbook xssfWb = new XSSFWorkbook(fis);
                XSSFSheet xsfSheet = xssfWb.getSheetAt(0);
                for (int i = 1; i < xsfSheet.getPhysicalNumberOfRows(); i++) {
                    Row oneRow = xsfSheet.getRow(i);
                    Cell tableName = oneRow.getCell(0);
                    if (tableName != null) {
                        tableNameSet.add(tableName.getStringCellValue());
                    }
                    Cell sql = oneRow.getCell(1);
                    if (tableName != null && sql != null) {
                        tableNameToSQL.put(tableName.getStringCellValue(), sql.getStringCellValue());
                    }

                }
                xssfWb.close();
            }
            System.out.println(tableNameSet);
            System.out.println(tableNameToSQL);
        }catch(IOException e){
            e.printStackTrace();
        }finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

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

    public static LinkedList<String> generatePrimaryKeys(DatabaseMetaData databaseMetaData,String schemaPattern,String tableName) throws SQLException {
        primaryKeyList=new LinkedList<>();
        ResultSet resultSet=databaseMetaData.getPrimaryKeys(null,schemaPattern,tableName);
        while(resultSet.next()){
            primaryKeyList.add(resultSet.getString("COLUMN_NAME"));
        }
        Collections.reverse(primaryKeyList);
        return  primaryKeyList;
    }

    public static LinkedHashMap<String, String> generateColumnInfo(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        columnMap = new LinkedHashMap<String, String>();
        ResultSet columnSet = databaseMetaData.getColumns(null, null,tableName, "%");
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
        if ("oracle".equalsIgnoreCase(databaseName) || viewNameSet.contains(viewName)) {
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
            System.out.println(parentPath + fileName);
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
                    sb = new StringBuilder();
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

    public static void extractDataBySQL(Statement statement,String sql,String path,LinkedList<String> primaryKeyList,boolean flag,int startNum) throws SQLException, IOException {
        ResultSet resultSet=statement.executeQuery(sql);
        resultSet.last();
        int lastIndex=resultSet.getRow();
        System.out.println("The Extract number of records is: "+lastIndex);
        resultSet.beforeFirst();
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

            if(j%1000==0){
                writeFile(pageData,path);
                pageData.clear();
                timeuse=System.currentTimeMillis();
                System.out.println("Output "+j+" rows,Timeuse: "+(timeuse-loopBegin)+"ms");
                loopBegin=timeuse;
            }
        }

        if(j%1000!=0){
            writeFile(pageData,path);
        }

        long end=System.currentTimeMillis();
        System.out.println("Extraction total timeuse is: "+(end-begin)+"ms");
    }

    public static void writeFile(List<String> data,String path) throws IOException {
        if(null==data||data.size()==0){
            return;
        }

        File file=new File(path);

//        if(file.exists()){
//            file.delete();
//            System.out.println("Delete Old File!");
//        }

        if(!file.exists()){
            new File(file.getParent()).mkdirs();
            file.createNewFile();
            System.out.println("Create New File!");
        }

        FileOutputStream fileOutputStream=new FileOutputStream(path,true);
        OutputStreamWriter outputStreamWriter=new OutputStreamWriter(fileOutputStream,"UTF-8");
        BufferedWriter bufferedWriter=new BufferedWriter(outputStreamWriter);

        for(String row:data){
            bufferedWriter.write(row);
            bufferedWriter.flush();
        }

        bufferedWriter.close();
        outputStreamWriter.close();
        fileOutputStream.close();
    }

    public static void gzipFile(String path,String schemaPattern,String tableName){
        try {
            File file = new File(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF-8"));
            File fileout = new File((file.getParent() + "/"+schemaPattern));
            if (!fileout.exists()) {
                fileout.mkdir();
            }
            BufferedWriter bos = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(fileout + "/" + tableName + ".gz")),"UTF-8"));
            System.out.println("Start writing the Gzip file...");
            long start = System.currentTimeMillis();
            String line;
            while ((line = br.readLine()) != null) {
                bos.write(line+"\r\n");
                bos.flush();
            }
            br.close();
            bos.close();
            long end = System.currentTimeMillis();
            System.out.println("Compress Complete!!! Timeuse:" + (end - start) + "ms");
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void decomposeGzipFile(File file,String folder,String dest) throws IOException {
        String fileName=file.getName();
        String realFilename=null;
        String fileFolder=null;
        String realFolder=null;
        GZIPInputStream gis=new GZIPInputStream(new FileInputStream(file));
        BufferedReader br=new BufferedReader(new InputStreamReader(gis));
        boolean flag=true;
        String line=null;
        List<String> page=new LinkedList<>();

        int j = 0;
        int num = 0;

        while ((line = br.readLine()) != null) {
            j++;
            page.add(line);
            if(line.endsWith("|")){
                page.add("\r\n");
            }

            if (j % 1000 == 0) {
                num++;
                realFilename = fileName.substring(0,fileName.length()-3) + num;
                fileFolder=fileName.substring(3,fileName.length()-3)+num;
                if (num % 4 == 0) {
                    realFolder = folder + 0;
                } else if (num % 4 == 1) {
                    realFolder = folder + 1;
                } else if (num % 4 == 2) {
                    realFolder = folder + 2;
                } else {
                    realFolder = folder + 3;
                }
                String destPath = dest + "/" + realFolder + "/" + "/"+fileFolder+"/"+realFilename + ".gz";
                writeGzipFile(page, destPath);
                page.clear();
            }
        }

        if (j % 1000 != 0) {
            num++;
            realFilename = fileName.substring(0,fileName.length()-3) + num;
            fileFolder=fileName.substring(3,fileName.length()-3)+num;
            if (num % 4 == 0) {
                realFolder = folder + 0;
            } else if (num % 4 == 1) {
                realFolder = folder + 1;
            } else if (num % 4 == 2) {
                realFolder = folder + 2;
            } else {
                realFolder = folder + 3;
            }
            String destPath = dest + "/" + realFolder + "/" + "/"+fileFolder+"/"+realFilename + ".gz";
            writeGzipFile(page, destPath);
        }

        br.close();
        gis.close();

    }

    public static void writeGzipFile(List<String> data,String destPath) throws IOException {
        if(null==data||data.size()==0){
            return;
        }

        File file=new File(destPath);

        if(!file.exists()){
            new File(file.getParent()).mkdirs();
            file.createNewFile();
            System.out.println("Create New File!");
        }

        FileOutputStream fileOutputStream=new FileOutputStream(destPath,true);
        GZIPOutputStream gos=new GZIPOutputStream(fileOutputStream);
        BufferedWriter bufferedWriter=new BufferedWriter(new OutputStreamWriter(gos,"UTF-8"));

        for(String row:data){
            bufferedWriter.write(row);
            bufferedWriter.flush();
        }

        bufferedWriter.close();
        gos.close();
        fileOutputStream.close();
    }

    public static void deleteOnlyFile(String fileDestPath) {
        File file = new File(fileDestPath);
        File[] files = file.listFiles();
        for (File subFile : files) {
            if (subFile.isFile()) {
                subFile.delete();
            }
        }
    }

    public static void deleteAllFile(File deletefile) {
        if (!deletefile.exists()) {
            return;
        }
        if (deletefile.isFile()) {
            deletefile.delete();
            return;
        }
        File[] files = deletefile.listFiles();
        for (File subFile : files) {
            deleteAllFile(subFile);
        }
        deletefile.delete();
    }

    public static void zipAllFile(String srcPath, OutputStream outputStream, boolean KeepDirStructure) {
        long start = System.currentTimeMillis();
        ZipOutputStream zos = null;
        try {
            zos = new ZipOutputStream(outputStream);
            File sourceFile = new File(srcPath);
            compress(sourceFile, zos, sourceFile.getName(), KeepDirStructure);
            long end = System.currentTimeMillis();
            System.out.println("Compress Complete,Timeuse:" + (end - start) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("System Error:" + e);
        } finally {
            if (zos != null) {
                try {
                    zos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println("System Error:" + e);
                }
            }
        }
    }

    private static void compress(File sourceFile, ZipOutputStream zos, String name,
                                 boolean KeepDirStructure) {
        byte[] buf = new byte[2048];
        try {
            if (sourceFile.isFile()) {
                zos.putNextEntry(new ZipEntry(name));
                int len;
                FileInputStream in = new FileInputStream(sourceFile);
                while ((len = in.read(buf)) != -1) {
                    zos.write(buf, 0, len);
                }
                zos.closeEntry();
                in.close();
            } else {
                File[] listFiles = sourceFile.listFiles();
                if (listFiles == null || listFiles.length == 0) {
                    if (KeepDirStructure) {
                        zos.putNextEntry(new ZipEntry(name + "/"));
                        zos.closeEntry();
                    }
                } else {
                    for (File file : listFiles) {
                        if (KeepDirStructure) {
                            compress(file, zos, name + "/" + file.getName(), KeepDirStructure);
                        } else {
                            compress(file, zos, file.getName(), KeepDirStructure);
                        }
                    }
                }
            }
            zos.flush();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("System Error:" + e);
        }
    }

    public static void uploadToS3(String bucketName, String outputPath, String filePath) {
        try {
            FileInputStream fileInputStream = new FileInputStream(new File(filePath));
            ObjectMetadata objectMetadata = new ObjectMetadata();
            if (filePath.contains(".gz")) {
                objectMetadata.setContentType("application/x-gzip");
            } else if (filePath.contains(".zip")) {
                objectMetadata.setContentType("application/zip");
            }
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            PutObjectResult putObjectResult = s3Client.putObject(bucketName, outputPath, fileInputStream, objectMetadata);
            if (putObjectResult.getETag() != null) {
                System.out.println("Upload Successfully!");
            } else {
                System.out.println("Upload Failed!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
