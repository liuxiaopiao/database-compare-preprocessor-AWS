package com.refinitiv.ejvqa;

import com.refinitiv.ejvqa.util.CommonUtil;
import org.testng.annotations.Test;

import javax.swing.*;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

public class SampleTest {
    @Test
    public void fileIOTest() throws IOException {
        String str="天龙八部";
        StringBuilder sb=new StringBuilder();
        String filepath=System.getProperty("user.dir")+"/src/data/";
        System.out.println(filepath);
        File file=new File(filepath+"FileText.txt");
        if(!file.exists()){
            new File(file.getParent()).mkdirs();
            file.createNewFile();
        }
        FileWriter fileWriter=new FileWriter(file);
        for(int i=0;i<10000;i++){
            sb.append(str);
            sb.append("\r\n");
            fileWriter.write(sb.toString());
            fileWriter.flush();
            sb=new StringBuilder();
        }
        fileWriter.close();
    }

    @Test
    public void stringNullTest(){
        String str=null;
        if(str==null){
            System.out.println("空");
        }
    }

    @Test
    public void deleteFileTest(){
        File file=new File("C:/Users/uc260386/Desktop/New folder");
//        String[] files=file.list(new FilenameFilter() {
//            private Pattern pattern=Pattern.compile(".*.txt");
//            @Override
//            public boolean accept(File dir, String name) {
//                return pattern.matcher(name).matches();
//            }
//        });
//        for(String file1:files){
//            System.out.println(file1);
//        }
        File[] files1=file.listFiles();
        for(File file2:files1){
            if(file2.isFile()){
                file2.delete();
            }
        }
    }

    @Test
    public void zipfiletest() throws IOException {
        String destDir=System.getProperty("user.dir")+"/output/";
        String srcDir="C:\\work\\1\\comparedata";
//        FileOutputStream fos=new FileOutputStream(new File(destDir+"compare.zip"));
        FileOutputStream fos=new FileOutputStream(new File(new File(srcDir)+"/data.zip"));
//        FileOutputStream fos=new FileOutputStream(new File(destDir.replace("output/","data.zip")));
        CommonUtil.zipAllFile(srcDir,fos,true);
        fos.close();
    }

    @Test
    public void uploadS3ObjectTest(){
        String bucketName="fileoutput-20191126-bucket";
        String outputPath="2/compare.zip";
        String filepath=System.getProperty("user.dir")+"/output/compare.zip";
        CommonUtil.uploadToS3(bucketName,new File(filepath).getName(),filepath);
    }

    @Test
    public void ConnectTest() throws SQLException, ClassNotFoundException {

        Connection connection=null;
        String ip_port="10.218.77.150:1433";
        String username="readonly";
        String pwd="Welcome1";
        connection=CommonUtil.createConnection("mssql",connection,ip_port,"qai_master",username,pwd);
    }

    @Test
    public void getSQLInfoFromExcelTest(){

        String path=System.getProperty("user.dir")+"/src/data/BIMBQMTOCIQM_CCRMapping.xlsx";
        File file=new File(path);
        LinkedHashSet<String> tableNameSet=new LinkedHashSet<>();
        LinkedHashMap<String,String> tableNameToPrimaryKeyMap=new LinkedHashMap<>();
        LinkedHashMap<String,String> tableNameToColumnLabelMap=new LinkedHashMap<>();
        LinkedHashMap<String,String> tableNameToFilterMap=new LinkedHashMap<>();

        CommonUtil.getSQLInfoFromExcel(file,tableNameSet,tableNameToPrimaryKeyMap,tableNameToColumnLabelMap,tableNameToFilterMap);
    }

    @Test
    public void decomposeGzipFileTest(){
        try {
            String dest = "C:/";
            String folder = "Data";
            File file1 = new File("C:/sb_CCPASDI_QUOTECOMMENT_BKP.gz");
            File file2 = new File("C:/ss_CCPASDI_QUOTECOMMENT_BKP.gz");
            CommonUtil.decomposeGzipFile(file1, folder, dest);
            CommonUtil.decomposeGzipFile(file2,folder,dest);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void wirteFileTest(){
        try {
            List<String> data = new LinkedList<>();
            data.add("2017-02-21 00:00:00196760319510SophiaAdd a new quote|196760319510|Sophia|2017-02-21 00:00:00|Add a new quote|1|\n" +
                    "2017-02-21 00:00:00196760328011EJVQAAdd a new quote|196760328011|EJVQA|2017-02-21 00:00:00|Add a new quote|2|\n" +
                    "2017-02-21 00:00:00196760334613EJVQAAdd a new quote|196760334613|EJVQA|2017-02-21 00:00:00|Add a new quote|3|\n" +
                    "2017-02-21 00:00:00196760334616EJVQAAdd a new quote|196760334616|EJVQA|2017-02-21 00:00:00|Add a new quote|4|\n" +
                    "2017-02-21 00:00:00196760334620EJVQAAdd a new quote|196760334620|EJVQA|2017-02-21 00:00:00|Add a new quote|5|\n" +
                    "2017-02-21 00:00:00196760334624EJVQAAdd a new quote|196760334624|EJVQA|2017-02-21 00:00:00|Add a new quote|6|\n" +
                    "2017-02-21 00:00:00196760334848EJVQAAdd a new quote|196760334848|EJVQA|2017-02-21 00:00:00|Add a new quote|7|\n" +
                    "2017-02-21 00:00:00196760337250EJVQAAdd a new quote|196760337250|EJVQA|2017-02-21 00:00:00|Add a new quote|8|\n" +
                    "2017-02-21 00:00:00196760337549EJVQAAdd a new quote|196760337549|EJVQA|2017-02-21 00:00:00|Add a new quote|9|\n" +
                    "2017-02-21 00:00:00196760337552EJVQAAdd a new quote|196760337552|EJVQA|2017-02-21 00:00:00|Add a new quote|10|");
            String path = System.getProperty("user.dir") + "/Output/testfile";
            CommonUtil.writeFile(data, path);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void gzipFileTest(){
        String path = System.getProperty("user.dir") + "/Output/testfile";
        String tableName="testfile";
        String schema="Test";
        CommonUtil.gzipFile(path,schema,tableName);
    }
}
