package com.refinitiv.ejvqa;

import com.refinitiv.ejvqa.util.CommonUtil;
import org.testng.annotations.Test;

import javax.swing.*;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
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

}
