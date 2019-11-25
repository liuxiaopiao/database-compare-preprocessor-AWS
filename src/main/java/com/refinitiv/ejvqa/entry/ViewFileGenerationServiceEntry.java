package com.refinitiv.ejvqa.entry;


import com.refinitiv.ejvqa.service.ViewFileGenerationService;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ViewFileGenerationServiceEntry {
    public static void main(String[] args) {
//        String DBTag=args[0];
//        String ip_port=args[1];
//        String databaseName1=args[2];
//        String databaseName2=args[3];
//        String username=args[4];
//        String password=args[5];
//        String tableNamePath=args[6];
//        String schemaPattern=args[7];
//        String extractNum=args[8];
//        String fileDestPath=args[9];
        try {
            Properties properties = new Properties();
            String path = System.getProperty("user.dir") + "/src/main/resources/config.properties";
            properties.load(new FileInputStream(new File(path)));
            String DBTag = properties.getProperty("DBTag");
            String ip_port = properties.getProperty("ip_port").replaceAll("\\\\", "");
            String databaseName1 = properties.getProperty("databaseName1");
            String databaseName2 = properties.getProperty("databaseName2");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String tableNamePath = properties.getProperty("tableNamePath");
            String schemaPattern = properties.getProperty("schemaPattern");
            String extractNum = properties.getProperty("extractNum");
            String fileDestPath = properties.getProperty("fileDestPath");
            String configstr = "{\n" +
                    "\t\"DBTag\":" + DBTag + ",\n" +
                    "\t\"ip_port\":" + ip_port + ",\n" +
                    "\t\"databaseName1\":" + databaseName1 + ",\n" +
                    "\t\"databaseName2\":" + databaseName2 + ",\n" +
                    "\t\"username\":" + username + ",\n" +
                    "\t\"password\":" + password + ",\n" +
                    "\t\"tableNamePath\":" + tableNamePath + ",\n" +
                    "\t\"schemaPattern\":" + schemaPattern + ",\n" +
                    "\t\"extractNum\":" + extractNum + ",\n" +
                    "\t\"fileDestPath\":" + fileDestPath + "\n" +
                    "}";
            System.out.println("The Config is:\n" + configstr);
            ViewFileGenerationService VFGService = new ViewFileGenerationService();
            VFGService.generateView(DBTag, ip_port, databaseName1, databaseName2, username, password,
                    tableNamePath, schemaPattern, extractNum, fileDestPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
