package com.refinitiv.ejvqa.entry;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.refinitiv.ejvqa.service.ViewFileGenerationService;

public class LambdaHandler implements RequestHandler<JSONObject,String> {

    ViewFileGenerationService VFGService=new ViewFileGenerationService();

    public String handleRequest(JSONObject jsonObject, Context context){

        System.out.println(jsonObject.toString());

        String DBTag=(String)jsonObject.get("DBTag");
        System.out.println(DBTag);
        String ip_port=(String)jsonObject.get("ip_port");
        System.out.println(ip_port);
        String databaseName1=(String)jsonObject.get("databaseName1");
        System.out.println(databaseName1);
        String databaseName2=(String)jsonObject.get("databaseName2");
        System.out.println(databaseName2);
        String username=(String)jsonObject.get("username");
        System.out.println(username);
        String password=(String)jsonObject.get("password");
        System.out.println(password);
        String tableNamePath=(String)jsonObject.get("tableNamePath");
        System.out.println(tableNamePath);
        String schemaPattern=(String)jsonObject.get("schemaPattern");
        System.out.println(schemaPattern);
        String extractNum=(String)jsonObject.get("extractNum");
        System.out.println(extractNum);
        String fileDestPath=(String)jsonObject.get("fileDestPath");
        System.out.println(fileDestPath);

        boolean isComplete=VFGService.generateView(DBTag,ip_port,databaseName1,databaseName2,username,password,
                tableNamePath,schemaPattern,extractNum,fileDestPath);
        if(isComplete){
            System.out.println("Successfully!!!");
            return "OK";
        }else{
            System.out.println("Failed!!!");
        }

        return "NO";
    }
}
