package com.refinitiv.ejvqa.entry;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.refinitiv.ejvqa.service.ViewFileGenerationService;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

public class RunConfigHandler implements RequestHandler<JSONObject,String> {

    public String handleRequest(JSONObject jsonObject, Context context){

        String DBTag = (String) jsonObject.get("DBTag");
        System.out.println(DBTag);
        String ip_port = (String) jsonObject.get("ip_port");
        System.out.println(ip_port);
        String databaseName1 = (String) jsonObject.get("databaseName1");
        System.out.println(databaseName1);
        String databaseName2 = (String) jsonObject.get("databaseName2");
        System.out.println(databaseName2);
        String username = (String) jsonObject.get("username");
        System.out.println(username);
        String password = (String) jsonObject.get("password");
        System.out.println(password);
        String tableNamePath = (String) jsonObject.get("tableNamePath");
        System.out.println(tableNamePath);
        String schemaPattern = (String) jsonObject.get("schemaPattern");
        System.out.println(schemaPattern);
        String extractNum = (String) jsonObject.get("extractNum");
        System.out.println(extractNum);
        String fileDestPath = (String) jsonObject.get("fileDestPath");
        System.out.println(fileDestPath);

        if(ip_port.contains("aws")){
            ViewFileGenerationService VFGService=new ViewFileGenerationService();
            boolean isComplete=VFGService.generateView(DBTag,ip_port,databaseName1,databaseName2,username,password,
                    tableNamePath,schemaPattern,extractNum,fileDestPath);
            if(isComplete){
                System.out.println("Cloud Successfully!");
                return "Cloud Successfully!";
            }else{
                System.err.println("Cloud Failed!");
                return "Cloud Failed!";
            }
        }else {
            try {
                Git.cloneRepository().setURI("https://github.com/wjxpeking2019/simple-java-maven-app.git").setDirectory(new File("/tmp")).call();
                File modifyFile = new File("/tmp/config.properties");
                FileOutputStream fileOutputStream = new FileOutputStream(modifyFile);
                Properties properties = new Properties();

                properties.setProperty("DBTag", DBTag);
                properties.setProperty("ip_port", ip_port);
                properties.setProperty("databaseName1", databaseName1);
                properties.setProperty("databaseName2", databaseName2);
                properties.setProperty("username", username);
                properties.setProperty("password", password);
                properties.setProperty("tableNamePath", tableNamePath);
                properties.setProperty("schemaPattern", schemaPattern);
                properties.setProperty("extractNum", extractNum);
                properties.setProperty("fileDestPath", fileDestPath);
                properties.store(fileOutputStream, "update the config for new run!");
                fileOutputStream.flush();
                fileOutputStream.close();

                Git git = Git.open(new File("/tmp/.git"));
                git.add().addFilepattern(".").call();
                CommitCommand commitCommand = git.commit().setMessage("update config for a new run.").setAllowEmpty(true);
                commitCommand.call();
                Repository repository = new FileRepository("/tmp/.git");
                git = new Git(repository);
                git.push().setCredentialsProvider(new UsernamePasswordCredentialsProvider("wjxpeking2019", "5aiyun22019")).call();
                System.out.println("Successfully!");
                return "Successfully!";
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed!");
                return "Failed!";
            }
        }
    }
}
