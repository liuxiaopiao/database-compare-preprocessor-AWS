package com.refinitiv.ejvqa;


import com.refinitiv.ejvqa.service.ExtractDataFromDB;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ExtractDataFromDBTest {

    ExtractDataFromDB edfd=new ExtractDataFromDB();

    @Parameters({"DBTag","ip_port","databaseName1","username","password","tableNamePath","schemaPattern","filter","extractNum","fileDestPath"})
    public void viewFileGenerationTest(String DBTag,String ip_port,String databaseName1,String username,String password,
                                       String tableNamePath,String schemaPattern,String filter,String extractNum,String fileDestPath){
        edfd.extractDataFromDb(DBTag,ip_port,databaseName1,username,password,tableNamePath,schemaPattern,filter,extractNum,fileDestPath);

    }
}
