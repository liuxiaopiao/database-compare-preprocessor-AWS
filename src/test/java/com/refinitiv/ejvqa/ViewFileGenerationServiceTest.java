package com.refinitiv.ejvqa;

import com.refinitiv.ejvqa.service.ViewFileGenerationService;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ViewFileGenerationServiceTest {

    ViewFileGenerationService vFG=new ViewFileGenerationService();

    @Parameters({"DBTag","ip_port","databaseName1","databaseName2","username","password","tableNamePath","schemaPattern","extractNum","fileDestPath"})
    public void viewFileGenerationTest(String DBTag,String ip_port,String databaseName1,String databaseName2,String username,String password,
                                       String tableNamePath,String schemaPattern,String extractNum,String fileDestPath){
        boolean isCompelete=vFG.generateView(DBTag,ip_port,databaseName1,databaseName2,username,password,tableNamePath,schemaPattern,extractNum,fileDestPath);
        Assert.assertTrue(isCompelete);
    }

}
