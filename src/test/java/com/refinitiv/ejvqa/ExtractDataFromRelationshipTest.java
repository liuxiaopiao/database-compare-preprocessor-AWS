package com.refinitiv.ejvqa;

import com.refinitiv.ejvqa.service.ExtractDataBySQL;
import com.refinitiv.ejvqa.service.ExtractDataFromRelationship;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ExtractDataFromRelationshipTest {

    ExtractDataFromRelationship extractDataFromRelationship=new ExtractDataFromRelationship();

    @Parameters({"DBTag","ip_port","username","password","excelPath","destPath","startNum","increment","separator"})
    public void extractDataBySQL(String DBTag,String ip_port,String username,String password,String excelPath,String destPath,int startNum,int increment,String separator){
        extractDataFromRelationship.extractDataBySQL(DBTag,ip_port,username,password,excelPath,destPath,startNum,increment,separator);
    }
}
