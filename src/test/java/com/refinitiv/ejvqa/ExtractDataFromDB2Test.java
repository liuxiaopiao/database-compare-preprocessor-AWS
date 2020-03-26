package com.refinitiv.ejvqa;


import com.refinitiv.ejvqa.service.ExtractDataFromDB2;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ExtractDataFromDB2Test {

    ExtractDataFromDB2 edfd=new ExtractDataFromDB2();

    @Parameters({"DBTag","ip_port","username","password","excelPath","destPath","startNum","increment","separator"})
    public void extractDataFromDB2Test(String DBTag,String ip_port,String username,String password,
                                       String excelPath,String destPath,int startNum,int increment,String separator){
        edfd.extractDataFromDataBaseBySQL(DBTag,ip_port,username,password,excelPath,destPath,startNum,increment,separator);

    }
}
