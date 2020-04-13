package com.refinitiv.ejvqa;


import com.refinitiv.ejvqa.service.ExtractDataFromMultiDBBySQL;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ExtractDataFromMultiDBBySQLTest {

    ExtractDataFromMultiDBBySQL edfmds=new ExtractDataFromMultiDBBySQL();

    @Parameters({"DBTag","ip_port","username","password","excelPath","destPath","startNum","increment","separator","isExtractAll","append"})
    public void extractDataFromMultiDBBySQL(String DBTag,String ip_port,String username,String password,
                                       String excelPath,String destPath,int startNum,int increment,String separator,String isExtractAll,String append){
        edfmds.extractDataFromMultiDataBaseBySQL(DBTag,ip_port,username,password,excelPath,destPath,startNum,increment,separator,isExtractAll,append);

    }
}
