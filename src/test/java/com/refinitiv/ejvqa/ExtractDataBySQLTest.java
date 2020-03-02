package com.refinitiv.ejvqa;

import com.refinitiv.ejvqa.service.ExtractDataBySQL;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ExtractDataBySQLTest {

    ExtractDataBySQL extractDataBySQL=new ExtractDataBySQL();

    @Parameters({"DBTag","ip_port","username","password","excelPath","destPath"})
    public void extractDataBySQL(String DBTag,String ip_port,String username,String password,String excelPath,String destPath){
        extractDataBySQL.extractDataBySQL(DBTag,ip_port,username,password,excelPath,destPath);
    }
}
