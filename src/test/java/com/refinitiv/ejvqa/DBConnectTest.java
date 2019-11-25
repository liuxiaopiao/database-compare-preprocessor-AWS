package com.refinitiv.ejvqa;

import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Sample test for DB jdbc.
 */
public class DBConnectTest
{
    /**
     * Rigorous Test :-)
     */

    @Test
    public void MysqlConnectTest(){
        String url="jdbc:mysql://localhost:3306/compare1";
        String user="root";
        String password="zxcasd";
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(url, user, password);
            if (connection != null) {
                System.out.println("Connect successfully!!!");
            } else {
                System.out.println("Connect failed!!!");
            }
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    @Test
    public void MssqlConnectTest(){
        String url="jdbc:sqlserver://192.168.99.100:1433;databaseName=data_govcorp";
        String user="sa";
        String password="Wuaiyun22019;";
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection connection = DriverManager.getConnection(url, user, password);
            if (connection != null) {
                System.out.println("Connect successfully!!!");
            } else {
                System.out.println("Connect failed!!!");
            }
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    @Test
    public void SybaseConnectTest(){
//        String jdbcurl="jdbc:sybase:Tds:10.53.8.21:4074/scratch";
        String jtdsurl="jdbc:jtds:sybase://10.53.8.21:4074/scratch";
        String user="dsos_dist";
        String password="EJV4fiswr";
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            Connection connection = DriverManager.getConnection(jtdsurl, user, password);
            if (connection != null) {
                System.out.println("Connect successfully!!!");
            } else {
                System.out.println("Connect failed!!!");
            }
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    @Test
    public void OracleDbConnectTest(){
        String url="jdbc:oracle:thin:@10.218.77.190:1521:orcl";
        String user="TRQA";
        String password="TQA123P";
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            Connection connection = DriverManager.getConnection(url, user, password);
            if (connection != null) {
                System.out.println("Connect successfully!!!");
            } else {
                System.out.println("Connect failed!!!");
            }
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }catch(SQLException e){
            e.printStackTrace();
        }
    }
}
