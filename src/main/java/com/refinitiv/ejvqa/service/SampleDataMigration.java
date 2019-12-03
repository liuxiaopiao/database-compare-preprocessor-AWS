package com.refinitiv.ejvqa.service;

import com.refinitiv.ejvqa.util.CommonUtil;


import java.sql.*;

public class SampleDataMigration {
    public static void main(String[] args) {
        try {
            Connection conn = null;
            conn = CommonUtil.createConnection("mssql", conn, "192.168.99.100:1433", "data_govcorp", "sa", "Wuaiyun22019;");
            Statement statement = conn.createStatement();
            String sql = "select * from id_obj_xref_hist";
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData rSMD = resultSet.getMetaData();
            Connection conn2 = null;
            conn2 = CommonUtil.createConnection("mssql", conn, "192.168.99.100:1433", "scratch", "sa", "Wuaiyun22019;");

            StringBuilder sb = new StringBuilder();
            StringBuilder sb1 = new StringBuilder();
            int columnConuts = rSMD.getColumnCount();
            for (int i = 1; i <= columnConuts; i++) {
                if(rSMD.getColumnTypeName(i).equalsIgnoreCase("binary")){
                    sb.append("CONVERT(binary(8),?),");
                }else if(rSMD.getColumnTypeName(i).equalsIgnoreCase("char")){
                    sb.append("CAST(? as char),");
                }else if(rSMD.getColumnTypeName(i).equalsIgnoreCase("smalldatetime")||rSMD.getColumnTypeName(i).equalsIgnoreCase("datetime")){
                    sb.append("CONVERT(datetime,?),");
                }else if(rSMD.getColumnTypeName(i).equalsIgnoreCase("int")||rSMD.getColumnTypeName(i).equalsIgnoreCase("smallint")){
                    sb.append("CONVERT(int,?),");
                }else if(rSMD.getColumnTypeName(i).equalsIgnoreCase("float")){
                    sb.append("CONVERT(float,?),");
                }else if(rSMD.getColumnTypeName(i).equalsIgnoreCase("double")){
                    sb.append("CONVERT(double,?),");
                }
                sb1.append(rSMD.getColumnTypeName(i)+"|");
            }
            String sql2 = "insert into id_obj_xref_hist values(" + sb.toString().substring(0, sb.length() - 1) + ")";
            System.out.println(sql2);
            System.out.println(sb1.toString());

            PreparedStatement ps = conn2.prepareStatement(sql2);
            while (resultSet.next()) {
                for(int i=1;i<=columnConuts;i++){
                    ps.setString(i,resultSet.getString(i));
                }
                ps.executeUpdate();
            }
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
