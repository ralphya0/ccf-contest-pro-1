package me.ralphya0.pre_processing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import me.ralphya0.tools.DB;

public class InputGenerator {

    Connection connection = null;
    public void inputFileGenerator(String path) throws SQLException, IOException{
        connection = new DB().getConn();
        Statement ps = connection.createStatement();
        ResultSet rs = null;
        String sql = "";
        int round = 1;
        int count = 0;
        do{
            sql = "select source_type,title,format_content from t_lable_group_comp limit " + (round - 1) * 2000 + ",2000";
            rs = ps.executeQuery(sql);
            
            count = batchProcess(path,rs);
            System.out.println("round " + round);
            round ++;
            rs.close();
        }while(count == 2000);
        ps.close();
        connection.close();
        System.out.println("Done!");
    }
    
    public int batchProcess(String path,ResultSet rs) throws SQLException, IOException{
        BufferedWriter bw = null;
        int counter = 0;
        if(rs != null){
            StringBuilder sb = new StringBuilder();
            while(rs.next()){
                counter ++;
                
                int type = rs.getInt("source_type");
                String title = rs.getString("title");
                String content = rs.getString("format_content");
                //System.out.println(title);
                if(type == 4){
                	
                	sb.append(content + " \n");
                }
                else{
                	sb.append(title + " " + content + " \n");
                }
                
                
            }
            bw = new BufferedWriter(new FileWriter(path,true));
            bw.write(new String(sb.toString().getBytes(),"utf-8"));
            bw.close();
            
        }
        return counter;
    }
    
}
