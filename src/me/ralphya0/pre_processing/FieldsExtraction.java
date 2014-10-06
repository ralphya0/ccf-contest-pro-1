package me.ralphya0.pre_processing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FieldsExtraction {

    long counter;
    static Connection connection;
    
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://xxxx/xxx", "xxx", "xxx");
            
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void extractAndOutput(String outputpath) {
       
        PreparedStatement ps = null;
        ResultSet rs = null;
        int resultSetCounter = 0;
        long round = 1;
        System.out.println("开始...");
        try {
            
            do{
                String sql = "select url_crc,location,province,gender,created_at,followers_count,friends_count,statuses_count,"
                        + "active_days,level_now,daren_level from w_user_info_comp limit " + (round - 1)*5000 + ",5000";
                ps = connection.prepareStatement(sql);
                rs = ps.executeQuery();
                resultSetCounter = recordReader(rs,outputpath);
                round ++;
            }
            while(resultSetCounter == 5000);
            System.out.println("完成.");
            
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally{
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        
    }
    
    public int recordReader(ResultSet rs,String path) throws SQLException, IOException{
        int counter = 0;
        BufferedWriter writer = null;
        StringBuilder sb = new StringBuilder();
        
        if(rs != null ){
            while(rs.next()){
                counter ++;
                sb.append(rs.getBigDecimal("url_crc") + ",");
                sb.append(rs.getString("location") + ",");
                sb.append(rs.getString("province") + ",");
                sb.append(rs.getString("gender") + ",");
                String createDate = rs.getString("created_at");
                if(createDate != null){
                    String [] ls = createDate.split("-");
                    int year = Integer.parseInt(ls[0]);
                    int month = Integer.parseInt(ls[1]);
                    int months = 10 - month + (2014 - year)*12;
                   
                    sb.append(months + ",");
                }
                sb.append(rs.getInt("followers_count") + ",");
                sb.append(rs.getInt("friends_count") + ",");
                sb.append(rs.getInt("statuses_count") + ",");
                sb.append(rs.getInt("active_days") + ",");
                sb.append(rs.getString("level_now") + ",");
                String daren = rs.getString("daren_level");
                if(daren == null)
                    sb.append(0 + "\n");
                else{
                    switch(daren){
                    case "初级达人" :
                        sb.append(1 + "\n");
                        break;
                    case "中级达人" :
                        sb.append(2 + "\n");
                        break;
                    case "高级达人" :
                        sb.append(3 + "\n");
                        break;
                    case "白银达人" :
                        sb.append(4 + "\n");
                        break;
                    case "黄金达人" :
                        sb.append(5 + "\n");
                        break;
                    case "白金达人" :
                        sb.append(6 + "\n");
                        break;
                    case "星钻达人" :
                        sb.append(7 + "\n");
                        break;
                    case "晶钻达人" :
                        sb.append(8 + "\n");
                        break;
                    case "璀钻达人" :
                        sb.append(9 + "\n");
                        break;
                    }
                }
            }
            
            try {
                writer = new BufferedWriter(new FileWriter(path,true));
                writer.write(sb.toString());
                System.out.println("向" + path + "写入" + counter + " 条记录");
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            finally{
                if(writer != null)
                    writer.close();
            }
        }
        return counter;
    }
    
    
}
