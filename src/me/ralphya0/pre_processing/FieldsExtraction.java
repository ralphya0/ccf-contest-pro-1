package me.ralphya0.pre_processing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//为了对用户进行聚类处理，从user表中提取出目标字段
public class FieldsExtraction {

    long counter;
    static Connection connection;
    
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://202.113.76.229/ccf", "xx", "xx");
            
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void extractAndOutput(String outputpath) throws SQLException {
       
        Statement ps = connection.createStatement();
        ResultSet rs = null;
        int resultSetCounter = 0;
        long round = 1;
        try {
            
            do{
                String sql = "select url_crc,location,province,gender,created_at,followers_count,friends_count,statuses_count,"
                        + "active_days,level_now,daren_level from w_user_info_comp limit " + (round - 1)*5000 + ",5000";
                rs = ps.executeQuery(sql);
                resultSetCounter = recordReader(rs,outputpath);
                round ++;
            }
            while(resultSetCounter == 5000);
            
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
                //拼接所需的输出信息
                if(rs.getBigDecimal("url_crc") != null){
                    sb.append(rs.getBigDecimal("url_crc") + ",");
                }
                else
                    sb.append(" ,");
                
                String location = rs.getString("location");
                if(location != null)
                    sb.append(location + ",");
                else
                    sb.append(" ,");
                
                String province = rs.getString("province");
                if(province != null)
                    sb.append(province + ",");
                else
                    sb.append(" ,");
                
                String gender = rs.getString("gender");
                if(gender != null)
                    sb.append(gender + ",");
                else
                    sb.append(" ,");
                
                
                String createDate = rs.getString("created_at");
                if(createDate != null){
                    String [] ls = createDate.split("-");
                    int year = Integer.parseInt(ls[0]);
                    int month = Integer.parseInt(ls[1]);
                    int months = 10 - month + (2014 - year)*12;
                   
                    sb.append(months + ",");
                }
                else{
                    sb.append(" ,");
                }
                
                Integer followers = rs.getInt("followers_count");
                if(followers != null)
                    sb.append(followers + ",");
                else
                    sb.append(" ,");
                
                Integer friends = rs.getInt("friends_count");
                if(friends != null)
                    sb.append(friends + ",");
                else
                    sb.append(" ,");
                
                Integer statuses = rs.getInt("statuses_count");
                if(statuses != null)
                    sb.append(statuses + ",");
                else
                    sb.append(" ,");
                
                Integer active = rs.getInt("active_days");
                if(active != null)
                    sb.append(active + ",");
                else
                    sb.append(" ,");
                
                String level = rs.getString("level_now");
                if(level != null)
                    sb.append(level + ",");
                else
                    sb.append(" ,");
                
                
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
                    case "黄金达人":
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
                //输出提取的字段
                writer = new BufferedWriter(new FileWriter(path,true));
                writer.write(sb.toString());
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
