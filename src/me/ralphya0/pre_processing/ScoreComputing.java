package me.ralphya0.pre_processing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import me.ralphya0.tools.DB;

public class ScoreComputing {

    Connection connection = null;
    Statement ps = null;
    Statement ps2 = null;
    
    public void run() throws SQLException{
        connection = new DB().getConn();
        ps = connection.createStatement();
        ps2 = connection.createStatement();
        fetching(1);
        System.out.println("一个话题处理结束");
        fetching(2);
        System.out.println("一个话题处理结束");
        fetching(3);
        System.out.println("一个话题处理结束");
        ps.close();
        ps2.close();
        connection.close();
        System.out.println("Done!");
    }
    public void fetching(int type) throws SQLException{
        
        ResultSet rs = null;
        int round = 1;
        int items = 0;
        do{
            String sql = "";
            if(type == 1)
                sql = "select idnum,title_important,abs_important,content_important from violence"
                    + " limit " + (round - 1)*500 + ",500";
            else if(type == 2)
                sql = "select idnum,title_important,abs_important,content_important from campus"
                        + " limit " + (round - 1)*500 + ",500";
            else if(type == 3)
                sql = "select idnum,title_important,abs_important,content_important from bus"
                        + " limit " + (round - 1)*500 + ",500";
            
            rs = ps.executeQuery(sql);
            items = computingAndUpdating(rs,type);
            round ++;
            System.out.println("round " + round);
        }while(items == 500);
        
        rs.close();
        
    }
    
    public int computingAndUpdating(ResultSet rs,int type) throws SQLException{
        int counter = 0;
        
        
        if(rs != null){
            while(rs.next()){
                int id = rs.getInt("idnum");
                String title = rs.getString("title_important");
                String abs = rs.getString("abs_important");
                String content = rs.getString("content_important");
                
                counter ++;
                
                String [] title_group = null;
                String [] abs_group = null;
                String [] content_group = null;
                
                if(title != null && title.trim().length() > 0)
                    title_group = title.split("#");
                if(abs != null && abs.trim().length() > 0)
                    abs_group = abs.split("#");
                if(content != null && content.trim().length() > 0 )
                    content_group = content.split("#");
                
                Map<String,Double> scores = new HashMap<String,Double>();
                Map<String,String> pro = new HashMap<String,String>();
                
                
                if(title_group != null){
                    for(String l : title_group){
                        if(l != null && l.trim().length() > 0){
                            String [] title_group_items = l.split("/");
                            if(title_group_items != null && title_group_items[0] != null && title_group_items[2] != null
                                    && !title_group_items[0].equals(" ")){
                                
                                scores.put(title_group_items[0], Double.parseDouble(title_group_items[2]) * 5);
                                pro.put(title_group_items[0], title_group_items[1]);
                            }
                        }
                        
                    }
                    
                }
                if(abs_group != null){
                    for(String s : abs_group){
                        if(s != null && s.trim().length() > 0){
                            String [] abs_group_items = s.split("/");
                            if(abs_group_items != null && abs_group_items[0] != null && abs_group_items[2] != null 
                                    && !abs_group_items[0].equals(" ")){
                                if(!scores.containsKey(abs_group_items[0])){
                                    scores.put(abs_group_items[0], (double) 0);
                                    pro.put(abs_group_items[0], abs_group_items[1]);
                                }
                                
                                scores.put(abs_group_items[0], scores.get(abs_group_items[0]) + Double.parseDouble(abs_group_items[2]) * 3);
                                
                            }
                        }
                    }
                }
                if(content_group != null){
                    for(String s : content_group){
                        if(s != null && s.trim().length() > 0){
                            String [] content_group_items = s.split("/");
                            if(content_group_items != null && content_group_items[0] != null && content_group_items[2] != null
                                    && !content_group_items[0].equals(" ")){
                                if(!scores.containsKey(content_group_items[0])){
                                    scores.put(content_group_items[0], (double)0);
                                    pro.put(content_group_items[0],content_group_items[1]);
                                }
                                
                                scores.put(content_group_items[0], scores.get(content_group_items[0]) + Double.parseDouble(content_group_items[2]));
                                
                            }
                        }
                    }
                }
                
                StringBuilder us = new StringBuilder();
                if(type == 1)
                    us.append("update violence set all_important = '");
                else if(type ==2)
                    us.append("update campus set all_important = '");
                else if(type ==3)
                    us.append("update bus set all_important = '");
                
                String [] keys = scores.keySet().toArray(new String[0]);
                
                for(String k : keys){
                    us.append(k + "/" + pro.get(k) + "/" + scores.get(k) + "#");
                }
                if(us.lastIndexOf("#") == us.length() - 1)
                    us.deleteCharAt(us.length() - 1);
                
                us.append("' where idnum = " + id);
                
                scores.clear();
                pro.clear();
                ps2.executeUpdate(us.toString());
                us = null;
            }
            
            
        }
        return counter;
    }
    
    public static void main(String[] args) throws SQLException {
        new ScoreComputing().run();
    }
}
