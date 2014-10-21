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
    
    
    public void fetching() throws SQLException{
        connection = new DB().getConn();
        ps = connection.createStatement();
        ps2 = connection.createStatement();
        ResultSet rs = null;
        int round = 1;
        int items = 0;
        do{
            
            String sql = "select idnum,title_important,abs_important,content_important from t_lable_group_comp"
                    + " limit " + (round - 1)*500 + ",500";
            
            rs = ps.executeQuery(sql);
            items = computingAndUpdating(rs);
            round ++;
            System.out.println("round " + round);
        }while(items == 500);
        System.out.println("Done!");
        rs.close();
        ps.close();
        ps2.close();
        connection.close();
    }
    
    public int computingAndUpdating(ResultSet rs) throws SQLException{
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
                us.append("update t_lable_group_comp set all_important = '");
                
                String [] keys = scores.keySet().toArray(new String[0]);
                
                for(String k : keys){
                    us.append(k + "/" + pro.get(k) + "/" + scores.get(k) + "#");
                }
                if(us.lastIndexOf("#") == us.length() - 1)
                    us.deleteCharAt(us.length() - 1);
                
                us.append("' where idnum = " + id);
                
                scores.clear();
                pro.clear();
                ps2.executeQuery(us.toString());
                us = null;
            }
            
            
        }
        return counter;
    }
}
