package me.ralphya0.pre_processing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.ralphya0.tools.DB;

//将同一话题下的新闻按照发布时间归类（以一定cosin阈值进行筛选），并且提炼出人物、时间、地点信息
public class AggregateByDate {

    List<String> dates = null;
    List<Double> relative_values = null; 
    Connection connection = null;
    Statement st = null;
    //将新闻按照时间归类(同时以固定阈值对cosin值进行过滤)
    public AggregateByDate() throws SQLException, IOException{
        connection = new DB().getConn();
        st = connection.createStatement();
        newsAggregation();
    }
    
    
    public void newsAggregation() throws SQLException, IOException{
        String in1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-4\\violence.txt";
        String in2 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-4\\campus.txt";
        String in3 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-4\\bus_explosion.txt";
        String sql1 = "select idnum,person,location,time_tag,first_time,cosin,all_important from violence where release_date_day = '#' and cosin > 0.05";
        String sql2 = "select idnum,person,location,time_tag,first_time,cosin,all_important from campus where release_date_day = '#' and cosin > 0.05";
        String sql3 = "select idnum,person,location,time_tag,first_time,cosin,all_important from bus_explosion where release_date_day = '#' and cosin > 0.05";
        
        String sql4 = "select * from violence where idnum = #";
        String sql5 = "select * from campus where idnum = #";
        String sql6 = "select * from bus_explosion where idnum = #";

        String update1 = "insert into yx_daytopic_violence values(";
        String update2 = "insert into yx_daytopic_campus values(" ;
        String update3 = "insert into yx_daytopic_bus_explosion values(";
        
        String update4 = "insert into yx_daytopic_violence_new values(";
        String update5 = "insert into yx_daytopic_campus_new values(" ;
        String update6 = "insert into yx_daytopic_bus_explosion_new values(";
        init(in1);
        filter(sql1,sql4,update1,update4);
        init(in2);
        filter(sql2,sql5,update2,update5);
        init(in3);
        filter(sql3,sql6,update3,update6);
        st.close();
        connection.close();
        System.out.println("done");
    }
    
    //初始化
    public void init(String input) throws IOException{
        if(dates == null)
            dates = new ArrayList<String>();
        if(relative_values == null)
            relative_values = new ArrayList<Double>();
        
        dates.clear();
        relative_values.clear();
        
        BufferedReader br = new BufferedReader(new FileReader(input));
        String l = "";
        while((l = br.readLine()) != null){
            if(l.length() > 0){
                String[] items = l.split("/");
                if(items != null){
                    dates.add(items[0]);
                    relative_values.add(Double.parseDouble(items[1]));
                }
            }
        }
        
        br.close();
    }
    
    public void filter(String query,String sql2,String update,String update2) throws SQLException{
        Map<String,Integer> peopleCache = new HashMap<String,Integer>();
        Map<String,Integer> locationCache = new HashMap<String,Integer>();
        Map<String,Integer> timeCache = new HashMap<String,Integer>();
        Map<String,Integer> tagCache = new HashMap<String,Integer>();
        Map<String,Map<String,Double>> imp_cache = new HashMap<String,Map<String,Double>>();
        ResultSet rs = null;
        int ct = 0;
        for(String date : dates){
            peopleCache.clear();
            locationCache.clear();
            timeCache.clear();
            tagCache.clear();
            rs = st.executeQuery(query.replace("#", date));
            imp_cache.clear();
            int counter = 0;
            int max_cosin_idnum = -1;
            double max_cosin = -1;
            
            //分别统计人物、地点、时间戳到出现次数，为后续处理做准备
            while(rs.next()){
                counter ++;
                int id = rs.getInt("idnum");
                String people = rs.getString("person");
                String location = rs.getString("location");
                String time = rs.getString("first_time");
                String time_tag = rs.getString("time_tag");
                double cosin = rs.getDouble("cosin");
                String all_impor = rs.getString("all_important");
                
                //分割字段
                if(people != null && people.trim().length() > 0){
                    String [] arr_people = people.split(" ");
                    if(arr_people != null){
                        for(String p : arr_people){
                            if(p != null && p.trim().length() > 0)
                                if(!peopleCache.containsKey(p)){
                                    peopleCache.put(p, 1);
                                }
                                else{
                                    peopleCache.put(p, peopleCache.get(p) + 1);
                                }
                        }
                    }
                }
                
                if(location != null && location.trim().length() > 0 ){
                    String [] arr_location = location.split("#");
                    if(arr_location != null){
                        for(String l : arr_location){
                            if(l != null && l.trim().length() > 0) 
                                if(!locationCache.containsKey(l))
                                    locationCache.put(l, 1);
                                else
                                    locationCache.put(l, locationCache.get(l) + 1);
                        }
                    }
                }
                
                if(time != null && time.trim().length() > 0 ){
                    String [] arr_time = time.split(" ");
                    if(arr_time != null){
                        for(String t : arr_time){
                            if(t != null && t.trim().length() > 0)
                                if(!timeCache.containsKey(t))
                                    timeCache.put(t, 1);
                                else
                                    timeCache.put(t, timeCache.get(t) + 1);
                        }
                    }
                }
                
                if(time_tag != null && time_tag.trim().length() > 0){
                    String [] arr_tag = time_tag.split(" ");
                    if(arr_tag != null)
                        for(String t : arr_tag){
                            if(t != null && t.trim().length() > 0)
                                if(!tagCache.containsKey(t))
                                    tagCache.put(t, 1);
                                else
                                    tagCache.put(t, tagCache.get(t) + 1);
                        }
                }
                
                //以计算平均值的方式生成all_important字段
                if(all_impor != null){
                    String [] arr_imp = all_impor.split("#");
                    if(arr_imp != null){
                        for(String i : arr_imp){
                            if(i != null && i.trim().length() > 0){
                                String [] tmp = i.split("/");
                                if(tmp != null && !tmp[1].equals("ns")&& !tmp[1].equals("nsf") && !tmp[1].equals("nr")
                                        && !tmp[1].equals("nr1") && !tmp[1].equals("nr2") && !tmp[1].equals("nrj") 
                                        && !tmp[1].equals("nrf")){
                                    String wd = tmp[0] + "/" + tmp[1];
                                    double val = Double.parseDouble(tmp[2]);
                                    if(!imp_cache.containsKey(wd)){
                                        Map<String,Double> mm = new HashMap<String,Double>();
                                        mm.put("sum", val);
                                        mm.put("count", (double) 1);
                                        imp_cache.put(wd, mm);
                                    }
                                    else{
                                        imp_cache.get(wd).put("sum", imp_cache.get(wd).get("sum") + val);
                                        imp_cache.get(wd).put("count", imp_cache.get(wd).get("count") + 1);
                                    }
                                }
                            }
                        }
                    }
                }
                
                if(cosin > max_cosin){
                    max_cosin = cosin;
                    max_cosin_idnum = id;
                }

            }
            
            StringBuilder sb = new StringBuilder();
            //计算新all_important
            String [] arr_imp = imp_cache.keySet().toArray(new String[0]);
            if(arr_imp != null){
                AItem [] arr = new AItem[arr_imp.length];
                int cct = 0;
                for(String s : arr_imp){
                    if(s != null && s.trim().length() > 0){
                        double res = imp_cache.get(s).get("sum") / imp_cache.get(s).get("count");
                        arr[cct] = new AItem(s,res);
                        cct ++;
                    }
                }
                
                Arrays.sort(arr, new AIComparator());
                int ct2 = 1;
                for(AItem ai : arr){
                    if(ct2 <= 20){
                        sb.append(ai.wd + "/" + ai.val + "#");
                        ct2 ++;
                    }else
                        break;
                }
                if(sb.lastIndexOf("#") >= 0 && sb.lastIndexOf("#") == sb.length() - 1)
                    sb.deleteCharAt(sb.length() - 1);
            }
                
                
            
            
            //对缓存内容排序并截取结果的前x条
            String people_field = sort(peopleCache,counter / 12,3," ");
            String location_field = sort(locationCache,counter / 12,3,"#");
            String time_field = sort(timeCache,counter / 8,1," ");
            String tag_field = sort(tagCache,counter / 15,1," ");
            double val_field = relative_values.get(ct);
            ct ++;
            //提取最大cosin新闻的其他字段内容
            ResultSet rs2 = st.executeQuery(sql2.replace("#", String.valueOf(max_cosin_idnum)));
            if(rs2 != null && rs2.next()){
                String id = rs2.getString("id");
                BigDecimal url_crc = rs2.getBigDecimal("url_crc");
                Date release_date = rs2.getDate("release_date");
                int source_type = rs2.getInt("source_type");
                String media_name = rs2.getString("media_name");
                Date release_date_day = rs2.getDate("release_date_day");
                String title = rs2.getString("title");
                String format_content = rs2.getString("format_content");
                BigDecimal siteurl_crc = rs2.getBigDecimal("siteurl_crc");
                String hit_tag = rs2.getString("hit_tag");
                String abs = rs2.getString("abs");
                String type = rs2.getString("type");
                String title_important = rs2.getString("title_important");
                String abs_important = rs2.getString("abs_important");
                String content_important = rs2.getString("content_important");
                String all_important = rs2.getString("all_important");
                
                String release_time = rs2.getString("release_time");
                
                String order_time = rs2.getString("order_time");
                String time = rs2.getString("time");
                String content_media_name = rs2.getString("content_media_name");
                int words = rs2.getInt("words");
                int validate_tag = rs2.getInt("validate_tag");
                double score = rs2.getFloat("score");
                int analogy = rs2.getInt("analogy");
                String q = update + max_cosin_idnum + ",'" + id + "'," + url_crc
                        + ",'" + release_date + "'," + source_type + ",'" + media_name + "','"
                        + release_date_day + "','" + title + "','" + format_content
                        + "'," + siteurl_crc + ",'" + hit_tag + "','" + abs + "','" + type
                        + "','" + title_important + "','" + abs_important + "','" + content_important
                        + "','" + all_important + "','" + people_field + "','" + location_field
                        + "','" + release_time + "','" + tag_field + "','" + time_field + "','"
                        + order_time + "','" + time + "','" + content_media_name + "'," + words
                        + "," + validate_tag + "," + max_cosin + "," + score + "," + analogy
                        + "," + counter + "," + val_field + ")";
                String p = update2 + max_cosin_idnum + ",'" + id + "'," + url_crc
                        + ",'" + release_date + "'," + source_type + ",'" + media_name + "','"
                        + release_date_day + "','" + title + "','" + format_content
                        + "'," + siteurl_crc + ",'" + hit_tag + "','" + abs + "','" + type
                        + "','" + title_important + "','" + abs_important + "','" + content_important
                        + "','" + sb.toString() + "','" + people_field + "','" + location_field
                        + "','" + release_time + "','" + tag_field + "','" + time_field + "','"
                        + order_time + "','" + time + "','" + content_media_name + "'," + words
                        + "," + validate_tag + "," + max_cosin + "," + score + "," + analogy
                        + "," + counter + "," + val_field + ")";
                System.out.println(q);
                
                st.executeUpdate(q);
                st.executeUpdate(p);
                rs2.close();
            }
            
        }
        System.out.println("已处理完一个主题");
    }
    
    public String sort(Map<String,Integer> map,int threshold,int holdnum,String spliter){
            String [] top_items = new String[holdnum];
            int [] top_values = new int[holdnum];
            StringBuilder statement = new StringBuilder();
            int number_of_current_topic = 0;
            //选择排序
            for(int i = 0;i < holdnum; i ++){
                String[] arr = map.keySet().toArray(new String[0]);
                if(arr != null){
                    String current_max = null;
                    int len = arr.length;
                    for(int j = 0;j < len;j ++ ){
                        if(map.get(arr[j]) > threshold && (current_max == null || map.get(arr[j]) > map.get(current_max))){
                            current_max = arr[j];
                        }
                    }
                    if(current_max != null){
                        top_items[i] = current_max;
                        top_values[i] = map.get(current_max);
                        map.remove(current_max);
                        number_of_current_topic ++;
                    }
                    else{
                        System.out.println("current_max is null...");
                    }
                    
                }
                
            }
            if(number_of_current_topic > 0){
                for(int i = 0;i < Math.min(holdnum, number_of_current_topic); i ++){
                    statement.append(top_items[i] + spliter);
                }
                if(statement.lastIndexOf(spliter) == statement.length() - 1)
                    statement.deleteCharAt(statement.length() - 1);
                return statement.toString();
            }
            return "";
            
    }
    
}

class AItem{
    String wd;
    double val;
    public AItem(String i,double c){
        wd = i;
        val = c;
    }
}
class AIComparator implements Comparator<AItem>{

    @Override
    public int compare(AItem o1, AItem o2) {
        if(o1 != null && o2 != null){
            
            double vala = o1.val;
            double valb = o2.val;
            if(vala < valb)
                return 1;
            if(vala > valb)
                return -1;
            return 0;
        }
        return 0;
        
    }

}

