package me.ralphya0.pre_processing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.ralphya0.tools.DB;

public class NewsPropagateAnalysis {

    
    Connection connection = null;
    Statement st = null;
    Statement st2 = null;
    Statement st3 = null;
    //key为自然月份
    static Map<String,Map<String,Integer>> cache = new HashMap<String,Map<String,Integer>>();
    //原始出处
    static Map<String,List<String>> originSource = new HashMap<String,List<String>>();
    //用户类型
    Map<Integer,List<String>> userType = new HashMap<Integer,List<String>>();
    
    //对事件的传播进行数据统计
    public NewsPropagateAnalysis() throws SQLException, IOException{
        run();
        
    }
    
    public void run() throws SQLException, IOException{
        String sql1 = "select * from violence_model_day";
        String sql2 = "select * from campus_model_day";
        String sql3 = "select * from bus_model_day";
        
        String sql12 = "select * from violence_cluster_news where analogy = ";
        String sql22 = "select * from campus_cluster_news where analogy = ";
        String sql32 = "select * from bus_cluster_news where analogy = ";
        
        String sql13 = "insert into xxx1 values(";
        String sql23 = "insert into xxx2 values(";
        String sql33 = "insert into xxx3 values(";
        
        String out1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-9\\huizong\\violence_huizong.csv";
        String out2 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-9\\huizong\\campus_huizong.csv";
        String out3 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-9\\huizong\\bus_huizong.csv";
        
        //读取用户聚类文件
        List<String> l1 = new ArrayList<String>();
        List<String> l2 = new ArrayList<String>();
        List<String> l3 = new ArrayList<String>();
        List<String> l4 = new ArrayList<String>();
        userType.put(1, l1);
        userType.put(2, l2);
        userType.put(3, l3);
        userType.put(4, l4);
        String in = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-7\\两步聚类结果.csv";
        BufferedReader br = new BufferedReader(new FileReader(in));
        br.readLine();
        String l = "";
        while((l = br.readLine()) != null && l.trim().length() > 0){
            String [] arr = l.split(",");
            if(arr != null && !arr[arr.length - 1].equals("$null$")){
                userType.get(Integer.parseInt(arr[arr.length - 1])).add(arr[0]);
            }
            else if(arr[arr.length - 1].equals("$null$")){
                userType.get(4).add(arr[0]);
            }
        }
        br.close();
        connection = new DB().getConn();
        st = connection.createStatement();
        st2 = connection.createStatement();
        st3 = connection.createStatement();
        process(sql1,sql12,null,out1);
        
        process(sql2,sql22,null,out2);
        
        process(sql3,sql32,null,out3);
        
        st.close();
        st2.close();
        st3.close();
        connection.close();
        System.out.println("done");
    }
    
    public int process(String sql0,String sql,String sql2,String output) throws SQLException, IOException{
        int counter = 0;
        cache.clear();
        originSource.clear();

        
        //查xx_model_day表，按模板处理
        ResultSet rs = st.executeQuery(sql0) ;
        if(rs != null){
            while(rs.next()){
                int analogy = rs.getInt("analogy");
                //读取时间
                String time_tag = rs.getString("time_tag");
                String first_time = rs.getString("first_time");
                String release_time = rs.getString("release_date_day");
                String month = null;
                int month_num = 0;
                if(time_tag != null && time_tag.trim().length() > 0){
                    String [] arr = time_tag.split("·");
                    if(arr != null){
                        month = arr[0] + "月";
                        month_num = Integer.parseInt(arr[0]);
                    }
                }
                else if(first_time != null && first_time.trim().length() > 0){
                    first_time = first_time.replace("月", "-");
                    String [] arr = first_time.split("-");
                    if(arr != null){
                        month = arr[0] + "月";
                        month_num = Integer.parseInt(arr[0]);
                    }
                }
                else{
                    String [] arr = release_time.split("-");
                    if(arr != null){
                        month_num = Integer.parseInt(arr[1]);
                        month = month_num + "月";
                    }
                }
                
                //把该模板按自然月归类
                if(!cache.containsKey(month)){
                    Map<String,Integer> mm = new HashMap<String,Integer>();
                    mm.put("month_num", month_num);
                    mm.put("type_count", 0);
                    mm.put("media_count", 0);
                    mm.put("web_count",0);
                    mm.put("news_count", 0);
                    mm.put("user_num", 0);
                    mm.put("trans_count", 0);
                    //用户类型
                    mm.put("user_type_1", 0);
                    mm.put("user_type_2", 0);
                    mm.put("user_type_3", 0);
                    //离群值与资讯新闻传播数分离
                    mm.put("user_type_9", 0);
                    mm.put("user_type_0", 0);
                    mm.put("comment_count", 0);
                    mm.put("quote_count", 0);
                    mm.put("attitudes_count", 0);
                    mm.put("inter_time", 0);
                    cache.put(month, mm);
                    List<String> ll = new ArrayList<String>();
                    originSource.put(month, ll);
                }
                
                //更新当月各项统计值
                cache.get(month).put("type_count", cache.get(month).get("type_count") + 1);
                
                //从xx_cluster_news中取出描述相应事件的新闻
                ResultSet rs2 = st2.executeQuery(sql + analogy);
                while(rs2.next()){
                    String content_media_name = rs2.getString("content_media_name");
                    //原始出处去重
                    if(!originSource.get(month).contains(content_media_name)){
                        originSource.get(month).add(content_media_name);
                        cache.get(month).put("media_count", cache.get(month).get("media_count") + 1);
                    }
                    
                    int source_type = rs2.getInt("source_type");
                    BigDecimal url_crc = rs2.getBigDecimal("url_crc");
                    BigDecimal siteurl_crc = rs2.getBigDecimal("siteurl_crc");
                    
                    if(source_type == 0){
                        //新闻出现次数
                        cache.get(month).put("news_count", cache.get(month).get("news_count") + 1);
                        cache.get(month).put("user_type_0", cache.get(month).get("user_type_0") + 1);
                    }
                    else if(source_type == 4){
                        //统计用户类型
                        if(userType.get(1).contains(String.valueOf(siteurl_crc))){
                            cache.get(month).put("user_type_1", cache.get(month).get("user_type_1") + 1);
                        }
                        else if(userType.get(2).contains(String.valueOf(siteurl_crc))){
                            cache.get(month).put("user_type_2", cache.get(month).get("user_type_2") + 1);
                        }
                        else if(userType.get(3).contains(String.valueOf(siteurl_crc))){
                            cache.get(month).put("user_type_3", cache.get(month).get("user_type_3") + 1);
                        }
                        else if(userType.get(4).contains(String.valueOf(siteurl_crc))){
                            cache.get(month).put("user_type_9", cache.get(month).get("user_type_9") + 1);
                        }
                        
                        ResultSet rs3 = null ;
                        //统计微博原始发布次数
                        
                        String type = rs2.getString("type");
                        
                        if(type.equals("0")){
                            //原创微博
                            cache.get(month).put("user_num", cache.get(month).get("user_num") + 1);
                        }
                        else if(type.equals("1")){
                            //转发微博
                            cache.get(month).put("trans_count", cache.get(month).get("trans_count") + 1);
                        }
                        
                        //统计当前微博回复总数、支持总数、点赞总数
                        rs3 = st3.executeQuery("select * from t_dpt_comp where url_crc = " + url_crc);
                        if(rs3 != null && rs3.next()){
                            Integer huifu = rs3.getInt("comment_count");
                            Integer zhichi = rs3.getInt("quote_count");
                            Integer zan = rs3.getInt("attitudes_count");
                            if(huifu == null){
                                huifu = 0;
                            }
                            if(zhichi == null){
                                zhichi = 0;
                            }
                            if(zan == null){
                                zan = 0;
                            }
                            
                            cache.get(month).put("comment_count", cache.get(month).get("comment_count") + huifu);
                            cache.get(month).put("quote_count", cache.get(month).get("quote_count") + zhichi);
                            cache.get(month).put("attitudes_count", cache.get(month).get("attitudes_count") + zan);
                            
                            cache.get(month).put("web_count", cache.get(month).get("web_count") + 1 + huifu + zhichi + zan);
                            
                        }
                        
                    }
                }
                System.out.println("处理完一个模板事件");
            }
            //计算时间间隔
            String [] arr_tmp = cache.keySet().toArray(new String[0]);
            
            if(arr_tmp != null){
                System.out.println("当前话题共有 " + arr_tmp.length + " 个月份");
                
                int [] indics = new int[12];
                for(int i = 0;i < 12;i ++){
                    indics[i] = -1;
                }
                for(String s : arr_tmp){
                    indics[cache.get(s).get("month_num") - 1] = 1;
                }
                for(String s : arr_tmp){
                    int num = cache.get(s).get("month_num");
                    int ct2 = 1;
                    while(num -1 - ct2 >= 0 && indics[num -1 - ct2] == -1){
                        ct2 ++;
                    }
                    cache.get(s).put("inter_time", ct2);
                    
                }
                
                
            }
            
            //写入数据表和文件
            StringBuilder sb = new StringBuilder();
            sb.append("month,type_count,media_count,web_count,news_count,user_num,trans_count,user_type_1,user_type_2,user_type_3,user_type_9,user_type_0,comment_count,quote_count,"
                    + "attitudes_count,inter_time \n");
            
            for(String s : arr_tmp){
                //String utype = "type-1:" + cache.get(s).get("user_type_1") + "#type-2:" + cache.get(s).get("user_type_2") + "#type-3:" + cache.get(s).get("user_type_3");
                sb.append(s + "," + cache.get(s).get("type_count") + "," + cache.get(s).get("media_count") + "," + cache.get(s).get("web_count")
                        + "," + cache.get(s).get("news_count") + "," + cache.get(s).get("user_num") + "," + cache.get(s).get("trans_count")
                        + "," +  cache.get(s).get("user_type_1") + "," + cache.get(s).get("user_type_2") + "," + cache.get(s).get("user_type_3") + ","
                        + cache.get(s).get("user_type_9") + "," + cache.get(s).get("user_type_0") + "," + cache.get(s).get("comment_count") + "," + cache.get(s).get("quote_count") + ","
                        + cache.get(s).get("attitudes_count") + "," + cache.get(s).get("inter_time") + "\n");
                
                
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(output));
            bw.write(sb.toString());
            bw.close();
            System.out.println("一个话题处理完成");
        }
        return counter;
    }
    
    public static void main(String[] args) throws SQLException, IOException {
        new NewsPropagateAnalysis();
    }
}
