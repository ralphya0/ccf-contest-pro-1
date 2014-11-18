package me.ralphya0.pre_processing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//提取并分析微博正文的感情信息并对其进行分析
public class EmotionAnalysis {

    Connection connection;
    
    Map<String,Map<String,Double>> event_list = new HashMap<String,Map<String,Double>>();
    
    Map<String,Integer> angry = new HashMap<String,Integer>();
    
    public EmotionAnalysis() throws ClassNotFoundException, SQLException{
        Class.forName("com.mysql.jdbc.Driver");
        
        connection = DriverManager.getConnection("jdbc:mysql://202.113.76.41:3306/relative", "yaoxin", "310b");
    }
    
    public void fun1(int inter) throws ClassNotFoundException, SQLException, IOException{
        //初始化，读取事件列表文件
        String in = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-18\\violence_tongji_new.csv";
        BufferedReader br = new BufferedReader(new FileReader(in));
        String l = "";
        while((l = br.readLine()) != null){
            String [] arr = l.split(",");
            if(arr != null){
                if(!event_list.containsKey(arr[0])){
                    Map<String,Double> mm = new HashMap<String,Double>();
                    
                    event_list.put(arr[0].replaceAll("/", "-"), mm);
                }
            }
        }
        br.close();
        
        //读取情感词汇库中代表愤怒的词
        String in2 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-18\\emotion_wd_list.csv";
        BufferedReader br2 = new BufferedReader(new FileReader(in2));
        String l2 = "";
        br2.readLine();
        while((l2 = br2.readLine()) != null){
            String [] arr = l2.split(",");
            if(arr != null){
                String type = arr[4];
                if(type != null && type.trim().length() > 0 && type.equals("NA")){
                    //愤怒类词汇
                    int val = Integer.parseInt(arr[5]);
                    angry.put(arr[0], val);
                }
            }
        }
        br2.close();
        
        System.out.println("init success");
        
        Statement st1 = connection.createStatement();
        Statement st2 = connection.createStatement();
        Statement st3 = connection.createStatement();
        Statement st4 = connection.createStatement();
        
        String sql1 = "select content_format from violence_cluster where release_date_day < '@1' and release_date_day >= '#2' and source_type = 4";
        String sql2 = "select count(*) from violence_cluster where release_date_day < '@1' and release_date_day >= '#2' and source_type = 4";
        String sql3 = "select count(*) from violence_cluster where release_date_day < '@1' and release_date_day >= '#2' and source_type = 0";
        String sql4 = "select count(*) from violence_cluster where release_date_day < '@1' and release_date_day >= '#2' and (media_name like '%人民网 公安部 法制网%' or media_name like '%公安部%' or media_name like '%法制网%')";
        String [] arr = event_list.keySet().toArray(new String[0]);
        if(arr != null){
            for(String s : arr){
                //查找当前暴恐事件发生inter天时间内的微博信息
                String [] arr2 = s.split("-");
                if(arr2 != null){
                    int day_curr = Integer.parseInt(arr[2]);
                    int month_curr = Integer.parseInt(arr[1]);
                    int year_curr  = Integer.parseInt(arr[0]);
                    
                    //构造时间窗口
                    int year = 0;
                    int month = 0;
                    int day = 0;
                    if(month_curr == 1 || month_curr == 3 || month_curr == 5 || month_curr == 7 || month_curr == 8 || month_curr == 10 ||
                            month_curr == 12 ){
                        if(day_curr + inter <= 31){
                            day = day_curr + inter ;
                            month = month_curr;
                            year = year_curr;
                        }
                        else{
                            day = day_curr + inter - 31;
                            if(month_curr + 1 <= 12){
                                month = month_curr + 1;
                                year = year_curr;
                            }
                            else{
                                month = 1;
                                year = year_curr + 1;
                            }
                        }
                    }
                    else if(month_curr == 4 || month_curr == 6 || month_curr == 9 || month_curr == 11 ){
                        if(day_curr + inter <= 30){
                            day = day_curr + inter;
                            month = month_curr;
                            year = year_curr;
                        }
                        else{
                            day = day_curr + inter - 30;
                            if(month_curr + 1 <= 12){
                                year = year_curr;
                                month = month_curr + 1;
                            }
                            else{
                                month = 1;
                                year = year_curr + 1;
                            }
                        }
                    }
                    else if(month_curr == 2){
                        if(day_curr + inter <= 28){
                            day = day_curr + inter;
                            month = month_curr;
                            year = year_curr;
                        }
                        else{
                            day = day_curr + inter - 28;
                            month = 3;
                            year = year_curr;
                        }
                    }
                    String begin_date = year_curr + "-" + month_curr + "-" + day_curr;
                    String end_date = year + "-" + month + "-" + day;
                    
                    //首先计算时间窗口内资讯新闻数、微博数和官媒数
                    ResultSet rs1 = st1.executeQuery(sql3.replace("@1", end_date).replace("#2", begin_date));
                    if(rs1 != null && rs1.next()){
                        event_list.get(s).put("zixun", (double) rs1.getInt(1));
                    }
                    rs1.close();
                    
                    ResultSet rs2 = st2.executeQuery(sql2.replace("@1", end_date).replace("#2", begin_date));
                    if(rs2 != null && rs2.next()){
                        event_list.get(s).put("weibo", (double) rs2.getInt(1));
                    }
                    rs2.close();
                    ResultSet rs3 = st3.executeQuery(sql4.replace("@1", end_date).replace("#2", begin_date));
                    if(rs3 != null && rs3.next()){
                        event_list.get(s).put("renmin", (double) rs3.getInt(1));
                    }
                    rs3.close();
                    
                    int sum = 0;

                    //取出数据表中所有处于时间窗口内的微博正文并统计愤怒指数
                    ResultSet rs = st4.executeQuery(sql1.replace("@1", end_date).replace("#2", begin_date));
                    if(rs != null){
                        while(rs.next()){
                            //统计微博正文中的愤怒情绪
                            String content = rs.getString(1);
                            if(content != null ){
                                String [] wls = content.split(" ");
                                if(wls != null){
                                    //逐词检查
                                    for(String w : wls){
                                        if(angry.containsKey(w)){
                                            sum += angry.get(w);
                                        }
                                    }
                                }
                            }
                        }
                        //计算愤怒指数平均值
                        double result = sum / event_list.get(s).get("weibo");
                        event_list.get(s).put("angry", result);
                    }
                            
                }
            }
            
            //输出最终结果
            StringBuilder sb = new StringBuilder();
            sb.append("暴恐事件微博正文愤怒情绪统计（时间间隔为" + inter + "天）:\n");
            sb.append("事件,news_count,weibo_count,Peoples_Daily_count,angry_average\n");
            
            for(String s : arr){
                sb.append(s + "," + event_list.get(s).get("zixun") + "," + event_list.get(s).get("weibo") + ","
                        + event_list.get(s).get("renmin") + "," + event_list.get(s).get("angry") + "\n");
            }
            String out = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-18\\violence_emotion.csv";
            BufferedWriter bw = new BufferedWriter(new FileWriter(out));
            bw.write(sb.toString());
            bw.close();
            System.out.println("done");
        }
        st1.close();
        st2.close();
        st3.close();
        st4.close();
        connection.close();
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        new EmotionAnalysis().fun1(7);
    }
}
