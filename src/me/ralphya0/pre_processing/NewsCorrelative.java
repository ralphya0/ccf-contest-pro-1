package me.ralphya0.pre_processing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import me.ralphya0.tools.DB;

//统计暴恐事件间的时间关联性
public class NewsCorrelative {

    public NewsCorrelative(){
        
    }
    
    //暂存对2011年4月至2014年4月的暴恐事件的统计信息
    Map<Integer,Map<Integer,Map<String,Integer>>> cache = new HashMap<Integer,Map<Integer,Map<String,Integer>>>();
    
    Map<Integer,Map<String,String>> news = new HashMap<Integer,Map<String,String>>();
    
    Map<Integer,Integer> latest = new HashMap<Integer,Integer>();
    
    public void run(int inter) throws IOException, SQLException, ClassNotFoundException{
        //初始化
        for(int i = 2011;i <= 2014;i ++){
            Map<Integer,Map<String,Integer>> m2 = new HashMap<Integer,Map<String,Integer>>();
            for(int j = 1;j <=12 ; j ++){
                Map<String,Integer> m1 = new HashMap<String,Integer>();
                m1.put("zixun", 0);
                m1.put("weibo", 0);
                m1.put("renmin", 0);
                m1.put("total", 0);
                m2.put(j, m1);
                
            }
            cache.put(i, m2);
        }
        
        latest.put(1, -1);
        latest.put(2, 0);
        latest.put(3, 5);
        latest.put(6, 16);
        latest.put(7, 2);
        latest.put(10, 4);
        latest.put(12, 1);
        latest.put(14, 1);
        latest.put(15, 0);
        latest.put(16, 1);
        latest.put(17, 1);
        latest.put(18, 1);
        
        String in = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-12\\in.txt";
        BufferedReader br = new BufferedReader(new FileReader(in));
        String l = "";
        while((l = br.readLine()) != null){
            String [] arr = l.split(",");
            if(arr != null){
                Map<String,String> mm = new HashMap<String,String>();
                String [] tmp = arr[0].split("-");
                if(tmp != null){
                    mm.put("year", tmp[0]);
                    mm.put("month", tmp[1]);
                    mm.put("day", tmp[2]);
                }
                mm.put("location", arr[1]);
                //取之前聚类之后的新闻分类号
                int analogy = Integer.parseInt(arr[2]);
                //缓存
                news.put(analogy, mm);
                
            }
        }
        br.close();
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://202.113.76.41:3306/relative", "yaoxin", "310b");
        Statement st1 = connection.createStatement();
        Statement st2 = connection.createStatement();
        Statement st3 = connection.createStatement();
        System.out.println("init success");
        ResultSet rs1 = null;
        ResultSet rs2 = null;
        ResultSet rs3 = null;
        //由violence_cluster表计算各统计量
        String sql1 = "select count(*) from violence_cluster where analogy = @1 and source_type = #2";
        String sql2 = "select count(*) from violence_cluster where analogy = @1 and media_name like '%人民日报%'";
        Integer[] keys = news.keySet().toArray(new Integer[0]);
        for(Integer i : keys){
            int year = Integer.parseInt(news.get(i).get("year"));
            int month = Integer.parseInt(news.get(i).get("month"));
            cache.get(year).get(month).put("total", cache.get(year).get(month).get("total") + 1);
            
            rs1 = st1.executeQuery(sql1.replace("@1", String.valueOf(i)).replace("#2", String.valueOf(0)));
            if(rs1 != null && rs1.next()){
                int zixun = rs1.getInt(1);
                if(zixun > 0)
                    cache.get(year).get(month).put("zixun", cache.get(year).get(month).get("zixun") + zixun);
                
            }
            
            rs2 = st2.executeQuery(sql1.replace("@1", String.valueOf(i)).replace("#2", String.valueOf(4)));
            if(rs2 != null && rs2.next()){
                int weibo = rs2.getInt(1);
                if(weibo > 0){
                    cache.get(year).get(month).put("weibo", cache.get(year).get(month).get("weibo") + weibo);
                }
            }
            
            rs3 = st3.executeQuery(sql2.replace("@1", String.valueOf(i)));
            if(rs3 != null && rs3.next()){
                int renmin = rs3.getInt(1);
                if(renmin > 0){
                    cache.get(year).get(month).put("renmin", cache.get(year).get(month).get("renmin") + renmin);
                }
            }
            
            if(i == 10){
                //补充
                rs1 = st1.executeQuery(sql1.replace("@1", String.valueOf(11)).replace("#2", String.valueOf(0)));
                if(rs1 != null && rs1.next()){
                    int zixun = rs1.getInt(1);
                    if(zixun > 0)
                        cache.get(year).get(month).put("zixun", cache.get(year).get(month).get("zixun") + zixun);
                    
                }
                
                rs2 = st2.executeQuery(sql1.replace("@1", String.valueOf(11)).replace("#2", String.valueOf(4)));
                if(rs2 != null && rs2.next()){
                    int weibo = rs2.getInt(1);
                    if(weibo > 0){
                        cache.get(year).get(month).put("weibo", cache.get(year).get(month).get("weibo") + weibo);
                    }
                }
                
                rs3 = st3.executeQuery(sql2.replace("@1", String.valueOf(11)));
                if(rs3 != null && rs3.next()){
                    int renmin = rs3.getInt(1);
                    if(renmin > 0){
                        cache.get(year).get(month).put("renmin", cache.get(year).get(month).get("renmin") + renmin);
                    }
                }
            }
        }
        System.out.println("统计结束,开始输出...");
        rs1.close();
        rs2.close();
        rs3.close();
        st1.close();
        st2.close();
        st3.close();
        connection.close();
        
        StringBuilder sb = new StringBuilder();
        sb.append("暴恐态势感知指标 (时间跨度为" + inter + "个月)：\n");
        sb.append("事件,news_count,weibo_count,Peoples_Daily_count,time_span,event_count\n");
        
        //根据间隔参数计算统计量
        Arrays.sort(keys);
        
        for(Integer i : keys){
            int year = Integer.parseInt(news.get(i).get("year"));
            int month = Integer.parseInt(news.get(i).get("month"));
            int day = Integer.parseInt(news.get(i).get("day"));
            int yeart = year;
            int montht = month;
            //计算需要考察的时间跨度
            int [] years = new int[inter];
            int [] months = new int[inter];
            
            for(int j = 0;j < inter; j ++){
                if(month > 1){
                    years[j] = year;
                    months[j] = month - 1;
                    month --;
                }
                else if(month == 1){
                    years[j] = year - 1;
                    months[j] = 12;
                    month = 12;
                    year --;
                }
                else
                    System.out.println("出错!");
                
                
            }
            
            //累加时间跨度范围内的各项统计值
            int zixun = 0;
            int weibo = 0;
            int renmin = 0;
            int total = 0;
            for(int k = 0;k < inter;k ++){
                zixun += cache.get(years[k]).get(months[k]).get("zixun");
                weibo += cache.get(years[k]).get(months[k]).get("weibo");
                renmin += cache.get(years[k]).get(months[k]).get("renmin");
                total += cache.get(years[k]).get(months[k]).get("total");
                
            }
            //输出
            sb.append(yeart + "-" + montht + "-" + day + "," + zixun + "," + weibo + "," + renmin + "," + latest.get(i) + "," + total + "\n");
        }
        String out = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-12\\violence_tongji.csv";
        BufferedWriter bw = new BufferedWriter(new FileWriter(out));
        bw.write(sb.toString());
        bw.close();
        System.out.println("done");
       
    }
    
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        new NewsCorrelative().run(3);
    }
}
