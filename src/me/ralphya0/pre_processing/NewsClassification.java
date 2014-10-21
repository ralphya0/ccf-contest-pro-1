package me.ralphya0.pre_processing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.ralphya0.tools.DB;

public class NewsClassification {

          /*  暴恐事件（3）：
            暴力+恐怖
            暴恐
            暴行+恐怖
        
            校园砍伤事件（4）：
            校园+砍伤
            学校+砍伤
            学生+砍伤
            校园内+砍伤
        
            公交车爆炸事件（6）：
            公交车+爆炸
            公交车+纵火
            公交+爆炸
            公交+纵火
            公交车+泼油
            公交+泼油*/
    

	
	Connection connection = null;
	Statement st1 = null;
	Statement st2 = null;
	
	public NewsClassification() throws SQLException, IOException{
	    connection = new DB().getConn();
	    st1 = connection.createStatement();
	    st2 = connection.createStatement();
	    
	    fetching(1);
	}
	
	
	
	Map<String,Map<String,Double>> terrorist = new HashMap<String,Map<String,Double>>();
	Map<String,Map<String,Double>> campus = new HashMap<String,Map<String,Double>>();
	Map<String,Map<String,Double>> bus = new HashMap<String,Map<String,Double>>();
	
	int terroristWordCounter = 0;
	int campusWordCounter = 0;
	int busWordCounter = 0;

	public void fetching(int type) throws SQLException, IOException{
	    
	    if(type > 3)
	        return ;

		ResultSet rs = null;
		
		int round = 1;
		int count = 0;
		String sql = null;
		
		do{
		    if(type == 1)
		        sql = "select idnum,all_important from violence limit " + (round - 1)*5000 + ",5000";
		    else if(type == 2)
		        sql = "select idnum,all_important from campus limit " + (round - 1)*5000 + ",5000";
		    else if(type == 3)
		        sql = "select idnum,all_important from bus_explosion limit " + (round - 1)*5000 + ",5000";
		    
			rs = st1.executeQuery(sql);
			
			count = validateAndUpdate(rs,type);
			rs.close();
			System.out.println("fetching and updating round " + round + " complete [ type " + type + " ]");
			
			round ++;
		}while(count == 5000);
		
		System.out.println("type " + type + " complete!");
		
		String input = "";
		String output1 = "";
		String output2 = "";
		
		Map<String,Double> aveValue = new HashMap<String,Double>();
		double a = 0;
		double b = 0;
		double c = 0;
		StringBuilder sb = new StringBuilder();
		if(type == 1){
		    //计算话题关键词平均权重
		    sb.append("暴恐话题前100个关键词平均权重: \n");
		    String [] arr = this.terrorist.keySet().toArray(new String[0]);
		    for(String i : arr){
		        sb.append(i + "," + (this.terrorist.get(i).get("sum") / this.terrorist.get(i).get("times")) + "\n");
		        aveValue.put(i, (this.terrorist.get(i).get("sum") / this.terrorist.get(i).get("times")));
		    }
		    input = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\terrorist-tmp.csv";
		    output1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\baokong_word_top_100.csv";
		    output2 ="F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\baokong-topic-cosin.csv";
		}
		else if(type == 2){
            sb.append("校园砍伤话题前100个关键词平均权重: \n");
            String [] arr = this.campus.keySet().toArray(new String[0]);
            for(String i : arr){
                sb.append(i + "," + (this.campus.get(i).get("sum") / this.campus.get(i).get("times")) + "\n");
                aveValue.put(i, (this.campus.get(i).get("sum") / this.campus.get(i).get("times")));
            }
            
            input = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\campus-tmp.csv";
            output1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\xiaoyuan_word_top_100.csv";
            output2 ="F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\xiaoyuan-topic-cosin.csv";
		}
		else if(type == 3){
            sb.append("公交爆炸话题前100个关键词平均权重: \n");
            String [] arr = this.bus.keySet().toArray(new String[0]);
            for(String i : arr){
                sb.append(i + "," + (this.bus.get(i).get("sum") / this.bus.get(i).get("times")) + "\n");
                aveValue.put(i, (this.bus.get(i).get("sum") / this.bus.get(i).get("times")));
            }
            
            input = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\bus-tmp.csv";
            output1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\gongjiao_word_top_100.csv";
            output2 ="F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\gongjiao-topic-cosin.csv";
		}
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(output1));
        bw.write(sb.toString());
        bw.close();
        System.out.println("top 100 words of type " + type + " written to " + output1);
        sb = null;
        
        //根据话题关键词平均权重计算各条新闻的余弦值
        String [] topicKeys = aveValue.keySet().toArray(new String[0]);
        
        for(String s : topicKeys){
            b += Math.pow(aveValue.get(s), 2);
        }
        b = Math.sqrt(b);
        
        BufferedReader br = new BufferedReader(new FileReader(input));
        String l = "";
        StringBuilder sb2 = new StringBuilder();
        while((l = br.readLine()) != null){
            String [] ls = l.split(",");
            if(ls != null){
                String [] words = ls[1].split("#");
                if(words != null){
                    for(String s : words){
                        String [] items = s.split("/");
                        if(items != null){
                            c += Math.pow(Double.parseDouble(items[2]), 2);
                            
                            if(aveValue.containsKey(items[0])){
                                a += aveValue.get(items[0]) * Double.parseDouble(items[2]);
                            }
                        }
                    }
                    c = Math.sqrt(c);
                    double cos = a / (b * c);
                    sb2.append(ls[0] + "," + cos + "\n");
                    
                }
            }
        }
        br.close();
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(output2));
        bw2.write(sb2.toString());
        bw2.close();
        System.out.println("cosin of type " + type + " written to " + output2);
		sb2 = null;
		aveValue.clear();
		aveValue = null;
		
		
		fetching(type + 1);
	}
	
	
	public int validateAndUpdate(ResultSet rs,int type) throws SQLException, IOException{
		int counter = 0;
		if(rs != null){
		    List<Integer> idCache = new ArrayList<Integer>();
		    
		    StringBuilder sb = new StringBuilder();
		    
			while(rs.next()){
			    counter ++;
			    int id = rs.getInt("idnum");
			    String allImp = rs.getString("all_important");
			    String [] tmp = null;
			    if(allImp != null){
			        
			    //1代表处理暴恐事件
			        if(type == 1){
			        
			            if((allImp.contains("暴力") && allImp.contains("恐怖")) || (allImp.contains("暴恐"))
			                    || (allImp.contains("暴行") && allImp.contains("恐怖"))){
			                idCache.add(id);
			                sb.append(id + "," + allImp + "\n");
			                tmp = allImp.split("#");
			                
			                if(tmp != null){
			                    for(String s : tmp){
			                        String [] tmp2 = s.split("/");
			                        if(tmp2 != null){
			                            if(!this.terrorist.containsKey(tmp2[0]) && this.terroristWordCounter < 100){
			                                Map<String,Double> mm = new HashMap<String,Double>();
			                                mm.put("times", (double)1);
			                                mm.put("sum", Double.parseDouble(tmp2[2]));
			                                this.terrorist.put(tmp2[0], mm);
			                                this.terroristWordCounter ++;
			                            }
			                            else if(this.terrorist.containsKey(tmp2[0])){
			                                terrorist.get(tmp2[0]).put("sum",terrorist.get(tmp2[0]).get("sum") + Double.parseDouble(tmp2[2]));
			                                terrorist.get(tmp2[0]).put("times",terrorist.get(tmp2[0]).get("times") + 1);
			                            }
			                            else{
			                                //ignore
			                            }
			                             
			                        }
			                    }
			                }
			            }
			        
			        }
			        else if(type == 2){
	                    if((allImp.contains("校园") && allImp.contains("砍伤")) || (allImp.contains("学校") && allImp.contains("砍伤"))
	                            || (allImp.contains("学生") && allImp.contains("砍伤")) || (allImp.contains("学校内") && allImp.contains("砍伤"))){
	                        idCache.add(id);
	                        sb.append(id + "," + allImp + "\n");
                            tmp = allImp.split("#");
                            
                            if(tmp != null){
                                for(String s : tmp){
                                    String [] tmp2 = s.split("/");
                                    if(tmp2 != null){
                                        if(!this.campus.containsKey(tmp2[0]) && this.campusWordCounter < 100){
                                            Map<String,Double> mm = new HashMap<String,Double>();
                                            mm.put("times", (double)1);
                                            mm.put("sum", Double.parseDouble(tmp2[2]));
                                            this.campus.put(tmp2[0], mm);
                                            this.campusWordCounter ++;
                                        }
                                        else if(this.campus.containsKey(tmp2[0])){
                                            campus.get(tmp2[0]).put("sum",campus.get(tmp2[0]).get("sum") + Double.parseDouble(tmp2[2]));
                                            campus.get(tmp2[0]).put("times",campus.get(tmp2[0]).get("times") + 1);
                                        }
                                        else{
                                            //ignore
                                        }
                                         
                                    }
                                }
                            }
	                    }
	                }
			        else if(type == 3){
			            if((allImp.contains("公交车") && allImp.contains("爆炸")) || (allImp.contains("公交车") && allImp.contains("纵火"))
			                    || (allImp.contains("公交") && allImp.contains("爆炸")) || (allImp.contains("公交") && allImp.contains("纵火"))
			                    || (allImp.contains("公交车") && allImp.contains("泼油"))
			                    || (allImp.contains("公交") && allImp.contains("泼油"))){
			                idCache.add(id);
			                
			                sb.append(id + "," + allImp + "\n");
                            tmp = allImp.split("#");
                            
                            if(tmp != null){
                                for(String s : tmp){
                                    String [] tmp2 = s.split("/");
                                    if(tmp2 != null){
                                        if(!this.bus.containsKey(tmp2[0]) && this.busWordCounter < 100){
                                            Map<String,Double> mm = new HashMap<String,Double>();
                                            mm.put("times", (double)1);
                                            mm.put("sum", Double.parseDouble(tmp2[2]));
                                            this.bus.put(tmp2[0], mm);
                                            this.busWordCounter ++;
                                        }
                                        else if(this.bus.containsKey(tmp2[0])){
                                            bus.get(tmp2[0]).put("sum",bus.get(tmp2[0]).get("sum") + Double.parseDouble(tmp2[2]));
                                            bus.get(tmp2[0]).put("times",bus.get(tmp2[0]).get("times") + 1);
                                        }
                                        else{
                                            //ignore
                                        }
                                         
                                    }
                                }
                            }
			            }
			        }
			    }
			    
				
			}
			
			//首先更新数据表
            StringBuilder sql = new StringBuilder(); 
            String fileName = null;
            if(type == 1){
                sql.append("update violence set validate_tag = 1 where idnum in(");
                fileName = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\terrorist-tmp.csv";
            }
            else if(type == 2){
                sql.append("update campus set validate_tag = 1 where idnum in(");
                fileName = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\campus-tmp.csv";
            }
            else if(type == 3){
                sql.append("update bus_explosion set validate_tag = 1 where idnum in(");
                fileName = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\bus-tmp.csv";
            }
            
            for(int t : idCache){
                sql.append(t + ",");
            }
            if(sql.lastIndexOf(",") == sql.length() - 1)
                sql.deleteCharAt(sql.length() - 1);
            sql.append(")");
            
            st2.executeUpdate(sql.toString());
            
            //将all_important存入文件
            
            BufferedWriter bw = new BufferedWriter(new FileWriter(fileName,true));
            bw.write(sb.toString());
            bw.close();
			
		}
		return counter;
	}
}
