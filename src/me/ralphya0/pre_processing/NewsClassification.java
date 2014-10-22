package me.ralphya0.pre_processing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
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

public class NewsClassification {

    Connection connection = null;
	Statement st1 = null;
	Statement st2 = null;
	
	Map<String,Double> terrorist_keywd = new HashMap<String,Double>();
	Map<String,Double> campus_keyewd = new HashMap<String,Double>();
	Map<String,Double> bus_keywd = new HashMap<String,Double>();
	Item[] items = null;
	int arrCounter = 0;
	
	public NewsClassification() throws SQLException, IOException{
	    connection = new DB().getConn();
	    st1 = connection.createStatement();
	    st2 = connection.createStatement();
	    
	    fetching(1);
	}
	
	public NewsClassification(int f) throws SQLException, IOException{
	    connection = new DB().getConn();
        st1 = connection.createStatement();
	    String in1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\baokong-topic-cosin.csv";
        String in2 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\xiaoyuan-topic-cosin.csv";
        String in3 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\gongjiao-topic-cosin.csv";
        String sql1 = "update violence set cosin = #1 where idnum = @2;";
        String sql2 = "update campus set cosin = #1 where idnum = @2;";
        String sql3 = "update bus_explosion set cosin = #1 where idnum = @2;";
        
        additionalTool(in1, sql1);
        additionalTool(in2, sql2);
        additionalTool(in3, sql3);
        
        st1.close();
        connection.close();
        System.out.println("All done!");
        
        
	}
	
	public NewsClassification(String f) throws Throwable{
	    computingCosin();
    }
	
	Map<String,Map<String,Double>> terrorist = new HashMap<String,Map<String,Double>>();
	Map<String,Map<String,Double>> campus = new HashMap<String,Map<String,Double>>();
	Map<String,Map<String,Double>> bus = new HashMap<String,Map<String,Double>>();

	public void fetching(int type) throws SQLException, IOException{
	    
	    if(type > 3){
	        st1.close();
	        st2.close();
	        connection.close();
	        return ;
	    
	    }

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
		
		
		StringBuilder sb = new StringBuilder();
		Map<String,Map<String,Double>> worker = null;
		String [] arr = null;
		int wordCounter = 0;
		if(type == 1){
		    //计算话题关键词平均权重
		    
		    sb.append("暴恐话题前100个关键词平均权重: \n");
		    sb.append("word,value \n");
		    
		    arr = this.terrorist.keySet().toArray(new String[0]);
		    worker = this.terrorist;
		    
		    
		    input = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\terrorist-tmp.csv";
		    output1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\baokong_word_top_100.csv";
		    output2 ="F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\baokong-topic-cosin.csv";
		}
		else if(type == 2){
            sb.append("校园砍伤话题前100个关键词平均权重: \n");
            sb.append("word,value \n");
            arr = this.campus.keySet().toArray(new String[0]);
            worker = this.campus;
            
            input = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\campus-tmp.csv";
            output1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\xiaoyuan_word_top_100.csv";
            output2 ="F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\xiaoyuan-topic-cosin.csv";
		}
		else if(type == 3){
            sb.append("公交爆炸话题前100个关键词平均权重: \n");
            sb.append("word,value \n");
            arr = this.bus.keySet().toArray(new String[0]);
            worker = this.bus;
            
            input = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\bus-tmp.csv";
            output1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\gongjiao_word_top_100.csv";
            output2 ="F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-21\\gongjiao-topic-cosin.csv";
		}
		
		//平均权值最大的100个关键词链表
        //从小到大排序
        SortItem list = new SortItem(null,0.1,null,null);
        SortItem tail = list;
        
        for(String i : arr){
            double value = worker.get(i).get("sum") / worker.get(i).get("times");
            SortItem pt = list;
            while(pt.next != null && pt.next.val < value){
                pt = pt.next;
            }
            if(wordCounter == 100 && pt == list){
                //ignore
                continue;
            }
            else{
                    SortItem item = new SortItem(i,value,pt.next,pt);
                    if(pt.next != null)
                        pt.next.pre = item;
                    pt.next = item;
                    if(item.next == null)
                        tail = item;
                    if(wordCounter < 100)
                        wordCounter ++;
                    else {
                        list.next.next.pre = list;
                        list.next = list.next.next;
                        
                    }
            }

        }
        
        SortItem iter = tail;
        while(iter != null && iter != list){
            sb.append(iter.word + "," + iter.val + "\n");
            aveValue.put(iter.word, iter.val);
            iter = iter.pre;
        }
		
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(output1));
        bw.write(sb.toString());
        bw.close();
        System.out.println("top 100 words of type " + type + " written to " + output1);
        sb = null;
        
        //根据话题关键词平均权重计算各条新闻的余弦值
        String [] topicKeys = aveValue.keySet().toArray(new String[0]);
        double b = 0;
        
        for(String s : topicKeys){
            b += Math.pow(aveValue.get(s), 2);
        }
        b = Math.sqrt(b);
        
        BufferedReader br = new BufferedReader(new FileReader(input));
        String l = "";
        StringBuilder sb2 = new StringBuilder();
        while((l = br.readLine()) != null){
            double c = 0;
            double a = 0;
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
			                        if(tmp2 != null && !tmp2[1].equals("ns")&& !tmp2[1].equals("nsf") && !tmp2[1].equals("nr")
			                                && !tmp2[1].equals("nr1") && !tmp2[1].equals("nr2") && !tmp2[1].equals("nrj") 
			                                && !tmp2[1].equals("nrf")){
			                            if(!this.terrorist.containsKey(tmp2[0])){
			                                Map<String,Double> mm = new HashMap<String,Double>();
			                                mm.put("times", (double)1);
			                                mm.put("sum", Double.parseDouble(tmp2[2]));
			                                this.terrorist.put(tmp2[0], mm);
			                            }
			                            else{
			                                terrorist.get(tmp2[0]).put("sum",terrorist.get(tmp2[0]).get("sum") + Double.parseDouble(tmp2[2]));
			                                terrorist.get(tmp2[0]).put("times",terrorist.get(tmp2[0]).get("times") + 1);
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
                                    if(tmp2 != null && !tmp2[1].equals("ns")&& !tmp2[1].equals("nsf") && !tmp2[1].equals("nr")
                                            && !tmp2[1].equals("nr1") && !tmp2[1].equals("nr2") && !tmp2[1].equals("nrj") 
                                            && !tmp2[1].equals("nrf")){
                                        if(!this.campus.containsKey(tmp2[0])){
                                            Map<String,Double> mm = new HashMap<String,Double>();
                                            mm.put("times", (double)1);
                                            mm.put("sum", Double.parseDouble(tmp2[2]));
                                            this.campus.put(tmp2[0], mm);
                                        }
                                        else{
                                            campus.get(tmp2[0]).put("sum",campus.get(tmp2[0]).get("sum") + Double.parseDouble(tmp2[2]));
                                            campus.get(tmp2[0]).put("times",campus.get(tmp2[0]).get("times") + 1);
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
                                    if(tmp2 != null && !tmp2[1].equals("ns")&& !tmp2[1].equals("nsf") && !tmp2[1].equals("nr")
                                            && !tmp2[1].equals("nr1") && !tmp2[1].equals("nr2") && !tmp2[1].equals("nrj") 
                                            && !tmp2[1].equals("nrf")){
                                        if(!this.bus.containsKey(tmp2[0])){
                                            Map<String,Double> mm = new HashMap<String,Double>();
                                            mm.put("times", (double)1);
                                            mm.put("sum", Double.parseDouble(tmp2[2]));
                                            this.bus.put(tmp2[0], mm);
                                        }
                                        else{
                                            bus.get(tmp2[0]).put("sum",bus.get(tmp2[0]).get("sum") + Double.parseDouble(tmp2[2]));
                                            bus.get(tmp2[0]).put("times",bus.get(tmp2[0]).get("times") + 1);
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
	
	public void additionalTool(String file,String sql) throws IOException, SQLException{
	    //更新表中的cosin字段
	    
	    BufferedReader br = new BufferedReader(new FileReader(file));
	    String l = "";
	    int counter = 0;
	    StringBuilder sb = new StringBuilder();
	    while((l = br.readLine()) != null){
	        //if(counter < 500){
	            String[] ll = l.split(",");
	            
	            if(ll != null && !ll[1].equals("0.0")){
	                String s = sql.replace("#1", ll[1]).replace("@2", ll[0]);
	                st1.executeUpdate(s);
	                //sb.append(s);
	                //counter ++;
	            }
	        //}
	        /*else{
	            System.out.println(sb.toString());
	            st1.executeUpdate(sb.toString());
	            sb = null;
	            sb = new StringBuilder();
	            counter = 0;
	        }*/
	        
	    }
	    br.close();
	    System.out.println("current round complete!");
	}
	
	public void computingCosin() throws Throwable{
	    connection = new DB().getConn();
	    st1 = connection.createStatement();
	    st2 = connection.createStatement();
	    String in1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-22\\topic-keywords\\terrorist.txt";
	    String in2 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-22\\topic-keywords\\campus.txt";
	    String in3 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-22\\topic-keywords\\bus.txt";
	    String sql1 = "select idnum,all_important from violence limit #1,5000";
	    String sql2 = "select idnum,all_important from campus limit #1,5000";
	    String sql3 = "select idnum,all_important from bus_explosion limit #1,5000";
	    double b;
	    
	    b = init(this.terrorist_keywd,in1);
	    fetchRecords(sql1,1,b);
	    System.out.println("type 1 done!");
	    b = init(this.campus_keyewd,in2);
	    fetchRecords(sql2,2,b);
	    System.out.println("type 2 done!");
	    b = init(this.bus_keywd,in3);
	    fetchRecords(sql3,3,b);
	    System.out.println("type 3 done!");
	}
	
	public double init(Map<String,Double> map,String in) throws Throwable, IOException{
	    BufferedReader br = new BufferedReader(new FileReader(in));
        String l = "";
        double res = 0;
        while((l = br.readLine()) != null){
            String [] ls = l.split(" ");
            if(ls != null){
                double x = Double.parseDouble(ls[1]) * 30;
                map.put(ls[0], x);
                res += Math.pow(x, 2);
            }
        }
        br.close();
        arrCounter = 0;
        this.items = null;
        System.out.println("init success");
        return Math.sqrt(res);
	}
	
	public void fetchRecords(String sql,int type,double b) throws SQLException, IOException{
	    
	    int round = 1;
	    int counter = 0;
	    ResultSet rs = null;
	    
	    int itemNum = 0;
	    if(type == 1){
	        rs = st1.executeQuery("select count(*) from violence");
	        if(rs.next()){
	            itemNum = rs.getInt(1);
	        }
	    }
	    else if(type == 2){
            rs = st1.executeQuery("select count(*) from campus");
            if(rs.next()){
                itemNum = rs.getInt(1);
            }
        }
	    else if(type == 3){
            rs = st1.executeQuery("select count(*) from bus_explosion");
            if(rs.next()){
                itemNum = rs.getInt(1);
            }
        }
	    
	    this.items = new Item[itemNum];  
	    
	    do{
	        String q = sql.replace("#1", String.valueOf((round - 1)*5000));
	        rs = st1.executeQuery(q);
	        
	        counter = cosinUsingWord2vec(rs,type,b);
	        System.out.println("round " + round);
	        round ++;
	    }while(counter == 5000);
	    
	    //排序
	    Arrays.sort(this.items, new IComparator());
	    StringBuilder sb = new StringBuilder();
	    String out = "";
	    if(type == 1){
	        sb.append("由暴恐话题关键词计算得到的新闻cosin值: \n");
	        out = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-22\\baokong-cosin.csv";
	    }
	    else if(type == 2){
	        sb.append("由校园砍伤话题关键词计算得到的新闻cosin值: \n");
	        out = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-22\\xiaoyuan-cosin.csv";
	    }
	    else if(type == 3){
	        sb.append("由公交爆炸关键词计算得到的新闻cosin值: \n");
	        out = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-10-22\\gongjiao-cosin.csv";
	    }
	    
	    sb.append("\n");
	    for(Item i : items){
	        if(i != null)
	            sb.append(i.id + "," + i.cosin + "\n");
	    }
	    
	    BufferedWriter bw = new BufferedWriter(new FileWriter(out));
	    bw.write(sb.toString());
	    bw.close();
	    System.out.println("cosin computing completed of type " + type + " ["  + out + "]");
	}
	
	public int cosinUsingWord2vec(ResultSet rs,int type,double b) throws SQLException, IOException{
	    int counter = 0;
	    if(rs != null){
	        Map<String,Double> worker = null;
	        String sql = "";
	        if(type == 1){
	            worker = this.terrorist_keywd;
	            sql = "update violence set cosin = #1 where idnum = @2";
            }
	        else if(type == 2){
	            worker = this.campus_keyewd;
	            sql = "update campus set cosin = #1 where idnum = @2";
	        }
	        else if(type == 3){
	            worker = this.bus_keywd;
	            sql = "update bus_explosion set cosin = #1 where idnum = @2";
	        }
	        while(rs.next()){
	            counter ++;
	            int id = rs.getInt("idnum");
	            String allImp = rs.getString("all_important");
	            String [] arr = allImp.split("#");
	            if(arr != null){
	                double a = 0;
	                double c = 0;
	                for(String i : arr){
	                    String [] arr2 = i.split("/");
	                    if(arr2 != null){
	                        c += Math.pow(Double.parseDouble(arr2[2]), 2);
	                        if(worker.containsKey(arr2[0])){
	                            a += Double.parseDouble(arr2[2]) * worker.get(arr2[0]);
	                            
	                        }
	                    }
	                }
	                c = Math.sqrt(c);
	                double res = a / (b * c);
	                
	                st2.executeUpdate(sql.replace("#1", String.valueOf(res)).replace("@2", String.valueOf(id)));

	                this.items[arrCounter++] = new Item(id,res);
	            }
	        }
	       
	    }
	    return counter;
	}
	
}
class SortItem{
    String word;
    double val;
    SortItem next;
    SortItem pre;
    public SortItem(String wd,double co,SortItem ne,SortItem pr){
        word = wd;
        val = co;
        next = ne;
        pre = pr;
    }
}

class Item{
    int id;
    double cosin;
    public Item(int i,double c){
        id = i;
        cosin = c;
    }
}
class IComparator implements Comparator<Item>{

    @Override
    public int compare(Item o1, Item o2) {
        if(o1 != null && o2 != null){
            
            double cosina = o1.cosin;
            double cosinb = o2.cosin;
            if(cosina < cosinb)
                return 1;
            if(cosina > cosinb)
                return -1;
            return 0;
        }
        return 0;
        
    }
    
}
