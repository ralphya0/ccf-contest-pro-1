package me.ralphya0.pre_processing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//按话题提取过滤后的新闻表中出现的所有地点信息
public class LocationAnalysis {

    Map<String,List<String>> cache = new HashMap<String,List<String>>();
    
    public LocationAnalysis() throws IOException{
        
    }
    
    public void fun1() throws IOException{
        List<String> l1 = new ArrayList<String> ();
        List<String> l2 = new ArrayList<String> ();
        List<String> l3 = new ArrayList<String> ();
        cache.put("violence", l1);
        cache.put("campus", l2);
        cache.put("bus", l3);
        
        String in = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\Basetable_split_location1.csv";
        BufferedReader br = new BufferedReader(new FileReader(in));
        String l = "";
        while((l = br.readLine()) != null){
            String [] arr = l.split(",");
            if(arr != null){
                int topic = Integer.parseInt(arr[2]);
                String province = arr[6];
                if(province != null && province.trim().length() > 0){
                    if(topic == 1){
                        if(!cache.get("violence").contains(province)){
                            cache.get("violence").add(province);
                        }
                    }
                    else if(topic == 2){
                        if(!cache.get("campus").contains(province)){
                            cache.get("campus").add(province);
                        }
                    }
                    else if(topic == 3){
                        if(!cache.get("bus").contains(province)){
                            cache.get("bus").add(province);
                        }
                    }
                }
            }
        }
        
        br.close();
        System.out.println("init success");
        
        //输出统计结果
        StringBuilder sb1 = new StringBuilder();
        List<String> violence = cache.get("violence");
        sb1.append("暴恐事件地理信息统计:\n");
        sb1.append("\n");
        
        StringBuilder sb2 = new StringBuilder();
        List<String> campus = cache.get("campus");
        sb2.append("校园砍伤事件地理信息统计:\n");
        sb2.append("\n");
        
        StringBuilder sb3 = new StringBuilder();
        List<String> bus = cache.get("bus");
        sb3.append("公交爆炸事件地理信息统计:\n");
        sb3.append("\n");
        
        sb1.append("idnum,");
        for(String s : violence){
            sb1.append(s + ",");
        }
        if(sb1.lastIndexOf(",") == sb1.length() - 1)
            sb1.deleteCharAt(sb1.length() - 1);
        
        sb1.append("\n");
        
        sb2.append("idnum,");
        for(String s : campus){
            sb2.append(s + ",");
        }
        if(sb2.lastIndexOf(",") == sb2.length() - 1)
            sb2.deleteCharAt(sb2.length() - 1);
        
        sb2.append("\n");
        
        sb3.append("idnum,");
        for(String s : bus){
            sb3.append(s + ",");
        }
        if(sb3.lastIndexOf(",") == sb3.length() - 1)
            sb3.deleteCharAt(sb3.length() - 1);
        
        sb3.append("\n");
        
        int violence_loc_num = violence.size();
        int campus_loc_num = campus.size();
        int bus_loc_num = bus.size();
        
        //再次遍历
        BufferedReader br2 = new BufferedReader(new FileReader(in));
        String t = "";
        while((t = br2.readLine()) != null){
            String [] arr = t.split(",");
            if(arr != null){
                int topic = Integer.parseInt(arr[2]);
                String province = arr[6];
                String id = arr[0];
                if(province != null && province.trim().length() > 0){
                    if(topic == 1){
                        sb1.append(id + ",");
                        int index = violence.indexOf(province);
                        if(index != -1){
                            for(int i = 0;i < violence_loc_num;i ++){
                                if(i != index)
                                    sb1.append("F,");
                                else 
                                    sb1.append("T,");
                                
                            }
                            if(sb1.lastIndexOf(",") == sb1.length() - 1)
                                sb1.deleteCharAt(sb1.length() - 1);
                            sb1.append("\n");
                        }
                    }
                    else if(topic == 2){
                        sb2.append(id + ",");
                        int index = campus.indexOf(province);
                        if(index != -1){
                            for(int i = 0;i < campus_loc_num;i ++){
                                if(i != index)
                                    sb2.append("F,");
                                else 
                                    sb2.append("T,");
                                
                            }
                            if(sb2.lastIndexOf(",") == sb2.length() - 1)
                                sb2.deleteCharAt(sb2.length() - 1);
                            sb2.append("\n");
                        }
                    }
                    else if(topic == 3){
                        sb3.append(id + ",");
                        int index = bus.indexOf(province);
                        if(index != -1){
                            for(int i = 0;i < bus_loc_num;i ++){
                                if(i != index)
                                    sb3.append("F,");
                                else 
                                    sb3.append("T,");
                                
                            }
                            if(sb3.lastIndexOf(",") == sb3.length() - 1)
                                sb3.deleteCharAt(sb3.length() - 1);
                            sb3.append("\n");
                        }
                    }
                }
                else{
                    if(topic == 1){
                        sb1.append(id + ",");
                        for(int i = 0;i < violence_loc_num;i ++){
                                sb1.append("F,");
                        }
                        if(sb1.lastIndexOf(",") == sb1.length() - 1)
                            sb1.deleteCharAt(sb1.length() - 1);
                        sb1.append("\n");
                    }
                    else if(topic == 2){
                        sb2.append(id + ",");
                        for(int i = 0;i < campus_loc_num;i ++){
                            sb2.append("F,");
                        }
                        if(sb2.lastIndexOf(",") == sb2.length() - 1)
                            sb2.deleteCharAt(sb2.length() - 1);
                        sb2.append("\n");
                    }
                    else if(topic == 3){
                        sb3.append(id + ",");
                        for(int i = 0;i < bus_loc_num;i ++){
                            sb3.append("F,");
                        }
                        if(sb3.lastIndexOf(",") == sb3.length() - 1)
                            sb3.deleteCharAt(sb3.length() - 1);
                        sb3.append("\n");
                    }
                }
            }
        }
        
        br2.close();
        
        //输出
        String out1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\violence_province.csv";
        String out2 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\campus_province.csv";
        String out3 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\bus_province.csv";
        BufferedWriter bw1 = new BufferedWriter(new FileWriter(out1));
        bw1.write(sb1.toString());
        bw1.close();
        
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(out2));
        bw2.write(sb2.toString());
        bw2.close();
        
        BufferedWriter bw3 = new BufferedWriter(new FileWriter(out3));
        bw3.write(sb3.toString());
        bw3.close();
        
        System.out.println("done");
    }

    
    //替换原表中的province字段
    public void fun2() throws IOException{
        String in1 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\violence_province.csv";
        String in2 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\campus_province.csv";
        String in3 = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\bus_province.csv";
        
        //读入缓存信息
        BufferedReader br1 = new BufferedReader(new FileReader(in1));
        
        //三个事件中出现过的所有地址信息
        List<String> location_union = new ArrayList<String>();
        
        String s1 = "";
        String s2 = "";
        String s3 = "";
        
        //当前话题的地址列表
        s1 = br1.readLine();
        String [] tmp = s1.split(",");
        if(tmp != null){
            //跳过idnum字段
            for(int i = 1;i < tmp.length;i ++){
                if(!location_union.contains(tmp[i]))
                    location_union.add(tmp[i]);
            }
        }
        
        br1.close();
        
        BufferedReader br2 = new BufferedReader(new FileReader(in2));
        s2 = br2.readLine();
        String [] tmp2 = s2.split(",");
        if(tmp2 != null){
            //跳过idnum字段
            for(int i = 1;i < tmp2.length;i ++){
                if(!location_union.contains(tmp2[i]))
                    location_union.add(tmp2[i]);
            }
        }
        br2.close();
        
        BufferedReader br3 = new BufferedReader(new FileReader(in3));
        s3 = br3.readLine();
        String [] tmp3 = s3.split(",");
        if(tmp3 != null){
            //跳过idnum字段
            for(int i = 1;i < tmp3.length;i ++){
                if(!location_union.contains(tmp3[i]))
                    location_union.add(tmp3[i]);
            }
        }
        br3.close();
        
        System.out.println("init success...");
        
        
        //更新原始数据
        String in = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\Basetable_split_location1.csv";
        StringBuilder sb = new StringBuilder();
        sb.append("展开之后的省份列表（有序）：\n");
        for(String s : location_union){
            sb.append(s + ",");
        }
        if(sb.lastIndexOf(",") == sb.length() - 1)
            sb.deleteCharAt(sb.length() - 1);
        sb.append("\n");
        
        BufferedReader br = new BufferedReader(new FileReader(in));
        String l = "";
        while((l = br.readLine()) != null){
            String [] arr = l.split(",");
            if(arr != null){
                //前6个字段不变
                String field0 = arr[0];
                String field1 = arr[1];
                String field2 = arr[2];
                String field3 = arr[3];
                String field4 = arr[4];
                String field5 = arr[5];
                
                String province = arr[6];
                
                String field6 = arr[7];
                String field7 = arr[8];
                String field8 = arr[9];
                String field9 = arr[10];
                String field10 = arr[11];
                String field11 = arr[12];
                String field12 = arr[13];
                String field13 = arr[14];
                String field14 = arr[15];
                String field15 = arr[16];
                
                StringBuilder location_str = new StringBuilder();
                if(province != null && province.trim().length() > 0){
                    int index = location_union.indexOf(province);
                    if(index != -1){
                        for(int k = 0;k < location_union.size(); k ++){
                            if(k != index)
                                location_str.append("F,");
                            else
                                location_str.append("T,");
                        }

                    }
                }
                else{
                    for(int k = 0;k < location_union.size(); k ++){
                        location_str.append("F,");
                        
                    }
                }
                
                
                sb.append(field0 + "," + field1 + "," + field2 + "," + field3 + "," + field4 + "," + field5 + "," + 
                        location_str.toString() + field6 + "," + field7 + "," + field8 + "," + field9 + "," + field10 + "," + field11 + "," +
                        field12 + "," + field13 + "," + field14 + "," + field15 + "\n");
                
            }
        }
        br.close();
        
        String out = "F:\\work-space\\project-base\\ccf\\data\\公共安全事件\\result\\2014-11-14\\Basetable_split_location1_new.csv";
        BufferedWriter bw = new BufferedWriter(new FileWriter(out));
        bw.write(sb.toString());
        bw.close();
        System.out.println("done");
        
        
    }
    
    public static void main(String[] args) throws IOException {
        new LocationAnalysis().fun2();
        
    }
}
