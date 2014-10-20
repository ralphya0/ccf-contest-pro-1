package me.ralphya0.pre_processing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import me.ralphya0.tools.DB;

public class NewsClassification {

	Connection connection = null;
	Statement st1 = null;
	Statement st2 = null;
	
	
	List<String> terrorist = new ArrayList<String>();
	List<String> campus = new ArrayList<String>();
	List<String> train = new ArrayList<String>();
	
	public void init(String te_path,String ca_path,String tr_path,double te_threshold,
			double ca_threshold,double tr_threshold) throws IOException{
		
		BufferedReader br = new BufferedReader(new FileReader(te_path));
		
		String t ;
		while((t = br.readLine()) != null){
			
		}
		
		br.close();
		
		br = new BufferedReader(new FileReader(ca_path));
		String c ;
		while((c = br.readLine()) != null){
			
		}
		
		br.close();
		br = new BufferedReader(new FileReader(tr_path));
		String tr;
		while((tr = br.readLine()) != null){
			
		}
		
		br.close();
		
	}
	
	public void fetching() throws SQLException{
		connection = new DB().getConn();
		st1 = connection.createStatement();
		st2 = connection.createStatement();
		
		ResultSet rs = null;
		
		int round = 1;
		int count = 0;
		
		do{
			String sql = "select * from t_lable_group_comp limit " + (round - 1)*5000 + ",5000";
			rs = st1.executeQuery(sql);
		}while(count == 5000);
	}
	
	public int replaceAndUpdate(ResultSet rs) throws SQLException{
		int counter = 0;
		if(rs != null){
			while(rs.next()){
				String wordList = rs.getString("all_important");    
				int id = rs.getInt("idnum");
				
				if(wordList.contains("校园")){
					
				}
				
			}
		}
		return counter;
	}
}
