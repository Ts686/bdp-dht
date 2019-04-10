package cn.wonhigh.dc.client.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;


/**
 * TODO: 增加描述
 * 
 * @author wangl
 * @date 2015年3月26日 下午12:19:30
 * @version 0.5.0 
 * @copyright wonhigh.cn 
 */
public class UpdateRelationClums {
	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:mysql://172.17.210.180:3306/dc_scheduler_client",
				"dc_scheduler_cli", "dc_scheduler_cli");
		Statement stmt = conn.createStatement();
		String[] dbs=new String[]{"dc_retail_fas","dc_retail_gms","dc_retail_mdm","dc_retail_mps","dc_retail_pms","dc_retail_pos","dc_bi_mdm"};
		for(int i=0;i<dbs.length;i++){
		ResultSet rs = stmt.executeQuery("select t.* from meta_tables t");
		PreparedStatement pstmt  =  conn.prepareStatement("UPDATE dc_client_task_metastore  SET relation_columns= ? WHERE group_name = ? and trigger_name=? ");
	    while(rs.next()){
	    	if(rs.getInt("p_id")!=0){
	    		System.out.println(rs.getInt("p_id"));
	    		PreparedStatement pstmt1 = conn.prepareStatement("select * from meta_tables where id  = ?");
	    		pstmt1.setInt(1, rs.getInt("p_id"));
	    		ResultSet rs1 = pstmt1.executeQuery();
	    		while(rs1.next()){
	    		System.out.println(rs1.getString("table_name"));
	    		pstmt.setString(1, rs.getString("p_join_columns"));
	    		pstmt.setString(2, dbs[i]);
	    		pstmt.setString(3, rs1.getString("table_name"));
	    		pstmt.execute();
	    		}
	    		rs1.close();
	    		pstmt1.close();
	    		//pstmt.setString(1, rs.getString("p_join_columns"));
	    		//pstmt.setString(2, rs1.getString("table_name"));
	    	}	
	    }
		
		
	}
		stmt.close();
		conn.close();
	}
}
