package cn.wonhigh.dc.client.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection;
import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.HiveDefinePartNameEnum;
import cn.wonhigh.dc.client.common.model.TaskPropertiesConfig;

import com.yougou.logistics.base.common.exception.ManagerException;

public class CreatePreImportXmlUtils {
	
	/**
	 * 重命名追加当前时间
	 */
	public static String appendName=null;
	
	/**
	 * 源xml模板文件--src
	 */
	public static InputStream order_main_src_xml=null;
	
	/**
	 * 源xml模板文件--ck(同步数据质量核查)
	 */
	public static InputStream order_main_ck_xml=null;	
	/**
	 * 源xml模板文件--thl
	 */
	public static InputStream order_main_thl_xml=null;
	/**
	 * 源xml模板文件--clnd
	 */
	public static InputStream order_main_clnd_xml=null;
	/**
	 * 源xml模板文件--all
	 */
	public static InputStream order_main_all_xml=null;
	/**
	 * 源xml模板文件--rt
	 */
	public static InputStream order_main_rt_xml=null;
	/**
	 * 源xml模板文件--sp
	 */
	public static InputStream order_main_sp_xml=null;	
	
	/**
	 * @param 
	 * 本类用于预生成不完整的Xml,请将已有所有xml配置文件放置
	 * 到sqoop配置目录，便于确认现有id值，防止id重复
	 */


	private static final Logger logger = Logger.getLogger(CreatePreImportXmlUtils.class);
	
	/**
	 * -i参数，才加载模板
	 */
	private static void loadModelXml(){
		 //源xml模板文件--src
		order_main_src_xml=CreatePreImportXmlUtils.class.getResourceAsStream("/model-config/retail_pos-order_main_src.xml");
		 //源xml模板文件--thl
		order_main_thl_xml=CreatePreImportXmlUtils.class.getResourceAsStream("/model-config/retail_pos-order_main_thl.xml");
		 //源xml模板文件--clnd
		order_main_clnd_xml=CreatePreImportXmlUtils.class.getResourceAsStream("/model-config/retail_pos-order_main_clnd.xml");
		 //源xml模板文件--all
		order_main_all_xml=CreatePreImportXmlUtils.class.getResourceAsStream("/model-config/retail_pos-order_main_all.xml");
		 //源xml模板文件--rt
		order_main_rt_xml=CreatePreImportXmlUtils.class.getResourceAsStream("/model-config/retail_pos-order_main_rt.xml");
		 //源xml模板文件--sp
		order_main_sp_xml=CreatePreImportXmlUtils.class.getResourceAsStream("/model-config/retail_pos-order_main_sp.xml");
		//源xml模板文件--ck
		order_main_ck_xml=CreatePreImportXmlUtils.class.getResourceAsStream("/model-config/retail_pos-order_main_ck.xml");
		
	}

	/**
	 * 模板xml所在路径
	 * @param newOrUpdateTablePath  配置文件，关于新增或修改表所路径
	 * @param sourceDbId  使用数据源编码
	 * @param latestXmlDirPath 导入xml模板的所在目录
	 * @param serialNum 起始编号值
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public static Map<String,String> createPreXml(String newOrUpdateTablePath,String sourceDbId,String latestXmlDirPath,Integer serialNum) throws SQLException, IOException {
		logger.info("==========>开始生成新表脚本");
		
		//缓存临时生成xml的内容
		Map<String,String> storageContent=new HashMap<String,String>();
		//********************初始化**************************//
		storageContent.put("sRetaiExport0", "");// 基础表导出, history_log 只有导入，导出，没有去重
		storageContent.put("sSportExport0", "");//体育
		storageContent.put("sSrc0", "");// 基础表导入（sSrc和sCDCImport都存储在这里）
		storageContent.put("sThl0", "");// 基础表cdc分发
		storageContent.put("sOds0", "");// 基础表去重
		storageContent.put("sAllExport0", "");// 基础表导出
		
		storageContent.put("error0", "");//输入的起始编号编号，不合法
		storageContent.put("importXmlNameArray", "");//存储修改或者新增表的内容
		storageContent.put("syncTime0", "");//标示时间列名（用于统计半年的数据，统计每天的数据量）
		
		storageContent.put("countNum", "");//总共生成几个表
		storageContent.put("is_i", null);//是否有新增表
		
		
		
		// begin
		ConcurrentMap<String, TaskPropertiesConfig> taskmap = ParseXMLFileUtil.getTaskList();
		//gms:6 pos: 7 retail_mdm:8 mps:9 fas:10 mdm:11
		List<String> importXmlNameList = new ArrayList<String>();
		
		String newOrUpdateContent=readFile(newOrUpdateTablePath);
		String[] importXmlNameArray = newOrUpdateContent.split(System.getProperty("line.separator"));
		
		if(importXmlNameArray[0].split("-").length != 5){
			storageContent.put("error", "配置文件格式不正确，关于新增或者修改表内容；【"+newOrUpdateTablePath+"】");//输入的起始编号编号，不合法
			return storageContent;
		}
		
		for(int i=0;i<importXmlNameArray.length;i++){
			if(StringUtils.isNotBlank(importXmlNameArray[i])){
				importXmlNameList.add(importXmlNameArray[i].trim());
			}
		}
		int importXmlNum = importXmlNameList.size();
		List<Integer> ids = new ArrayList<Integer>();
		for (Entry<String, TaskPropertiesConfig> task : taskmap.entrySet()) {
			ids.add(task.getValue().getId());
		}
		
		//生成配置的导入调度名数量*3的不重复可用id
		List<Integer> newIds = new ArrayList<Integer>();

		String szGroupName = null;
		String szSchedulerName = null;
		if(importXmlNameList.size()>0){
			szGroupName = importXmlNameList.get(0);
			System.out.println(szGroupName);
		}

		int xmlCount = 4;
		 if(getOptType(szGroupName) == 1){ // bi_mdm_all
			xmlCount = 4;
		}else if(getOptType(szGroupName) == 2){ // retail_mdm
			xmlCount = 4;
		}else if(getOptType(szGroupName) == 3){ // 零售和体育
			xmlCount = 5;
		}
		 
		//_ck.xml新增xml，只对-i 增量0      	（0增量  1全量）
		xmlCount+=1;
		//_ly_src.xml新增xml  只对分组gly_wms_city生成
		xmlCount+=1;
		 
		int minNum=55770;
		//如果没有对比的目标文件，那么编码值，从你设定的值开始
		if(serialNum!=null&&serialNum!=-1){
			minNum=serialNum;
		}

		// 新版本的 id 从55000- 55000 为traansface_history_log开始 55000 以后为正常的
		for (int i = minNum; i < Integer.MAX_VALUE && newIds.size() < xmlCount * importXmlNum; i++) {
			if (!ids.contains(i)) {
				storageContent.put("serialNum", i+"");//起始编号值
				newIds.add(i);
			}
		}
		
		//判断，输入起始值，是否有效
		if(ids.contains(serialNum)){
			storageContent.put("error", "您输入的起始值无效，建议用有效值【"+newIds.get(0)+"】");//输入的起始编号编号，不合法
			return storageContent;
		}else if(serialNum!=null&&serialNum!=-1&&!newIds.contains(serialNum)){
			newIds.remove(newIds.size()-1);
			newIds.add(serialNum);
		}
		
		//存储id:_ly_src.xml新增xml  只对分组gly_wms_city生成
		int size=newIds.size()-importXmlNum-1;
		for(int i=0;i<=importXmlNum;i++){
			storageContent.put("ly_src_id_"+i, newIds.get(size+i)+"");//起始编号值
		}

		/*以下获取一个模板生成导入、去重、导出xml
		 * 
		 */

		//1、获得导入模板配置文件信息,以dc_retail_mdm的shop为模板,读取sqoop文件夹中对应模板配置文件
		
		loadModelXml();
		String sSrc = readFileForStream(order_main_src_xml);
		String sThl = readFileForStream(order_main_thl_xml);
		String sOds = readFileForStream(order_main_clnd_xml);
		String sAllExport = readFileForStream(order_main_all_xml);
		String sRetaiExport = readFileForStream(order_main_rt_xml);
		String sSportExport = readFileForStream(order_main_sp_xml);
		String sck=readFileForStream(order_main_ck_xml);

		String sCDCImport = "";
		String sCDCCln = "";
		String sCDCExport = "";
		int j=0;
		int type = 0;
		System.out.println("importXmlNameList.size():"+importXmlNameList.size());
		storageContent.put("countNum",importXmlNameList.size()+"");
		for (int i=0;i<importXmlNameList.size();i++) {
			//[0]-组名  
			//[1]-表名
			//[2]-update_Time 
			//[3]-增量或全量 
			//[4]-(i:增量和全量；u:新增字段)
			String[] nameList = importXmlNameList.get(i).split("-");

			String  groupName = nameList[0];
			String schedulerName = nameList[1];
			String syncTime ="update_Time";
			String defaultThl="capture_time";//"capture_time"; he.c提出"
			String isOverWrite ="0";
			
			//固定化格式
//			if(nameList.length == 3){
//				syncTime = nameList[2];
//			}
//			if(nameList.length == 4){
//				syncTime = nameList[2];
//				isOverWrite = nameList[3];
//			}
			if(nameList.length == 5){
				syncTime = nameList[2];
				isOverWrite = nameList[3];
				//u:修改表  i:新增表的增量或者全量
				if("u".equalsIgnoreCase(nameList[4])){
					
					//读取目标源五个文件是否存在，不存在不让下一步操作
					//例如：
					//retail_pos-order_payway_clnd.xml
					//retail_pos-order_payway_rt.xml
					//retail_pos-order_payway_sp.xml
					//retail_pos-order_payway_src.xml
					//retail_pos-order_payway_thl.xml
					File clnd=new File(latestXmlDirPath+groupName+"-"+schedulerName+"_clnd.xml");
					File rt=new File(latestXmlDirPath+groupName+"-"+schedulerName+"_rt.xml");
					File sp=new File(latestXmlDirPath+groupName+"-"+schedulerName+"_sp.xml");
					File src=new File(latestXmlDirPath+groupName+"-"+schedulerName+"_src.xml");
					File all=new File(latestXmlDirPath+groupName+"-"+schedulerName+"_all.xml");
					if(!(clnd.exists()&&(rt.exists()||sp.exists()||all.exists())&&src.exists())){
						storageContent.put("error", "_clnd.xml，_rt.xml，_sp.xml，_src.xml目标文件不存在！！");//输入的起始编号编号，不合法
						return storageContent;
					}else{
						String[] searchIds={"selectColumns"};
						try {
							storageContent.put("importXmlNameArray"+"-"+groupName.toLowerCase()+"-"+schedulerName.toLowerCase(), ParseXMLFileUtil.parseTaskXMLReadId(src,searchIds));
						} catch (ManagerException e) {
							e.printStackTrace();
						}
					}
					
					isOverWrite = "1";//全量处理
					
				}else{
					storageContent.put("is_i","Y");//记录有新增表
				}
			}			
			storageContent.put("syncTime"+"-"+groupName+"-"+schedulerName, syncTime);
			System.out.println("groupName:"+groupName+"  schedulerName:"+schedulerName);
			String idImport=newIds.get(j).toString();
			j+=1;
			String idCk=newIds.get(j).toString();//_ck.xml
			String idThl = null;
			String idOds = null;
			j+=1;
			// 将History_log 表的去重排除
			if ( !schedulerName.equals(MessageConstant.TRANSCATION_HISTORY_LOG)   ) {
				if(!schedulerName.contains( MessageConstant.VERIFY)) {
					idThl = newIds.get(j).toString();
					j += 1;
				}
				idOds = newIds.get(j).toString();
				j += 1;
			}
			String idExport=newIds.get(j).toString();
			j+=1;
			String id3Export = "";
			if(getOptType(szGroupName) == 1){ // bi_mdm_all
				type = 1;
			}else if(getOptType(szGroupName) == 2){ // retail_mdm
				type = 2;
			}else if(getOptType(szGroupName) == 3){ // 零售和体育
				type = 3;
				id3Export = newIds.get(j).toString();
				j+=1;
			}

			//替换导入xml配置信息



			// 将History_log 表的去重排除
			if ( schedulerName.equals(MessageConstant.TRANSCATION_HISTORY_LOG)  ) {

				sSrc = covertSrc(sSrc, groupName,  schedulerName, sourceDbId,"",idImport,defaultThl,"0");
				sCDCImport = sSrc;
				//替换导出xml配置信息, history_log 只有导入，导出，没有去重，因此导出依赖于导入的id
				//sRetaiExport = covertExport(sRetaiExport, groupName, schedulerName,idExport,idImport,"");
				sRetaiExport = covertExport(sRetaiExport, groupName, schedulerName, idExport, idImport,MessageConstant.TRANSCATION_HISTORY_LOG, "_rt",defaultThl,"0");
				sSportExport = covertExport(sSportExport, groupName, schedulerName, id3Export, idImport ,MessageConstant.TRANSCATION_HISTORY_LOG,  "_sp",defaultThl,"0");

			}else{
				sSrc = covertSrc(sSrc, groupName, schedulerName, sourceDbId,"",idImport,syncTime,isOverWrite);
				sCDCImport = sSrc;
				if(!schedulerName.contains( MessageConstant.VERIFY)){
					//替换　cdc 的xml配置信息
					sThl = covertThl(sThl, groupName, schedulerName,idThl,idImport,defaultThl, isOverWrite);

					//替换　ods 的xml配置信息
					sOds = covertOds(sOds, groupName, schedulerName,idOds,idThl,syncTime, isOverWrite);
				}else {
					//如果只含 _verify 类型的表则 没有Thl 类型的xml 文件，只有 ods类型的文件
					sOds = covertOds(sOds, groupName, schedulerName,idOds,idImport,syncTime, isOverWrite);
				}


				//替换导出xml配置信息
				if(1 == type) { // all
					sAllExport = covertExport(sAllExport, groupName, schedulerName, idExport, idOds,"", "_all",syncTime, isOverWrite);
				}else if(2 == type){ // retail
					sRetaiExport = covertExport(sRetaiExport, groupName, schedulerName, idExport, idOds, "","_rt",syncTime, isOverWrite);
					sCDCExport = sRetaiExport;
				}else if(3 == type){ // sport and retail
					sRetaiExport = covertExport(sRetaiExport, groupName, schedulerName, idExport, idOds, "","_rt",syncTime, isOverWrite);
					sSportExport = covertExport(sSportExport, groupName, schedulerName, id3Export, idOds,"",  "_sp",syncTime, isOverWrite);
				}
			}

			//针对全量生成
			if("0".equals(isOverWrite)){
				//idImport不知道如何获得
				String ckContent=covertCK(sck, groupName, schedulerName, sourceDbId, idCk, syncTime, isOverWrite);
				storageContent.put("sCk"+i, ckContent);
			}

			if ( schedulerName.equals(MessageConstant.TRANSCATION_HISTORY_LOG) ) {
				// 基础表导入
				storageContent.put("sSrc"+i, sCDCImport);

				if (1 == getOptType(groupName.toLowerCase())) {
					// 基础表导出, history_log 只有导入，导出，没有去重
					storageContent.put("sRetaiExport"+i, sRetaiExport);
					
				}else if (2 == getOptType(groupName.toLowerCase())) { // EXPORT_RETAIL_TABLE_NAME_SUBFIX 只有零售
					// 基础表导出, history_log 只有导入，导出，没有去重
					storageContent.put("sRetaiExport"+i, sRetaiExport);
					
				}else if ( 3 == getOptType(groupName.toLowerCase())) { // 零售 和 体育
					storageContent.put("sRetaiExport"+i, sRetaiExport);
					storageContent.put("sSportExport"+i, sSportExport);
					
				}

			}else{
				// 基础表导入
				storageContent.put("sSrc"+i, sSrc);
				
				// 基础表cdc分发
				if(!schedulerName.contains(MessageConstant.VERIFY)) {
					storageContent.put("sThl"+i, sThl);
				}

				// 基础表去重
				storageContent.put("sOds"+i, sOds);
				
				// 基础表导出
				if (1 == getOptType(groupName.toLowerCase())) {
					storageContent.put("sAllExport"+i, sAllExport);
					
				}else if (2 == getOptType(groupName.toLowerCase())) { // EXPORT_RETAIL_TABLE_NAME_SUBFIX 只有零售
					storageContent.put("sRetaiExport"+i, sRetaiExport);
					
				}else if ( 3 == getOptType(groupName.toLowerCase())) { // 零售 和 体育
					storageContent.put("sRetaiExport"+i, sRetaiExport);
					storageContent.put("sSportExport"+i, sSportExport);
					
				}
			}
			
			groupName=groupName.toUpperCase();
			schedulerName=schedulerName.toUpperCase();
			
			//用于存储又组名和表名，映射到关联excel里面的内容
			storageContent.put("sSrc"+"-"+groupName+"-"+schedulerName,"sSrc"+i);// 基础表导入（sSrc和sCDCImport都存储在这里）
			storageContent.put("sThl"+"-"+groupName+"-"+schedulerName,"sThl"+i);// 基础表cdc分发
			storageContent.put("sOds"+"-"+groupName+"-"+schedulerName,"sOds"+i);// 基础表去重
			storageContent.put("sAllExport"+"-"+groupName+"-"+schedulerName,"sAllExport"+i);// 基础表导出
			storageContent.put("sSportExport"+"-"+groupName+"-"+schedulerName,"sSportExport"+i);//体育
			storageContent.put("sRetaiExport"+"-"+groupName+"-"+schedulerName,"sRetaiExport"+i);	
			storageContent.put("sCk"+"-"+groupName+"-"+schedulerName,"sCk"+i);// 同步数据质量核查
		}
		logger.info("==========>结束生成新表脚本");
		return storageContent;
	}

	/**
	 * 获取组名的类型
	 * return 1 是 bi_mdm
	 * return 2 是 只有零售
	 * return 3 是 零售和体育

	 * @author zhang.sl
	 *
	 */
	public static int getOptType(String groupName) {
		int groupType = 0;
		if( groupName.contains("miu") || groupName.contains("bl_f1")){ // 集团主数据
			groupType = 1;
		} else if(groupName.contains("retail_mdm")){
			groupType = 2;
		}else{
			groupType = 3;
		}
		return  groupType;
	}

	// 导入
	private static String covertSrc(String sImport, String groupName, String schedulerName, String sourceDbId,String selectColumnsStr,String id,String syncTime,String isOverWrite) {
		System.out.println(  "groupName:"+groupName +" schedulerName:"+ schedulerName+" syncTime:"+syncTime+ " isOverWrite:"+isOverWrite)	;
		//替换注释
		sImport = sImport.replaceAll("<!--This is a config of ﻿[\\s|\\S]*-->", "<!--This is a config of ﻿"+ schedulerName.toLowerCase()+  "_src" + "-->");
		
		sImport = sImport.replaceAll("<id>[\\s|\\S]*</id>", "<id>" + id + "</id>");
		sImport = sImport.replaceAll("<syncTimeColumn>[\\s|\\S]*</syncTimeColumn>", "<syncTimeColumn>" + syncTime + "</syncTimeColumn>");
		sImport = sImport.replaceAll("<isOverwrite>[\\s|\\S]*</isOverwrite>", "<isOverwrite>" + isOverWrite + "</isOverwrite>");
		sImport = sImport.replaceAll("<id>[\\s|\\S]*</id>", "<id>" + id + "</id>");
		sImport = sImport.replaceAll("<groupName>[\\s|\\S]*</groupName>", "<groupName>" + groupName.toLowerCase() + "</groupName>");
		sImport = sImport.replaceAll("<triggerName>[\\s|\\S]*</triggerName>", "<triggerName>" + schedulerName.toLowerCase()
				+  "_src" + "</triggerName>");
		sImport = sImport.replaceAll("<sourceDbId>[\\s|\\S]*</sourceDbId>", "<sourceDbId>" + sourceDbId
				+ "</sourceDbId>");

		if(schedulerName.contains(MessageConstant.TRANSCATION_HISTORY_LOG)){
			sImport = sImport.replaceAll("<sourceTable>[\\s|\\S]*</sourceTable>", "<sourceTable>" +groupName+"_"+ schedulerName
					+ "</sourceTable>");
		}else {
			sImport = sImport.replaceAll("<sourceTable>[\\s|\\S]*</sourceTable>", "<sourceTable>" + (true == schedulerName.contains("_verify") ? schedulerName.replace("_verify", "") : schedulerName)
					+ "</sourceTable>");
		}

		sImport = sImport.replaceAll("<targetTable>[\\s|\\S]*</targetTable>",
				"<targetTable>" + groupName.toLowerCase()+ "_"+schedulerName.toLowerCase()+CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue() + "</targetTable>");

		sImport = sImport.replaceAll("<selectColumns>[\\s|\\S]*</selectColumns>", "<selectColumns>" + selectColumnsStr
				+ "now() as "+ MessageConstant.SRC_UPDATE_TIME + "</selectColumns>");

		sImport=sImport.replace("dc_retail_pos.order_main", groupName
				+ "_"+schedulerName);
		return sImport;
	}
	// thl 表
	private static String covertThl(String sThl, String groupName, String schedulerName, String idOds, String idThl,String syncTime,String isOverWrite) {
		//替换注释
		sThl = sThl.replaceAll("<!--[\\s|\\S]*-->", "<!--This is a config of ﻿"+ schedulerName.toLowerCase()+ CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()+"-->");
		
		sThl = sThl.replaceAll("<id>[\\s|\\S]*</id>", "<id>" + idOds + "</id>");
		sThl = sThl.replaceAll("<dependencyTaskIds>[\\s|\\S]*</dependencyTaskIds>", "<dependencyTaskIds>" + idThl + "</dependencyTaskIds>");
		sThl = sThl.replaceAll("<groupName>[\\s|\\S]*</groupName>", "<groupName>" + groupName.toLowerCase() + "</groupName>");
		sThl = sThl.replaceAll("<triggerName>[\\s|\\S]*</triggerName>", "<triggerName>" + schedulerName.toLowerCase()+ CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()
				+ "</triggerName>");
		sThl = sThl.replaceAll("<sourceTable>[\\s|\\S]*</sourceTable>", "<sourceTable>" + groupName.toLowerCase()
				+ "_"+MessageConstant.TRANSCATION_HISTORY_LOG + CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue() + "</sourceTable>");
		sThl = sThl.replaceAll("<targetTable>[\\s|\\S]*</targetTable>", "<targetTable>" + groupName.toLowerCase()
				+ "_"+schedulerName  +  CommonEnumCollection.HiveDefinePartNameEnum.THL_TABLE_NAME_SUBFIX.getValue()+ "</targetTable>");
		sThl = sThl.replaceAll("<syncTimeColumn>[\\s|\\S]*</syncTimeColumn>", "<syncTimeColumn>" + "capture_time "
				+ "</syncTimeColumn>");
		sThl = sThl.replace("dc_retail_pos.order_main", groupName
				+ "_"+schedulerName );
		sThl = sThl.replaceAll("<syncTimeColumn>[\\s|\\S]*</syncTimeColumn>", "<syncTimeColumn>" + syncTime + "</syncTimeColumn>");
		sThl = sThl.replaceAll("<isOverwrite>[\\s|\\S]*</isOverwrite>", "<isOverwrite>" + isOverWrite + "</isOverwrite>");
		return sThl;
	}

	// ods 表
	private static String covertOds(String sOds, String groupName, String schedulerName, String idOds, String idThl,String syncTime,String isOverWrite) {
		
		//替换注释
		sOds = sOds.replaceAll("<!--[\\s|\\S]*-->", "<!--This is a config of ﻿"+
				schedulerName.toLowerCase()+CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue()
				+"-->");
		
		sOds = sOds.replaceAll("<id>[\\s|\\S]*</id>", "<id>" + idOds + "</id>");
		sOds = sOds.replaceAll("<dependencyTaskIds>[\\s|\\S]*</dependencyTaskIds>", "<dependencyTaskIds>" + idThl + "</dependencyTaskIds>");
		sOds = sOds.replaceAll("<groupName>[\\s|\\S]*</groupName>", "<groupName>" + groupName.toLowerCase() + "</groupName>");
		sOds = sOds.replaceAll("<triggerName>[\\s|\\S]*</triggerName>", "<triggerName>" +
				schedulerName.toLowerCase()+CommonEnumCollection.HiveDefinePartNameEnum.CLEANED_TABLE_NAME_SUBFIX.getValue()
				+ "</triggerName>");
		sOds = sOds.replaceAll("<sourceTable>[\\s|\\S]*</sourceTable>", "<sourceTable>" + groupName.toLowerCase()
				+ "_"+schedulerName + CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue() + "</sourceTable>");
		sOds = sOds.replaceAll("<targetTable>[\\s|\\S]*</targetTable>", "<targetTable>" + groupName.toLowerCase()
				+ "_"+schedulerName + "_ods" + "</targetTable>");
		sOds = sOds.replaceAll("<syncTimeColumn>[\\s|\\S]*</syncTimeColumn>", "<syncTimeColumn>" + MessageConstant.SYNCTIMECOLUMN
				+ "</syncTimeColumn>");
		sOds = sOds.replaceAll("<syncTimeColumn>[\\s|\\S]*</syncTimeColumn>", "<syncTimeColumn>" + syncTime + "</syncTimeColumn>");
		sOds = sOds.replaceAll("<isOverwrite>[\\s|\\S]*</isOverwrite>", "<isOverwrite>" + isOverWrite + "</isOverwrite>");
		sOds=sOds.replace("dc_retail_pos.order_main", groupName
				+ "_"+schedulerName );
		return sOds;
	}

	// 导出表
	private static String covertExport(String sExport, String groupName, String schedulerName, String idExport, String idCln,String selectColumnsStr,String subSchedulerNameFix,String syncTime,String isOverWrite) {
		//替换注释
		sExport = sExport.replaceAll("<!--[\\s|\\S]*-->", "<!--This is a config of ﻿"+ schedulerName.toLowerCase() + subSchedulerNameFix +"-->");		
		
		sExport = sExport.replaceAll("<id>[\\s|\\S]*</id>", "<id>" + idExport + "</id>");
		sExport = sExport.replaceAll("<dependencyTaskIds>[\\s|\\S]*</dependencyTaskIds>", "<dependencyTaskIds>" + idCln + "</dependencyTaskIds>");
		sExport = sExport.replaceAll("<groupName>[\\s|\\S]*</groupName>", "<groupName>" + groupName.toLowerCase()
				+ "</groupName>");
		sExport = sExport.replaceAll("<triggerName>[\\s|\\S]*</triggerName>", "<triggerName>" + schedulerName.toLowerCase() + subSchedulerNameFix
				+ "</triggerName>");
		if (schedulerName.contains(MessageConstant.TRANSCATION_HISTORY_LOG)) {
			sExport = sExport.replaceAll("<sourceTable>[\\s|\\S]*</sourceTable>",
					"<sourceTable>" + groupName.toLowerCase() + "_" + schedulerName.toLowerCase() + CommonEnumCollection.HiveDefinePartNameEnum.SRC_TABLE_NAME_SUBFIX.getValue() + "</sourceTable>");
		}else {
			sExport = sExport.replaceAll("<sourceTable>[\\s|\\S]*</sourceTable>",
					"<sourceTable>" + groupName.toLowerCase() + "_" + schedulerName.toLowerCase() +CommonEnumCollection.HiveDefinePartNameEnum.ODS_TABLE_NAME_SUBFIX.getValue()  + "</sourceTable>");
		}
		sExport = sExport.replaceAll("<targetTable>[\\s|\\S]*</targetTable>",
				"<targetTable>" + groupName.toLowerCase() + "_"+schedulerName.toLowerCase() + "</targetTable>");
		sExport = sExport.replaceAll("<syncTimeColumn>[\\s|\\S]*</syncTimeColumn>", "<syncTimeColumn>"+ MessageConstant.SYNCTIMECOLUMN
				+ "</syncTimeColumn>");
		sExport = sExport.replaceAll("<selectColumns>[\\s|\\S]*</selectColumns>", "<selectColumns>" + selectColumnsStr
				+ MessageConstant.HIVE_UPDATE_TIME + " as "+ MessageConstant.SRC_UPDATE_TIME + "</selectColumns>");
		sExport = sExport.replaceAll("<syncTimeColumn>[\\s|\\S]*</syncTimeColumn>", "<syncTimeColumn>" + syncTime + "</syncTimeColumn>");
		sExport = sExport.replaceAll("<isOverwrite>[\\s|\\S]*</isOverwrite>", "<isOverwrite>" + isOverWrite + "</isOverwrite>");
		sExport=sExport.replace("dc_retail_pos.order_main", groupName
				+ "_"+schedulerName.toLowerCase() );
		return sExport;
	}
	
	// ck-----(同步数据质量核查)
	private static String covertCK(String sImport, String groupName, String schedulerName, String sourceDbId,String id,String syncTime,String isOverWrite) {
		System.out.println(  "groupName:"+groupName +" schedulerName:"+ schedulerName+" syncTime:"+syncTime+ " isOverWrite:"+isOverWrite)	;
		//替换注释
		sImport = sImport.replaceAll("<!--This is a config of ﻿[\\s|\\S]*-->", "<!--This is a config of ﻿"+ schedulerName.toLowerCase()+ HiveDefinePartNameEnum.CK_TABLE_NAME_SUBFIX.getValue() + "-->");
		
		sImport = sImport.replaceAll("<id>[\\s|\\S]*</id>", "<id>" + id + "</id>");
		sImport = sImport.replaceAll("<syncTimeColumn>[\\s|\\S]*</syncTimeColumn>", "<syncTimeColumn>" + syncTime + "</syncTimeColumn>");
		sImport = sImport.replaceAll("<isOverwrite>[\\s|\\S]*</isOverwrite>", "<isOverwrite>" + isOverWrite + "</isOverwrite>");
		sImport = sImport.replaceAll("<id>[\\s|\\S]*</id>", "<id>" + id + "</id>");
		sImport = sImport.replaceAll("<groupName>[\\s|\\S]*</groupName>", "<groupName>" + groupName.toLowerCase() + "</groupName>");
		sImport = sImport.replaceAll("<triggerName>[\\s|\\S]*</triggerName>", "<triggerName>" + schedulerName.toLowerCase()
				+  HiveDefinePartNameEnum.CK_TABLE_NAME_SUBFIX.getValue() + "</triggerName>");
		sImport = sImport.replaceAll("<sourceDbId>[\\s|\\S]*</sourceDbId>", "<sourceDbId>" + sourceDbId
				+ "</sourceDbId>");

		if(schedulerName.contains(MessageConstant.TRANSCATION_HISTORY_LOG)){
			sImport = sImport.replaceAll("<sourceTable>[\\s|\\S]*</sourceTable>", "<sourceTable>" +groupName+"_"+ schedulerName
					+ "</sourceTable>");
		}else {
			sImport = sImport.replaceAll("<sourceTable>[\\s|\\S]*</sourceTable>", "<sourceTable>" + (true == schedulerName.contains("_verify") ? schedulerName.replace("_verify", "") : schedulerName)
					+ "</sourceTable>");
		}

		sImport = sImport.replaceAll("<targetTable>[\\s|\\S]*</targetTable>",
				"<targetTable>" + groupName.toLowerCase()+ "_"+schedulerName.toLowerCase()+CommonEnumCollection.HiveDefinePartNameEnum.ODS_TABLE_NAME_SUBFIX.getValue() + "</targetTable>");

//		sImport=sImport.replace("dc_retail_pos.order_main", groupName
//				+ "_"+schedulerName);
		return sImport;
	}


	public static String readFileForStream(InputStream isrInput) throws IOException {
		InputStreamReader isr = new InputStreamReader(isrInput, "UTF-8");
		StringBuffer sbread = new StringBuffer();
		while (isr.ready()) {
			sbread.append((char) isr.read());
		}
		isr.close();
		return sbread.toString();
	}


	public static String readFile(String filePath) throws IOException {
		if(!(new File(filePath)).exists())return "";
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(filePath), "UTF-8");
		StringBuffer sbread = new StringBuffer();
		while (isr.ready()) {
			sbread.append((char) isr.read());
		}
		isr.close();
		return sbread.toString();
	}

	public static void main(String[] args) {
		try {
			ParseXMLFileUtil.initTask();
		} catch (ManagerException e1) {
			e1.printStackTrace();
		}

	}
}
