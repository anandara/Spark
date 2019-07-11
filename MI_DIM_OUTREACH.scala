package com.molina

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import org.apache.hadoop.io.nativeio.NativeIO.Windows

object MI_DIM_OUTREACH_old {
	def main(args: Array[String]) {

		val conf = new SparkConf()

				val sc = new SparkContext(conf)
				val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
		sqlContext.setConf("hive.exec.dynamic.partition", "true")
					sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

				val outreach_df=sqlContext.sql("""SELECT distinct cm.SSIS_EXTERNAL_ID, op.CID, np.PATIENT_ID, em.externalId AS EXTERNAL_ID
, cm.medicaid_no
,cm.employer
,  oi.id AS ContactID, oi.Subject
, oi.create_date AS  CreateDate
, concat(cast(year(oi.create_date) as string),case when length(cast(month(oi.create_date) as String))=2 then 
cast(month(oi.create_date) as String) else concat('0',cast(month(oi.create_date) as String)) end )    AS Create_YYYYMM 
, COALESCE(pc.STR_VALUE, pcl.STR_VALUE, pc2.STR_VALUE, pc3.STR_VALUE) AS ContactDate
,phone.STR_VALUE AS UpdatedPhone
, COALESCE(s1.STRING, s2.STRING, s3.STRING, s4.STRING) AS ContactMethod
, COALESCE(s5.STRING, s6.STRING,s7.STRING, s8.STRING) AS OutcomeOfContact
, COALESCE(s9.STRING,  s10.STRING,s11.STRING,  s12.STRING) AS ContactType
, COALESCE(concat(ou.first_name,' ',ou.last_name),'') AS CreatedBy
, COALESCE(s13.STRING,  s14.STRING,s15.STRING,  s16.STRING) ContactDirection
, COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) as  ContactPurpose
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '1%' AND COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) NOT LIKE '%10%' then 'Y' else 'N' end  Assessment
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%2%' then 'Y' else 'N' end  Care_Plan_Development_Revision
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%3%' then 'Y' else 'N' end  Coordination_of_Services
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%4%' then 'Y' else 'N' end   Education_Coaching
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%5%' then 'Y' else 'N' end   Followup_Contact
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%6%' then 'Y' else 'N' end  Psychosocial_Support
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%7%' then 'Y' else 'N' end   Welcome_Contact
,'N' AS RDP_Contact
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%8%' then 'Y' else 'N' end Other
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%9%' then 'Y' else 'N' end Transition_of_Care
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%10%' then 'Y' else 'N' end Case_Manager_Change
,case when COALESCE(cp.STR_VALUE, cpl.STR_VALUE, cp2.STR_VALUE, cp3.STR_VALUE) LIKE '%11%' then 'Y' else  'N' end  PreCall_Review 
, '                                                 ' as Member_attribute
, current_timestamp() as insert_date

FROM cca_db.org_info oi  
INNER JOIN cca_db.org_notepad np ON   oi.id = np.info_id 
INNER JOIN cca_db.org_patient op ON  op.ID = np.PATIENT_ID 
INNER JOIN cca_db.member cm   ON cm.CID = op.CID 
INNER JOIN cca_db.external_member_data em ON em.CID = cm.CID
LEFT JOIN  cca_db.org_user ou ON ou.id = oi.user_id
LEFT JOIN (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE 
FROM cca_db.p_member_concept_value WHERE concept_id = 500643 ) pc 
ON pc.CID = cm.CID  AND substr(cast(pc.update_time as string),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND pc.Rn =1 

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE 
FROM cca_db.P_member_concept_value_log WHERE concept_id = 500643) pcl 
ON pcl.CID = cm.CID  AND substr(cast(pcl.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND pcl.Rn =1  

LEFT JOIN (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE 
FROM cca_db.P_member_concept_value WHERE concept_id = 500643 ) pc2 
ON pc2.CID = cm.CID  AND substr(cast(pc2.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND pc2.Rn =1  

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE 
FROM cca_db.P_member_concept_value_log WHERE concept_id = 500643 ) pc3
ON pc3.CID = cm.CID  AND substr(cast(pc3.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND pc3.Rn =1  

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE 
FROM cca_db.P_member_concept_value WHERE concept_id = 502575 ) pm 
ON pm.CID = cm.CID  AND substr(cast(pm.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND pm.Rn =1  
LEFT JOIN cca_db.STRING_LOCALE s1 ON  s1.id = 502575 AND cast(s1.sub_id as int) = cast(pm.str_value as int)

LEFT JOIN (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE 
FROM cca_db.P_member_concept_value_log WHERE concept_id = 502575 ) pml 
ON pml.CID = cm.CID  AND substr(cast(pml.update_time as STRING),0, 17) = substr(cast(oi.create_date as String),0, 17) AND pml.Rn =1  
LEFT JOIN cca_db.STRING_LOCALE s2 ON  s2.id = 502575 AND cast(s2.sub_id as int ) = cast(pml.str_value as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE 
FROM cca_db.P_member_concept_value WHERE concept_id = 502575 ) pm2
ON pm2.CID = cm.CID  AND substr(cast(pm2.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND pm2.Rn =1  
LEFT JOIN cca_db.STRING_LOCALE s3 ON  s3.id = 502575 AND cast(s3.sub_id as int) = cast(pm2.str_value as int)

LEFT JOIN (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE 
FROM cca_db.P_member_concept_value_log WHERE concept_id = 502575 ) pm3
ON pm3.CID = cm.CID  AND substr(cast(pm3.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND pm3.Rn =1  
LEFT JOIN cca_db.STRING_LOCALE s4 ON  s4.id = 502575 AND cast(s4.sub_id as int) = cast(pm3.str_value as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 500641 ) po
ON  po.CID = cm.CID AND substr(cast(po.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND po.Rn =1
LEFT JOIN cca_db.STRING_LOCALE s5 ON s5.id = 500641 AND cast(s5.SUB_ID as int) = cast(po.str_value as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value_log WHERE concept_id = 500641)  pol 
ON pol.CID = cm.CID AND substr(cast(pol.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND pol.Rn = 1
LEFT JOIN cca_db.STRING_LOCALE s6 ON s6.id = 500641 AND cast(s6.SUB_ID as int) = cast(pol.str_value as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 500641 ) po2
ON  po2.CID = cm.CID AND substr(cast(po2.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND po2.Rn =1
LEFT JOIN cca_db.STRING_LOCALE s7 ON s7.id = 500641 AND cast(s7.SUB_ID as int) = cast(po2.str_value as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value_log WHERE concept_id = 500641)  po3 
ON po3.CID = cm.CID AND substr(cast(po3.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND po3.Rn = 1
LEFT JOIN cca_db.STRING_LOCALE s8 ON s8.id = 500641 AND cast(s8.SUB_ID as int) = cast(po3.str_value as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 502278 ) pt
ON  pt.CID = cm.CID AND substr(cast(pt.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND pt.Rn =1 
LEFT JOIN cca_db.STRING_LOCALE s9 ON s9.id = 502278 AND cast(s9.SUB_ID as int) = cast(pt.str_value  as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value_log WHERE concept_id = 502278)  ptl 
ON ptl.CID = cm.CID AND substr(cast(ptl.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND ptl.Rn = 1  
LEFT JOIN cca_db.STRING_LOCALE s10 ON s10.id = 502278 AND cast(s10.SUB_ID as int) = cast(ptl.str_value  as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 502278 ) pt2
ON  pt2.CID = cm.CID AND substr(cast(pt2.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND pt2.Rn =1 
LEFT JOIN cca_db.STRING_LOCALE s11 ON s11.id = 502278 AND cast(s11.SUB_ID as int) = cast(pt2.str_value  as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value_log WHERE concept_id = 502278)  pt3 
ON pt3.CID = cm.CID AND substr(cast(pt3.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND pt3.Rn = 1  
LEFT JOIN cca_db.STRING_LOCALE s12 ON s12.id = 502278 AND cast(s12.SUB_ID as int) = cast(pt3.str_value  as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 509808 ) pd
ON  pd.CID = cm.CID AND substr(cast(pd.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND pd.Rn =1 
LEFT JOIN cca_db.STRING_LOCALE s13 ON s13.id = 509808 AND cast(s13.SUB_ID as int) = cast(pd.str_value  as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value_log WHERE concept_id = 509808)  pdl 
ON pdl.CID = cm.CID AND substr(cast(pdl.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND pdl.Rn = 1 
LEFT JOIN cca_db.STRING_LOCALE s14 ON s14.id = 509808 AND cast(s14.SUB_ID as int) = cast(pdl.str_value  as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 509808 ) pd2
ON  pd2.CID = cm.CID AND substr(cast(pd2.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND pd2.Rn =1 
LEFT JOIN cca_db.STRING_LOCALE s15 ON s15.id = 509808 AND cast(s15.SUB_ID as int) = cast(pd2.str_value  as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value_log WHERE concept_id = 509808)  pd3 
ON pd3.CID = cm.CID AND substr(cast(pd3.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND pd3.Rn = 1 
LEFT JOIN cca_db.STRING_LOCALE s16 ON s16.id = 509808 AND s16.SUB_ID = cast(pd3.str_value  as int)

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 500646 )  cp 
ON cp.CID = cm.CID AND substr(cast(cp.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND cp.Rn = 1

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 17) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value_log WHERE concept_id = 500646 )  cpl 
ON cpl.CID = cm.CID AND substr(cast(cpl.update_time as STRING),0, 17) = substr(cast(oi.create_date as STRING),0, 17) AND cpl.Rn = 1 

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 500646 )  cp2 
ON cp2.CID = cm.CID AND substr(cast(cp2.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND cp2.Rn = 1

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value_log WHERE concept_id = 500646 )  cp3 
ON cp3.CID = cm.CID AND substr(cast(cp3.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND cp3.Rn = 1 

LEFT JOIN  (SELECT cid, update_time, ROW_NUMBER() OVER (PARTITION BY CID, substr(cast(update_time as STRING),0, 16) ORDER BY update_time DESC) AS Rn,  STR_VALUE FROM cca_db.P_member_concept_value WHERE concept_id = 500255 ) phone
ON  phone.CID = cm.CID AND substr(cast(phone.update_time as STRING),0, 16) = substr(cast(oi.create_date as STRING),0, 16) AND phone.Rn =1 

where oi.template_id=500273 and year(oi.create_date) in (2016,2017,2018)""")

//outreach_df.cache()



				val external_system1_df= sqlContext.table("cca_db.external_system").selectExpr("id es_id","name es_name")
				broadcast(external_system1_df)
//				val external_system2_df=	external_system1_df.selectExpr("es_id es1_id","es_name es1_name")
//				val external_member_data_df=sqlContext.table("cca_db.external_member_data").selectExpr("cid emd_cid","system_id")

//				val joinwithEMD_df=	outreach_df.join(external_member_data_df,col("cid")===col("emd_cid"),"leftouter")
				val joinwithES_df=outreach_df.join(external_system1_df,col("es_id")===col("employer"),"leftouter")
//				val joinwithES2_df=joinwithES_df.join(external_system2_df,col("es1_id")===col("SSIS_EXTERNAL_ID"),"leftouter")
				val BeforeMemMonths_df=joinwithES_df.withColumn("State", expr("case when substring(es_name,6,2) ='MA' then 'MC' when substring(es_name,6,2)= 'UN' then 'NY' else substring(es_name,6,2) end")).drop("SSIS_EXTERNAL_ID").drop("es_id").drop("es_name").distinct()

				val membermonths_df=sqlContext.table("semantic_db.membermonth").selectExpr("carriermemid","case when productname is null then '                                                 ' else  productname end as EC_Product","case when regionname is null then '                                                 ' else regionname end as Region_Code","yearmonth")
				val JoinwithMembermonths_df=BeforeMemMonths_df.join(membermonths_df,col("carriermemid")===col("medicaid_no") && col("yearmonth")===col("Create_YYYYMM"),"inner")
				val final_Df=JoinwithMembermonths_df.drop("carriermemid").drop("yearmonth").distinct().withColumn("part_state", col("state"))
				
				
				final_Df.filter("upper(state)='IL'").withColumn("part_state",col("state")).write.mode("overwrite").partitionBy("part_state").insertInto("semantic_db.dim_outreach")





	}
}