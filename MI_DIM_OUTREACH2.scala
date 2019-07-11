package com.molina

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import org.apache.hadoop.io.nativeio.NativeIO.Windows
import java.util._  
import java.io._ 

object MI_DIM_OUTREACH{
	def main(args: Array[String]) {

		val conf = new SparkConf()

				val sc = new SparkContext(conf)
				val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
		
		     sqlContext.setConf("hive.exec.dynamic.partition", "true")
				sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
				
				var reader:FileReader=new FileReader("MI_DIM_OUTREACH.properties") 
        var prop:Properties=new Properties()
        prop.load(reader)
		
//				var databasename="cca_db"
        
        var CCA_DB_ORG_INFO=prop.getProperty("CCA_DB_ORG_INFO")
				var CCA_DB_ORG_NOTEPAD=prop.getProperty("CCA_DB_ORG_NOTEPAD")
				var CCA_DB_ORG_PATIENT=prop.getProperty("CCA_DB_ORG_PATIENT")
				var CCA_DB_MEMBER=prop.getProperty("CCA_DB_MEMBER")
				var CCA_DB_EXTERNAL_MEMBER_DATA=prop.getProperty("CCA_DB_EXTERNAL_MEMBER_DATA")
				var CCA_DB_ORG_USER=prop.getProperty("CCA_DB_ORG_USER")
				var CCA_DB_RESTRICTION_GROUP=prop.getProperty("CCA_DB_RESTRICTION_GROUP")
				var CCA_DB_P_MEMBER_CONCEPT_VALUE=prop.getProperty("CCA_DB_P_MEMBER_CONCEPT_VALUE")
				var CCA_DB_P_MEMBER_CONCEPT_VALUE_LOG=prop.getProperty("CCA_DB_P_MEMBER_CONCEPT_VALUE_LOG")
				var CCA_DB_STRING_LOCALE=prop.getProperty("CCA_DB_STRING_LOCALE")
				var SEMANTIC_DB_MEMBERMONTH=prop.getProperty("SEMANTIC_DB_MEMBERMONTH")
				
				
				sqlContext.table("cca_db.org_info_txn")
								
				val tblOrgInfoDf=sqlContext.table(CCA_DB_ORG_INFO).withColumn("row", row_number().over(Window.partitionBy(col("id"),col("part_state")).orderBy(desc("job_date")))).filter("row=1").selectExpr("id ContactID","Subject","create_date CreateDate","user_id","template_id").filter("template_id=500273 and year(CreateDate) >= 2016 ")
				val tblOrgNotepadDf=sqlContext.table(CCA_DB_ORG_NOTEPAD).withColumn("row", row_number().over(Window.partitionBy(col("id"),col("part_state")).orderBy(desc("job_date")))).filter("row=1").selectExpr("info_id","PATIENT_ID")
				val tblOrgPatientDF=sqlContext.table(CCA_DB_ORG_PATIENT).withColumn("row", row_number().over(Window.partitionBy(col("id"),col("part_state")).orderBy(desc("job_date")))).filter("row=1").selectExpr("id op_id","cid op_cid")
				val tblMemberDf=sqlContext.table(CCA_DB_MEMBER).withColumn("row", row_number().over(Window.partitionBy(col("cid"),col("part_state")).orderBy(desc("job_date")))).filter("row=1").selectExpr("cid","SSIS_EXTERNAL_ID", "medicaid_no","employer")
				val tblExternalMemberDataDf=sqlContext.table(CCA_DB_EXTERNAL_MEMBER_DATA).withColumn("row", row_number().over(Window.partitionBy(col("cid"),col("part_state")).orderBy(desc("job_date")))).filter("row=1").selectExpr("cid em_cid","externalId EXTERNAL_ID")
				val tblOrgUserDf=sqlContext.table(CCA_DB_ORG_USER).selectExpr("id ou_id","COALESCE(concat(first_name,' ',last_name),'') CreatedBy")
				
				
				
//				val external_system1_df= sqlContext.table("cca_db.external_system").selectExpr("id es_id","name es_name")
//				broadcast(external_system1_df)
//				val joinwithES2_df=tblMemberDf.join(external_system1_df,col("es_id")===col("employer"),"leftouter")
//				val MemberWithState=joinwithES2_df.withColumn("State", expr("case when substring(es_name,6,2) ='MA' then 'MC' when substring(es_name,6,2)= 'UN' then 'NY' else substring(es_name,6,2) end")).drop("SSIS_EXTERNAL_ID").drop("es_id").drop("es_name").distinct()//filter("upper(state)='IL'")
//		
				
				/*  Updated State Logic 07/01/2019  */ 
				val restriction_group_df=sqlContext.table(CCA_DB_RESTRICTION_GROUP).selectExpr("id RG_ID","group_value")
				val joinwithRG_df=tblMemberDf.join(restriction_group_df,col("RG_ID")===col("employer"),"leftouter")
				val MemberWithState=joinwithRG_df.withColumn("State",expr("substring(group_value,0,2)"))
				    
				    
				//Joins
				
				val joinWithNotepad=tblOrgInfoDf.join(tblOrgNotepadDf,col("ContactID")===col("info_id"),"inner")
				val joinWithPatient=joinWithNotepad.join(tblOrgPatientDF,col("op_id")===col("PATIENT_ID"),"inner")
				val joinWithMember=joinWithPatient.join(MemberWithState,col("cid")===col("op_cid"),"inner")
				val joinWithExternalMember=joinWithMember.join(tblExternalMemberDataDf,col("em_cid")===col("cid"),"inner").distinct()
							
				
				val joinWithUser=joinWithExternalMember.join(tblOrgUserDf,col("ou_id")===col("user_id"),"leftouter")

				//tables
				val tblMemberConceptValue=sqlContext.table(CCA_DB_P_MEMBER_CONCEPT_VALUE).withColumn("row", row_number().over(Window.partitionBy(col("cid"),col("concept_id")).orderBy(desc("update_time")))).filter("row=1").selectExpr("cid MCV_cid","update_time MCV_update_time","concept_id MCV_concept_id","STR_VALUE MCV_STR_VALUE").filter("MCV_concept_id in (500643,500255,502575,500641,502278,509808,500646)").withColumn("rn",row_number().over(Window.partitionBy(col("MCV_concept_id"),col("MCV_cid"),expr("substr(cast(MCV_update_time as STRING),0, 15)")).orderBy(desc("MCV_update_time"))).alias("Rn")).filter("Rn=1").drop("Rn").distinct
				val tblMemberConceptValue_pivot1= tblMemberConceptValue.groupBy(col("MCV_cid"), expr("substr(cast(MCV_update_time as STRING),0, 15)").alias("MCV_update_time")).pivot("MCV_concept_id").agg(expr("max(MCV_STR_VALUE)"))
				val tblMemberConceptValue_pivot=tblMemberConceptValue_pivot1.select( tblMemberConceptValue_pivot1.columns.map { c => tblMemberConceptValue_pivot1.col(c).as( "a_" + c ) } : _* )
				val tblMemberConceptValueLog=sqlContext.table(CCA_DB_P_MEMBER_CONCEPT_VALUE_LOG).withColumn("row", row_number().over(Window.partitionBy(col("cid"),col("concept_id")).orderBy(desc("update_time")))).filter("row=1").selectExpr("cid MCVL_cid","update_time MCVL_update_time","concept_id MCVL_concept_id","STR_VALUE MCVL_STR_VALUE").filter("MCVL_concept_id in (500643,500255,502575,500641,502278,509808,500646)").withColumn("rn",row_number().over(Window.partitionBy(col("MCVL_concept_id"),col("MCVL_cid"),expr("substr(cast(MCVL_update_time as STRING),0, 15)")).orderBy(desc("MCVL_update_time"))).alias("Rn")).filter("Rn=1").drop("Rn").distinct
				val tblMemberConceptValueLog_pivot1=tblMemberConceptValueLog.groupBy(col("MCVL_cid"), expr("substr(cast(MCVL_update_time as STRING),0, 15)").alias("MCVL_update_time")).pivot("MCVL_concept_id").agg(expr("max(MCVL_STR_VALUE)"))
				val tblMemberConceptValueLog_pivot=tblMemberConceptValueLog_pivot1.select( tblMemberConceptValueLog_pivot1.columns.map { c => tblMemberConceptValueLog_pivot1.col(c).as( "b_" + c ) } : _* )
				val tblMemberConceptValue1=tblMemberConceptValue.selectExpr("MCV_cid MCV1_cid","MCV_update_time MCV1_update_time","MCV_concept_id MCV1_concept_id","MCV_STR_VALUE MCV1_STR_VALUE").withColumn("rn",row_number().over(Window.partitionBy(col("MCV1_concept_id"),col("MCV1_cid"),expr("substr(cast(MCV1_update_time as STRING),0, 14)")).orderBy(desc("MCV1_update_time"))).alias("Rn")).filter("Rn=1").drop("Rn").distinct
				val tblMemberConceptValue1_pivot1=tblMemberConceptValue1.groupBy(col("MCV1_cid"), expr("substr(cast(MCV1_update_time as STRING),0, 14)").alias("MCV1_update_time")).pivot("MCV1_concept_id").agg(expr("max(MCV1_STR_VALUE)"))
				val tblMemberConceptValue1_pivot=tblMemberConceptValue1_pivot1.select( tblMemberConceptValue1_pivot1.columns.map { c => tblMemberConceptValue1_pivot1.col(c).as( "c_" + c ) } : _* )
				val tblMemberConceptValueLog1=tblMemberConceptValueLog.selectExpr("MCVL_cid MCVL1_cid","MCVL_update_time MCVL1_update_time","MCVL_concept_id MCVL1_concept_id","MCVL_STR_VALUE MCVL1_STR_VALUE").withColumn("rn",row_number().over(Window.partitionBy(col("MCVL1_concept_id"),col("MCVL1_cid"),expr("substr(cast(MCVL1_update_time as STRING),0, 14)")).orderBy(desc("MCVL1_update_time"))).alias("Rn")).filter("Rn=1").drop("Rn").distinct
				val tblMemberConceptValueLog1_pivot1=tblMemberConceptValueLog1.groupBy(col("MCVL1_cid"), expr("substr(cast(MCVL1_update_time as STRING),0, 14)").alias("MCVL1_update_time")).pivot("MCVL1_concept_id").agg(expr("max(MCVL1_STR_VALUE)"))
				val tblMemberConceptValueLog1_pivot=tblMemberConceptValueLog1_pivot1.select( tblMemberConceptValueLog1_pivot1.columns.map { c => tblMemberConceptValueLog1_pivot1.col(c).as( "d_" + c ) } : _* )
				
//				(500643,500255,502575,500641,502278,509808,500646)
				val tblStringLocale=sqlContext.table(CCA_DB_STRING_LOCALE).selectExpr("id","sub_id","String").filter("id in (502575,500641,502278,509808)").distinct 				
				val tblStringLocale1=tblStringLocale.selectExpr("id s1_id","sub_id s1_sub_id","String s1_STRING_value").filter("s1_id = 502575").distinct
  			val tblStringLocale2=tblStringLocale.selectExpr("id s2_id","sub_id s2_sub_id","STRING s2_STRING_value").filter("s2_id =500641").distinct
				val tblStringLocale3=tblStringLocale.selectExpr("id s3_id","sub_id s3_sub_id","STRING s3_STRING_value").filter("s3_id =502278").distinct
				val tblStringLocale4=tblStringLocale.selectExpr("id s4_id","sub_id s4_sub_id","STRING s4_STRING_value").filter("s4_id =509808").distinct
							
				val MCVWith_502575=tblMemberConceptValue_pivot.join(tblStringLocale1,expr("cast(a_502575 as int)")===col("s1_sub_id"),"leftouter").drop("s1_sub_id").drop("s1_id").withColumnRenamed("s1_STRING_value", "a_502575_strVal")
				val MCVWith_500641=MCVWith_502575.join(tblStringLocale2,expr("cast(a_500641 as int)")===col("s2_sub_id"),"leftouter").drop("s2_sub_id").drop("s2_id").withColumnRenamed("s2_STRING_value", "a_500641_strVal")
				val MCVWith_502278=MCVWith_500641.join(tblStringLocale3,expr("cast(a_502278 as int)")===col("s3_sub_id"),"leftouter").drop("s3_sub_id").drop("s3_id").withColumnRenamed("s3_STRING_value", "a_502278_strVal")
				val MCVWith_509808=MCVWith_502278.join(tblStringLocale4,expr("cast(a_509808 as int)")===col("s4_sub_id"),"leftouter").drop("s4_sub_id").drop("s4_id").withColumnRenamed("s4_STRING_value", "a_509808_strVal")
				
				val MCV_df=MCVWith_509808
				
				val MCVLWith_502575=tblMemberConceptValueLog_pivot.join(tblStringLocale1,expr("cast(b_502575 as int)")===col("s1_sub_id"),"leftouter").drop("s1_sub_id").drop("s1_id").withColumnRenamed("s1_STRING_value", "b_502575_strVal")
				val MCVLWith_500641=MCVLWith_502575.join(tblStringLocale2,expr("cast(b_500641 as int)")===col("s2_sub_id"),"leftouter").drop("s2_sub_id").drop("s2_id").withColumnRenamed("s2_STRING_value", "b_500641_strVal")
				val MCVLWith_502278=MCVLWith_500641.join(tblStringLocale3,expr("cast(b_502278 as int)")===col("s3_sub_id"),"leftouter").drop("s3_sub_id").drop("s3_id").withColumnRenamed("s3_STRING_value", "b_502278_strVal")
				val MCVLWith_509808=MCVLWith_502278.join(tblStringLocale4,expr("cast(b_509808 as int)")===col("s4_sub_id"),"leftouter").drop("s4_sub_id").drop("s4_id").withColumnRenamed("s4_STRING_value", "b_509808_strVal")
				
				val MCVL_df=MCVLWith_509808	
				
				val MCV1With_502575=tblMemberConceptValue1_pivot.join(tblStringLocale1,expr("cast(c_502575 as int)")===col("s1_sub_id"),"leftouter").drop("s1_sub_id").drop("s1_id").withColumnRenamed("s1_STRING_value", "c_502575_strVal")
				val MCV1With_500641=MCV1With_502575.join(tblStringLocale2,expr("cast(c_500641 as int)")===col("s2_sub_id"),"leftouter").drop("s2_sub_id").drop("s2_id").withColumnRenamed("s2_STRING_value", "c_500641_strVal")
				val MCV1With_502278=MCV1With_500641.join(tblStringLocale3,expr("cast(c_502278 as int)")===col("s3_sub_id"),"leftouter").drop("s3_sub_id").drop("s3_id").withColumnRenamed("s3_STRING_value", "c_502278_strVal")
				val MCV1With_509808=MCV1With_502278.join(tblStringLocale4,expr("cast(c_509808 as int)")===col("s4_sub_id"),"leftouter").drop("s4_sub_id").drop("s4_id").withColumnRenamed("s4_STRING_value", "c_509808_strVal")
				
				val MCV1_df=MCV1With_509808
				
				val MCVL1With_502575=tblMemberConceptValueLog1_pivot.join(tblStringLocale1,expr("cast(d_502575 as int)")===col("s1_sub_id"),"leftouter").drop("s1_sub_id").drop("s1_id").withColumnRenamed("s1_STRING_value", "d_502575_strVal")
				val MCVL1With_500641=MCVL1With_502575.join(tblStringLocale2,expr("cast(d_500641 as int)")===col("s2_sub_id"),"leftouter").drop("s2_sub_id").drop("s2_id").withColumnRenamed("s2_STRING_value", "d_500641_strVal")
				val MCVL1With_502278=MCVL1With_500641.join(tblStringLocale3,expr("cast(d_502278 as int)")===col("s3_sub_id"),"leftouter").drop("s3_sub_id").drop("s3_id").withColumnRenamed("s3_STRING_value", "d_502278_strVal")
				val MCVL1With_509808=MCVL1With_502278.join(tblStringLocale4,expr("cast(d_509808 as int)")===col("s4_sub_id"),"leftouter").drop("s4_sub_id").drop("s4_id").withColumnRenamed("s4_STRING_value", "d_509808_strVal")
				
				val MCVL1_df=MCVL1With_509808
			
				
				//joins
				
				val joinWithMCV=joinWithUser.join(MCV_df,col("cid")===col("a_MCV_cid")&& expr("substr(cast(a_MCV_update_time as string),0, 15) = substr(cast(CreateDate as STRING),0, 15)"),"leftouter")
				val joinWithMCVL=joinWithMCV.join(MCVL_df,col("cid")===col("b_MCVL_cid")&& expr("substr(cast(b_MCVL_update_time as string),0, 15) = substr(cast(CreateDate as STRING),0, 15)"),"leftouter")
				val joinWithMCV1=joinWithMCVL.join(MCV1_df,col("cid")===col("c_MCV1_cid")&& expr("substr(cast(c_MCV1_update_time as string),0, 14) = substr(cast(CreateDate as STRING),0, 14)"),"leftouter")
				val joinWithMCVL1=joinWithMCV1.join(MCVL1_df,col("cid")===col("d_MCVL1_cid")&& expr("substr(cast(d_MCVL1_update_time as string),0, 14) = substr(cast(CreateDate as STRING),0, 14)"),"leftouter")
				
			
//				joinWithMCVL1.write.mode("overwrite").parquet("/edl/transform/semantic/data/DIM_OUTREACH_intermediate")
//				val temp=joinWithSL4.persist()
				
//				val CCA_intermediate=sqlContext.read.parquet("user/hive/warehouse/semantic_db.db/dim_careplan")
//				var nullVal:String=null
				
				val AddContactDate=joinWithMCVL1.withColumn("ContactDate",expr("COALESCE(a_500643,b_500643,c_500643,d_500643)"))
				val AddUpdatedPhone=AddContactDate.withColumn("UpdatedPhone",expr("c_500255"))
				val AddContactMethod=AddUpdatedPhone.withColumn("ContactMethod",expr("COALESCE(a_502575_strVal,b_502575_strVal,c_502575_strVal,d_502575_strVal)"))
				val AddOutcomeOfContact=AddContactMethod.withColumn("OutcomeOfContact",expr("coalesce(a_500641_strVal,b_500641_strVal,c_500641_strVal,d_500641_strVal)"))
  			val AddContactType=AddOutcomeOfContact.withColumn("ContactType",expr("coalesce(a_502278_strval,b_502278_strval,c_502278_strval,d_502278_strval)"))
  			val AddContactDirection=AddContactType.withColumn("ContactDirection", expr("COALESCE(a_509808_strval,b_509808_strval,c_509808_strval,d_509808_strval)"))
  			val AddContactPurpose=AddContactDirection.withColumn("ContactPurpose",expr("COALESCE(a_500646,b_500646,c_500646,d_500646)"))
				val AddAssessment=AddContactPurpose.withColumn("Assessment",expr("case when ContactPurpose Like '1%' and ContactPurpose not like '%10%' then 'Y' else 'N' end"))
				val AddCare_Plan_Development_Revision=AddAssessment.withColumn("Care_Plan_Development_Revision",expr("case when  ContactPurpose like '%2%' then 'Y' else 'N' end"))
				val AddCoordination_of_Services=AddCare_Plan_Development_Revision.withColumn("Coordination_of_Services",expr("case when  ContactPurpose like '%3%' then 'Y' else 'N' end"))
				val AddEducation_Coaching=AddCoordination_of_Services.withColumn("Education_Coaching",expr("case when  ContactPurpose like '%4%' then 'Y' else 'N' end"))
				val AddFollowup_Contact=AddEducation_Coaching.withColumn("Followup_Contact",expr("case when  ContactPurpose like '%5%' then 'Y' else 'N' end"))
				val AddPsychosocial_Support=AddFollowup_Contact.withColumn("Psychosocial_Support",expr("case when  ContactPurpose like '%6%' then 'Y' else 'N' end"))
				val AddWelcome_Contact=AddPsychosocial_Support.withColumn("Welcome_Contact",expr("case when  ContactPurpose like '%7%' then 'Y' else 'N' end"))
				val AddOther=AddWelcome_Contact.withColumn("Other",expr("case when  ContactPurpose like '%8%' then 'Y' else 'N' end"))
				val AddTransition_of_Care=AddOther.withColumn("Transition_of_Care",expr("case when  ContactPurpose like '%9%' then 'Y' else 'N' end"))
				val AddCase_Manager_Change=AddTransition_of_Care.withColumn("Case_Manager_Change",expr("case when  ContactPurpose like '%10%' then 'Y' else 'N' end"))
				val AddPreCall_Review=AddCase_Manager_Change.withColumn("PreCall_Review",expr("case when  ContactPurpose like '%11%' then 'Y' else 'N' end"))
				
				val outreach_df=AddPreCall_Review.selectExpr("op_cid cid", "PATIENT_ID", "EXTERNAL_ID","medicaid_no","employer",
				    "ContactID","Subject","CreateDate","concat(cast(year(CreateDate) as string),case when length(cast(month(CreateDate) as String))=2 then cast(month(CreateDate) as String) else concat('0',cast(month(CreateDate) as String)) end )  Create_YYYYMM",
				    "ContactDate","UpdatedPhone",
				    "ContactMethod",
				    "OutcomeOfContact",
				    "ContactType",
				    "CreatedBy",
				    "ContactDirection",
				    "ContactPurpose",
				    "Assessment",
				    "Care_Plan_Development_Revision",
				    "Coordination_of_Services",
				    "Education_Coaching",
				    "Followup_Contact",
				    "Psychosocial_Support",
				    "Welcome_Contact",
				    "'N' RDP_Contact","Other",
				    "Transition_of_Care",
				    "Case_Manager_Change",
				    "PreCall_Review",
				    "'                                                 ' Member_attribute",
				    "current_timestamp() insert_date",
				    "state").distinct()  

//				val membermonths_df=sqlContext.table("semantic_db.membermonth").selectExpr("carriermemid","case when productname is null then '                                                 ' else  productname end as EC_Product","case when regionname is null then '                                                 ' else regionname end as Region_Code","yearmonth")
//				val JoinwithMembermonths_df=outreach_df.join(membermonths_df,col("carriermemid")===col("medicaid_no") && col("yearmonth")===col("Create_YYYYMM"),"inner")
//				val final_Df=JoinwithMembermonths_df.drop("carriermemid").drop("yearmonth").distinct()
				    
				val outreach_yearmonth=outreach_df.selectExpr("EXTERNAL_ID QNXT_MEMBERID","Create_YYYYMM","state").distinct
				
				val outreach_dist_MemberID=outreach_df.selectExpr("EXTERNAL_ID QNXT_MEMBERID1").distinct
				
//				val outreach_yearmonth_MC=outreach_df.select("MEDICAID_NO","Create_YYYYMM").distinct
				
				val membermonths_df=sqlContext.table(SEMANTIC_DB_MEMBERMONTH).selectExpr("lobname","memberid","case when productname is null then '                                                 ' else  productname end as EC_Product","case when regionname is null then '                                                 ' else regionname end as Region_Code","yearmonth","state membermonth_state")

//				val Max_yearMonth=membermonths_df.selectExpr("lobname lobname1","memberid memid","EC_Product EC_Product1","Region_Code Region_Code1", "membermonth_state membermonth_state1","yearmonth yearmonth1").withColumn("rn",row_number().over(Window.partitionBy(col("memid")).orderBy(desc("yearmonth1")))).filter("rn=1")
				
				val Max_yearMonth=sqlContext.read.parquet("/edl/transform/semantic/data/mi_membermonth_intermediate")
				val join_distMemID=Max_yearMonth.join(outreach_dist_MemberID,col("QNXT_MEMBERID1")===col("memid"),"inner")
				
//				val membermonths_df_withMC=membermonths_df.filter("upper(trim(Lobname))='MEDICARE'")
//				
//				val membermonths_withoutMC=membermonths_df.filter("upper(trim(Lobname))<>'MEDICARE'")
				
//				val distinct_MC=membermonths_df_withMC.join(outreach_yearmonth_MC,col("carriermemid")===col("MEDICAID_NO") && col("yearmonth")===col("Create_YYYYMM"),"inner").
//				selectExpr("carriermemid MC_carriermemid","yearmonth MC_yearmonth","EC_Product MC_EC_Product","Region_Code MC_Region_Code")
//				
//				val distinct_NonMC=membermonths_withoutMC.join(outreach_yearmonth,col("carriermemid")===col("MEDICAID_NO") && col("yearmonth")===col("Create_YYYYMM") && col("membermonth_state")===col("state") ,"inner").
//				selectExpr("carriermemid","yearmonth","EC_Product EC_product1","Region_Code Region_Code1","membermonth_state")
				val distinct_MM=membermonths_df.join(outreach_yearmonth,col("QNXT_MEMBERID")===col("memberid") && col("yearmonth")===col("Create_YYYYMM") && col("membermonth_state")===col("state") ,"inner").
				selectExpr("lobname lobname2","memberid","yearmonth","EC_Product EC_Product2","Region_Code Region_Code2","membermonth_state").withColumn("rn",row_number().over(Window.partitionBy("memberid","yearmonth","membermonth_state"))).filter("rn=1")
//				.groupBy("memberid","yearmonth","membermonth_state").agg(expr("max(EC_product)").alias("EC_product2"),expr("max(Region_Code)").alias("Region_Code2"),expr("max(lobname2)").alias("lobname2"))	 
				
//				val joinWithMC=outreach_df.join(distinct_MC,col("MC_carriermemid")===col("MEDICAID_NO") && col("MC_yearmonth")===col("Create_YYYYMM"),"leftouter").drop("MC_yearmonth").drop("MC_carriermemid")
//				
//				val joinWithoutMC=joinWithMC.join(distinct_NonMC,col("carriermemid")===col("MEDICAID_NO") && col("yearmonth")===col("Create_YYYYMM") && col("membermonth_state")===col("state"),"leftouter")
				
				val joinWithMM=outreach_df.join(distinct_MM,col("EXTERNAL_ID")===col("memberid") && col("yearmonth")===col("Create_YYYYMM") && col("membermonth_state")===col("state"),"leftouter")
					 
				val joinWithMaxYearmonth=joinWithMM.join(join_distMemID,col("EXTERNAL_ID")===col("memid") && col("membermonth_state1")===col("state"),"leftouter")
//				val EC_product_df=joinWithoutMC.withColumn("EC_product",coalesce(col("MC_EC_Product"),col("EC_product1"))).withColumn("Region_Code",coalesce(col("MC_Region_Code"),col("Region_Code1"))) 
				val EC_Product=joinWithMaxYearmonth.withColumn("EC_Product", coalesce(col("EC_Product2"),col("EC_Product1"))).withColumn("Region_Code", coalesce(col("Region_Code2"),col("Region_Code1"))).withColumn("lobname", coalesce(col("lobname2"),col("lobname1")))
				
				val final_Df=EC_Product.select("cid","patient_id","external_id","medicaid_no","employer","contactid","subject","createdate","create_yyyymm","contactdate", "updatedphone","contactmethod","outcomeofcontact","contacttype","createdby","contactdirection","contactpurpose","assessment","care_plan_development_revision","coordination_of_services","education_coaching","followup_contact","psychosocial_support","welcome_contact","rdp_contact","other","transition_of_care","case_manager_change","precall_review","member_attribute","insert_date","state","ec_product","region_code","lobname")
//				val final_Df=EC_product_df.drop("MC_EC_Product").drop("EC_product1").drop("carriermemid").drop("yearmonth").drop("membermonth_state").distinct()    
				    
				final_Df.withColumn("part_state",col("state")).write.mode("overwrite").partitionBy("part_state").insertInto("semantic_db_stg.CCA_outreach")    
			
//				final_Df.write.mode("overwrite").saveAsTable("semantic_db.mi_dim_outreach_optimized")
	}
}