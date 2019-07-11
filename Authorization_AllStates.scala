
//package com.molina
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import org.apache.hadoop.io.nativeio.NativeIO.Windows


object Authorization_AllStates {
	def main(args: Array[String]) {

		try{

			val conf = new SparkConf()

					val sc = new SparkContext(conf)
					val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)


					//sqlContext.setConf("yarn.scheduler.minimum-allocation-mb","4096")
					sqlContext.setConf("hive.exec.dynamic.partition", "true")
					sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
					sqlContext.clearCache()
					//sqlContext.setConf("spark.sql.shuffle.partitions","200")
					//sqlContext.setConf("spark.yarn.executor.memoryOverhead", "3000")
					//sqlContext.setConf("spark.yarn.am.memoryOverhead", "3000")

					sqlContext.setConf("spark.sql.tungsten.enabled","true")
					var databasename="qnxt_db"
					var databasename1="EXECDB_DB"
					var StateCode="TX"
					var databasename_semantic="semantic_db_tx"


					//---User Defined Functions-------------
					//----UDF BEGIN---------------------------------------------------


					val date_AdmitMonth_udf=udf({(AdmitDate_year:String,AdmitDate_month:String,effdate_year:String,effdate_month:String)=>
					if(AdmitDate_year!=null && AdmitDate_year!="2078")
					{	if(AdmitDate_month.length==2)
						AdmitDate_year+AdmitDate_month
						else
							AdmitDate_year+"0"+AdmitDate_month
					}
					else
					{
						if(effdate_month.length==2)
							effdate_year+effdate_month
							else
								effdate_year+"0"+effdate_month
					}
					})

					val set_AgeCategort_udf=udf({(age_days:Int)=>
					var age=(age_days/365)
					if(age<=18)
						"00-18"
						else if(age>18 && age<=24)
							"19-24"
							else if(age>24 && age<=34)
								"25-34"
								else if(age>34 && age<=54)
									"35-54"
									else if(age>54 && age<=64)
										"55-64"
										else if(age>=65)
											"65+"
											else
												"unknown"
					})

					val risk_group_udf=udf({(ratecode:String)=> 
					if(ratecode.length()>3)
						ratecode.trim.substring(ratecode.trim.length()-3,ratecode.trim.length()) //Amol - Fixed - Aug 13th - function was taking left 3 characters instead of right 3. also applied trim as per source.
						else
							ratecode

					})

					//----END UDF--------------------------------------------------



					val get_refereal_df= sqlContext.table(databasename+"."+"referral").selectExpr("dispositionid","servicecode","AdmitDate","effdate","referFROM","referTo","referralid","enrollid","admitdate AuthAdmitdate","dischargedate AuthDischDate","effdate AuthEffDate","termdate AuthTermDate","AuthStatus AuthStatus","authorizationid AuthNumber","issueinitials ReceivedBy","upper(createid) As createid ","rtrim(createid) createid_nochange","acuity","Appeal","AppealDate","Appealoutcome","memid","highlight","referraldate","nextreviewdate","rtrim(updateid) as updateid","paytoaffiliationid","priority","receiptdate receiptdate_ref","part_state","carriermemid").filter("part_state ='" + StateCode + "' and (effdate between date_sub(add_months(current_date(),-36),dayofmonth(current_date())-1) and current_date())").drop("part_state")
					val get_authcode_df=sqlContext.table(databasename+"."+"authcode_txn").selectExpr("rtrim(description) AuthType","rtrim(description) AuthTemp","rtrim(codeid) AuthCode","rtrim(authtype) AuthCategory","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_member_df=sqlContext.table(databasename+"."+"member").selectExpr("fullname","part_state","memid mem_memid","guardian","Sex Gender","DOB","headofhouse","ethnicid","primarylanguage mem_primarylanguage").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_MAPD_Member_df=sqlContext.table(databasename+"."+"member").selectExpr("fullname MAPD_fullname","part_state MAPD_partstate","memid MAPD_memid","dob MAPD_dob","guardian MAPD_guardian","Sex MAPD_Gender","headofhouse MAPD_headofhouse","ethnicid MAPD_ethnicid","primarylanguage MAPD_primarylanguage").filter("MAPD_partstate='MC'")
					val get_enrollcoverage_df=sqlContext.table(databasename+"."+"enrollcoverage").selectExpr("ratecode ec_ratecode","effdate EnrollCovEffDate","termdate EnrollCovTermDate","enrollid EC_enrollid","lastupdate  EnrollCovlastUpdate","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_enrollkeys_df=sqlContext.table(databasename+"."+"enrollkeys").selectExpr("enrollid EK_enrollid","planid EK_planid","carriermemid MedicaidID","effdate EnrollEffdate","termdate EnrollTermDate","lastupdate EnrollLastUpdate","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_benefitplan_txn_df=sqlContext.table(databasename+"."+"benefitplan_txn").selectExpr("programid bp_programid","PlanID bp_planid","description Benefit","description plandesc","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_program_df=sqlContext.table(databasename+"."+"program").selectExpr("description Program","programid prog_programid","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_authdiag_df=sqlContext.table(databasename+"."+"authdiag_txn").selectExpr("rtrim(diagcode) diagcode","rtrim(referralid) diag_referralid","rtrim(Upper(diagqualifier)) diagqualifier","rtrim(Sequence) Sequence","rtrim(IcdVersion) IcdVersion","rtrim(part_state) part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_language_df=sqlContext.table(databasename+"."+"language_txn").selectExpr("languageid","Description PrimaryLanguage","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_Provider_df=sqlContext.table(databasename+"."+"Provider").selectExpr("provid prv_provid","FullName DischargeFacility","provid as DischargeFacilityId","fedid","npi","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_Provider1_df=get_Provider_df.selectExpr("prv_provid referFROM_provid","DischargeFacility referFROM_fullname","fedid referFROM_fedid","npi referFROM_npi")
					val get_Provider2_df=get_Provider_df.selectExpr("prv_provid aff_provid","DischargeFacility aff_fullname","fedid aff_fedid","npi aff_npi")
					val get_affiliation_df=sqlContext.table(databasename+"."+"affiliation").selectExpr("affiliateid","provid","termdate","effdate","createdate","lastupdate","payflag","affiliationid","affiltype","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_Ethnicity_df=sqlContext.table(databasename+"."+"Ethnicity_txn").selectExpr("description as Ethnicity","ethnicid ETH_ethnicid","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_LOB_df=sqlContext.table(databasename1+"."+"LOB").selectExpr("lobname LOB_lobname","state LOB_state","planid LOB_plan_id","state").filter("state ='" + StateCode + "'").drop("state") 
					val get_product_df=sqlContext.table(databasename1+"."+"product").selectExpr("state p_state","planid p_planid","productname p_productname","aidcode p_aidcode","state").filter("state ='" + StateCode + "'").drop("state") 
					val get_Region_df=sqlContext.table(databasename1+"."+"Region").selectExpr("countyname R_CountyName","RegionName","planid R_planid","aidcode R_aidcode","state R_state","state").filter("state ='" + StateCode + "'").drop("state")

					println("---------------------------------Broadcasting started---------------------------------------")
					broadcast(get_Provider_df)
					broadcast(get_affiliation_df)

					println("---------------------------------Broadcasting Finished---------------------------------------")

					val add_AdmitMonth_df=get_refereal_df.withColumn("AdmitMonth", date_AdmitMonth_udf(year(col("AdmitDate")),month(col("AdmitDate")),year(col("effdate")),month(col("effdate"))))
					//Pradeepa : Modified to get a 2 digit month number.
					val set_ageCategort_df=get_member_df.withColumn("AgeCategory",set_AgeCategort_udf(datediff(current_timestamp(),col("DOB"))))

					val join_refandmem_df=add_AdmitMonth_df.join(set_ageCategort_df,set_ageCategort_df("mem_memid")<=>add_AdmitMonth_df("memid"),"inner")
					val join_withAuthcode_df=join_refandmem_df.join(get_authcode_df,get_authcode_df("AuthCode")<=>join_refandmem_df("servicecode"),"inner")
					val join_withEC_df= join_withAuthcode_df.join(get_enrollcoverage_df,get_enrollcoverage_df("EC_enrollid")<=>join_withAuthcode_df("enrollid"),"inner")
					val join_witn_EK_df=join_withEC_df.join(get_enrollkeys_df,get_enrollkeys_df("EK_enrollid")<=>join_withEC_df("EC_enrollid"),"inner")
					val join_with_BP_df=join_witn_EK_df.join(get_benefitplan_txn_df,get_benefitplan_txn_df("bp_planid")<=>join_witn_EK_df("EK_planid"),"inner")
					val join_withProg_df=join_with_BP_df.join(get_program_df,get_program_df("prog_programid")<=>join_with_BP_df("bp_programid"),"inner")
					val join_withdiag_df=join_withProg_df.join(get_authdiag_df,get_authdiag_df("diag_referralid")<=>join_withProg_df("referralid")&& get_authdiag_df("diagqualifier")<=>"PRINCIPAL","inner")
					val join_withProvider_df=join_withdiag_df.join(get_Provider_df,get_Provider_df("Prv_ProvID")<=>join_withdiag_df("referto"),"leftouter")



					val get_affiliationRank_df=get_affiliation_df.filter("payflag<>0 and affiltype in('DIRECT','GROUP','INCENTIVE','NETWORK')")
					val get_affiliationRank1_df=get_affiliationRank_df.select(col("affiliateid"),col("affiltype"),col("affiliationid"),col("payflag"),col("provid").alias("afprovid"),row_number().over(Window.partitionBy("provid").orderBy(desc("termdate"),desc("effdate"),desc("createdate"),desc("lastupdate"),asc("payflag"))).alias("rank"))
					val join_withRank_df=join_withProvider_df.join(get_affiliationRank1_df,get_affiliationRank1_df("afprovid")<=>join_withProvider_df("prv_provid")&&get_affiliationRank1_df("rank")<=>1,"leftouter")
					val join_withLOB_df=join_withRank_df.join(get_LOB_df,get_LOB_df("LOB_plan_id")<=>join_withRank_df("EK_planid") && get_LOB_df("LOB_state")<=>StateCode,"leftouter")
					val join_withProduct_df=join_withLOB_df.join(get_product_df,(get_product_df("p_planid")<=>join_withLOB_df("EK_planid")&& get_product_df("p_state")<=>StateCode&& get_product_df("p_aidcode")<=>"NA") ||(get_product_df("p_planid")<=>join_withLOB_df("EK_planid")&& get_product_df("p_state")<=>StateCode && risk_group_udf(col("ec_ratecode"))<=>get_product_df("p_aidcode")),"leftouter")


					println("---------------------------------persisting 1 started---------------------------------------")
					join_withProduct_df.persist()
					System.gc()

					println("---------------------------------persisting 1 Finished---------------------------------------")

					val join_withMAPD_df=join_withProduct_df.join(get_MAPD_Member_df,(get_MAPD_Member_df("MAPD_headofhouse")<=>join_withProduct_df("headofhouse") || expr("'"+StateCode+"'"+get_MAPD_Member_df("MAPD_headofhouse"))<=>join_withProduct_df("headofhouse")) && get_MAPD_Member_df("MAPD_DOB")<=>join_withProduct_df("DOB") && get_MAPD_Member_df("MAPD_fullname")<=>join_withProduct_df("fullname"),"leftouter") 
					val join_withRegion_df=join_withMAPD_df.join(get_Region_df,get_Region_df("R_planid")<=>"NA" && get_Region_df("R_State")<=>StateCode && substring(col("ec_ratecode"), 0, 2)<=> get_Region_df("R_AidCode"),"leftouter")
					val join_withEthnicity_df=join_withRegion_df.join(get_Ethnicity_df,get_Ethnicity_df("ETH_ethnicid")<=>join_withRegion_df("ethnicid"),"leftouter")

					val joinwithLang_df=join_withEthnicity_df.join(get_language_df,get_language_df("languageid")<=>join_withEthnicity_df("mem_primarylanguage"),"leftouter").withColumn("riskgroup", risk_group_udf(col("ec_ratecode")))

					//Amol - Removed 1 AdmitCount on sept 14th
					val TEMP_LOAD_TX_df1=joinwithLang_df.selectExpr("'TX' State","LOB_lobname LobName"," CASE when MAPD_headofhouse is NULL and bp_planid ='QMXBP8226' THEN 'DSNP' else  p_productname end ProductName","R_CountyName CountyName","AdmitMonth","RegionName","ec_ratecode ratecode","referFROM","referTo","fullname","memid","dob","guardian","MedicaidID" ,"EnrollEffdate" ,"EnrollTermDate","EnrollLastUpdate" ,"EnrollCovEffDate" ,"EnrollCovTermDate" ,"EnrollCovlastUpdate" ,"referralid" ,"enrollid" ,"AuthType" ,"AuthCode" ,"AuthCategory","AuthAdmitdate" ,"AuthDischDate","AuthEffDate" ,"AuthTermDate" ,"AuthStatus" ,"AuthNumber" ,"Program" ,"prog_programid programid","Benefit" ,"bp_planid Planid","diagcode" ,"'null' MemType","riskgroup","'OTHERS         ' BedType" ,"'OTHERS       ' AS BedType_M","'OTHERS       ' AS BedType_BH","ReceivedBy","createid","acuity","DischargeFacility","'null' IsNetwork" ,"Gender","AgeCategory","Ethnicity","PrimaryLanguage","DischargeFacilityId","affiliationid IsNetwrkAffiliationID","'null' AuthReason","Appeal","AppealDate","Appealoutcome","'Molina Risk' RiskType","dispositionid","affiliateid","updateid","highlight","referraldate","nextreviewdate","paytoaffiliationid","ReceiptDate_ref","fedid","npi","createid_nochange","RegionName","IcdVersion","carriermemid","AuthTemp","plandesc").distinct()

					val T_TX_df=	TEMP_LOAD_TX_df1.withColumn("RN",row_number().over(Window.partitionBy("referralid").orderBy("referralid")) )
					val T_Tx_distinctRef_df=T_TX_df.filter("RN>1").selectExpr("referralid T_distinct_referralid").distinct()
					val Temp_load_tx_referralfilter_df=TEMP_LOAD_TX_df1.join(T_Tx_distinctRef_df,T_Tx_distinctRef_df("T_distinct_referralid")<=>TEMP_LOAD_TX_df1("referralid"),"leftouter")
					val TEMP_LOAD_TX_inreferral_df=Temp_load_tx_referralfilter_df.filter("T_distinct_referralid is not null").drop("T_distinct_referralid")
					val TEMP_LOAD_TX_notinreferral_df=Temp_load_tx_referralfilter_df.filter("T_distinct_referralid is null").drop("T_distinct_referralid")

					val filterByCovDate_df=TEMP_LOAD_TX_inreferral_df.filter("case when year(authadmitdate)=2078 or authadmitdate is null then autheffdate else authadmitdate  end between EnrollCovEffDate and EnrollCovTermDate")
					val filterByenrollDate_df=filterByCovDate_df.filter("case when year(authadmitdate)=2078 or authadmitdate is null then autheffdate else authadmitdate  end between EnrollEffDate and EnrollTermDate")
					val T_TX1_beforeUnion=filterByenrollDate_df.withColumn("RN", rowNumber().over(Window.partitionBy("referralid").orderBy(desc("Enrolllastupdate"),desc("EnrollCovlastupdate")))).filter("RN=1")
					val T_Tx1_df=T_TX1_beforeUnion.unionAll(TEMP_LOAD_TX_notinreferral_df.selectExpr("*","1"))

					println("---------------------------------persisting 2 started---------------------------------------")

					System.gc()
					T_Tx1_df.persist()
					println("---------------------------------persisting 2 Finished---------------------------------------")

					val T_Tx1_distinctRef_df=T_Tx1_df.selectExpr("referralid Tx1_distinct_referralid").distinct()

					T_Tx1_distinctRef_df.persist()

					val Temp_load_tx1_referralfilter_df=TEMP_LOAD_TX_df1.join(T_Tx1_distinctRef_df,T_Tx1_distinctRef_df("Tx1_distinct_referralid")<=>TEMP_LOAD_TX_df1("referralid"),"leftouter")
					val distinctreferal_df=Temp_load_tx1_referralfilter_df.filter("Tx1_distinct_referralid is null").selectExpr("referralid drop_referralid").distinct()
					val T_Tx1_Temp_referralid_df=T_Tx1_df.join(distinctreferal_df,distinctreferal_df("drop_referralid")<=>T_Tx1_df("referralid"),"leftouter").filter("drop_referralid is not null").drop("drop_referralid")
					//Pradeepa : in Join changed the <= to <=>
					val data_toInsert_T_Tx1_df=T_Tx1_Temp_referralid_df.withColumn("RN", rowNumber().over(Window.partitionBy("referralid").orderBy(asc("referralid"),desc("Enrolllastupdate"),desc("EnrollCovlastupdate")))).filter("RN=1")
					val insertintoTx1_df=T_Tx1_df.unionAll(data_toInsert_T_Tx1_df)

					//---till here

					val get_authservice_df= sqlContext.table(databasename+"."+"authservice_txn").selectExpr("rtrim(catid) catid ","rtrim(subcatid) subcatid","rtrim(svcgroupid) svcgroupid","rtrim(referralid) AS_referralID","rtrim(servcategory) servcategory","rtrim(status) status","rtrim(totalunits) totalunits","rtrim(ltrim(codeid)) codeid","rtrim(usedunits) usedunits","rtrim(state) AS_State","case when Sequence=1 then 1 else 0 end as AdmitCount","sequence AS_sequence","to_date(from_unixtime(UNIX_TIMESTAMP(cast(dosdate as TIMESTAMP),'MM/dd/yyyy'))) SERVICE_DOS").filter("AS_State ='" + StateCode + "'") //and servcategory='REV'
					//Pradeepa : Sequence added
					val join_withAuthService_df=insertintoTx1_df.join(get_authservice_df,col("referralid")<=>col("AS_referralID"),"leftouter")
					val get_svccode_df= sqlContext.table(databasename+"."+"svccode_txn").selectExpr("description SVC_desc","codegroup SVC_codegroup","rtrim(ltrim(codeid)) SVC_CodeID","Part_state SVC_state").filter("SVC_state ='" + StateCode + "'")
					val join_withSVC_df=join_withAuthService_df.join(get_svccode_df,col("SVC_CodeID")<=>col("CodeID"),"leftouter")

					val get_svccatgroup_df= sqlContext.table(databasename+"."+"svccatgroup_txn").selectExpr("subcatid SV_subcatid","catid SV_catid ","description RevCategory","svcgroupid SV_svcgroupid","codeid SV_codeid","pArt_State SV_state").filter("SV_state ='" + StateCode + "'")
					val join_WithCATGroup_df=join_withSVC_df.join(get_svccatgroup_df,col("SV_catid")<=>col("catid") && col("SV_svcgroupid")<=>col("svcgroupid") && col("SV_subcatid")<=>col("subcatid"),"leftouter")

					//DB name has to be changed in get_quser_df
					val get_quser_df = sqlContext.table(databasename+"."+"quser").selectExpr("concat(rtrim(lastname),rtrim(firstname)) Issued_by","lastname","firstname","rtrim(loginid) quser_loginid")


					val get_umdispositionr_df = sqlContext.table(databasename+"."+"umdisposition_txn").selectExpr("dispositiondesc","dispositionid dis_dispositionid","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val join_umdisposition_df=join_WithCATGroup_df.join(get_umdispositionr_df,col("dis_dispositionid")<=>col("dispositionid"),"leftouter")
					val join_withprovider1_df=join_umdisposition_df.join(get_Provider1_df,col("referFROM_provid")<=>col("referFROM"),"leftouter")
					val join_withprovider2_df=join_withprovider1_df.join(get_Provider2_df,col("aff_provid")<=>col("affiliateid"),"leftouter")

					println("------------------------------Persist 4 start--------------------------------")
					join_withprovider2_df.persist()
					println("------------------------------Persist 4 End --------------------------------")

					val get_referralattribute_df=sqlContext.table(databasename+"."+"referralattribute_txn").selectExpr("rtrim(thevalue) thevalue","rtrim(referralid) RA_referralid","rtrim(attributeid) RA_attributeid","rtrim(Part_State) RA_state").filter("ra_state='"+StateCode+"'")
					val get_qattribute_df=sqlContext.table(databasename+"."+"qattribute_txn").selectExpr("attributeid q_attributeid","description q_description","Part_state q_state").filter("q_state='"+StateCode+"'")

					val set_rank1=get_referralattribute_df.join(get_qattribute_df,col("q_attributeid")<=>col("RA_attributeid"),"inner").filter("q_description LIKE '%DECISION%' AND q_description LIKE '%DATE%'").withColumn("Rank1", row_number().over(Window.partitionBy("ra_referralid").orderBy(desc("thevalue")))).selectExpr("Rank1","ra_referralid Rank1_referralid","thevalue DecisionDate").filter("rank1=1")

					val set_rank2=get_referralattribute_df.join(get_qattribute_df,col("q_attributeid")<=>col("RA_attributeid"),"inner").filter("q_description LIKE '%Auth%' AND q_description LIKE '%Receipt%' AND q_description LIKE '%Date%'").withColumn("Rank2", row_number().over(Window.partitionBy("ra_referralid").orderBy(desc("thevalue")))).selectExpr("Rank2","ra_referralid Rank2_referralid","thevalue ReceiptDate").filter("rank2=1")

					val set_rank3=get_referralattribute_df.join(get_qattribute_df,col("q_attributeid")<=>col("RA_attributeid"),"inner").filter("q_description LIKE '%Auth%' AND q_description LIKE '%Receipt%' AND q_description LIKE '%Time%'").withColumn("Rank3", row_number().over(Window.partitionBy("ra_referralid").orderBy(desc("thevalue")))).selectExpr("Rank3","ra_referralid Rank3_referralid","thevalue ReceiptTime").filter("rank3=1")

					val set_rank4=get_referralattribute_df.join(get_qattribute_df,col("q_attributeid")<=>col("RA_attributeid"),"inner").filter("q_attributeid = 'C00982924' and thevalue <> 'yes'").withColumn("Rank4", row_number().over(Window.partitionBy("ra_referralid").orderBy(desc("thevalue")))).selectExpr("Rank4","ra_referralid Rank4_referralid","thevalue ExtensionDate").filter("rank4=1")

					val get_ref_df=get_refereal_df.selectExpr("referralid tmp_referralid", "AuthNumber tmp_AuthNumber","priority").distinct()


					val join_withRank1_df=get_ref_df.join(set_rank1,col("tmp_referralid")<=>col("Rank1_referralid"),"leftouter")
					val join_withRank2_df=join_withRank1_df.join(set_rank2,col("tmp_referralid")<=>col("rank2_referralid"),"leftouter")
					val join_withRank3_df=join_withRank2_df.join(set_rank3,col("tmp_referralid")<=>col("rank3_referralid"),"leftouter")
					val join_withRank4_df=join_withRank3_df.join(set_rank4,col("tmp_referralid")<=>col("rank4_referralid"),"leftouter")


					println("------------------------------Persist 3 start--------------------------------")
					join_withRank4_df.persist()
					println("------------------------------Persist 3 End --------------------------------")


					val get_quser1_df = get_quser_df.selectExpr("Issued_by Created_by" ,"quser_loginid quser1_loginid")
					val get_quser2_df = get_quser_df.selectExpr("Issued_by Updated_by","quser_loginid quser2_loginid")

					val temp_df=join_withprovider2_df.join(join_withRank4_df,col("tmp_referralid")<=>col("referralid"),"leftouter").withColumn("ApprovedDays",expr("case when  AuthCategory = 'Inpatient' and AuthStatus in ('APPROVED','PEND') and status <> 'DENIED' AND (servcategory <> 'CPT' OR SVC_desc = 'NO SERVICE') then totalunits WHEN AuthCategory = 'Inpatient' and AuthStatus in ('INPROCESS', 'MEDREVIEW', 'PEND') and status <> 'DENIED'AND (ServCategory <> 'CPT' OR SVC_desc = 'NO SERVICE') THEN totalunits  else null end"))
					val temp_df_1=temp_df.withColumn("DeniedDays",expr("case when AuthCategory = 'Inpatient' and authstatus in ('APPROVED','PEND') and status = 'DENIED' AND (ServCategory <> 'CPT' OR SVC_desc = 'NO SERVICE') then totalunits WHEN AuthCategory = 'Inpatient' and authstatus in ('INPROCESS', 'MEDREVIEW', 'PEND') and status = 'DENIED' AND (ServCategory <> 'CPT' OR SVC_desc = 'NO SERVICE') THEN totalunits WHEN AuthCategory = 'Inpatient' and authstatus in ('DENIED') THEN totalunits else null end "))
					val temp_df_2=temp_df_1.withColumn("Priority",expr("case when priority = 'H' THEN 'HIGH' ELSE priority END"))
					val joinwith_quser_df=temp_df_2.join(get_quser_df,col("quser_loginid")<=>col("ReceivedBy"),"leftouter")
					val joinwith_quser1_df=joinwith_quser_df.join(get_quser1_df,col("quser1_loginid")<=>col("createid_nochange"),"leftouter")
					val joinwith_quser2_df=joinwith_quser1_df.join(get_quser2_df,col("quser2_loginid")<=>col("updateid"),"leftouter")

					val get_dim_date_dd_df=sqlContext.table(databasename+"."+"IL_DIM_DATE").selectExpr("to_date(from_unixtime(UNIX_TIMESTAMP(cast(date_date as TIMESTAMP),'MM/dd/yyyy'))) dd_date_date")
					val get_dim_date_de_df=sqlContext.table(databasename+"."+"IL_DIM_DATE").selectExpr("to_date(from_unixtime(UNIX_TIMESTAMP(cast(date_date as TIMESTAMP),'MM/dd/yyyy'))) de_date_date")
					//Pradeepa : Took twice date_date column
					val join_withdimdate_df_1=joinwith_quser2_df.join(get_dim_date_dd_df,col("dd_date_date")<=>expr("case when DecisionDate in ('','MM/DD/YYYY') then null else to_date(from_unixtime(UNIX_TIMESTAMP(DecisionDate,'MM/dd/yyyy'))) end"),"leftouter")
					val join_withdimdate_df=join_withdimdate_df_1.join(get_dim_date_de_df,col("de_date_date")<=>expr("case when ExtensionDate in ('','MM/DD/YYYY') then null else to_date(from_unixtime(UNIX_TIMESTAMP(ExtensionDate,'MM/dd/yyyy')))  end"),"leftouter")	

					//Pradeepa : Moved the code heres

					val get_authreason_df=sqlContext.table(databasename+"."+"authreason_txn").selectExpr("reasoncode auth_reasonCode","description DeniedReason","description AuthReason","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_reasongroupdt_df=sqlContext.table(databasename+"."+"reasongroupdtl_txn").selectExpr("rtrim(reasoncode) rd_reasoncode","rtrim(reasongroupdtlid) rd_reasongroupdtlid","rtrim(part_state) part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_authservicereason_df=sqlContext.table(databasename+"."+"authservicereason_txn").selectExpr("reasongroupdtlid","referralid ar_referralid","sequence ar_sequence","part_state").filter("part_state ='" + StateCode + "'").drop("part_state")

					val join_withauthservide_df=join_withdimdate_df.join(get_authservicereason_df,col("AS_sequence")<=>col("ar_sequence") && col("referralid")<=>("ar_referralid"),"leftouter")
					//Pradeepa ; added sequence column join 
					val join_withReasongrpdtl=join_withauthservide_df.join(get_reasongroupdt_df,col("rd_reasongroupdtlid")<=>col("reasongroupdtlid"),"leftouter")
					val get_AuthReasonValue_df=join_withReasongrpdtl.join(get_authreason_df,col("auth_reasonCode")<=>col("rd_reasoncode"),"leftouter")


					//Pradeepa : added DecisionDate_Cleaned column and changed TAT_HOUR_EXT and TAT_DAY_EXT
					val TEMP_LOAD_TX_df=get_AuthReasonValue_df.selectExpr("dispositionid","'"+StateCode+"' State","AdmitMonth","ratecode","referFROM","referTo","fullname as membername","memid as memberid","dob","guardian","MedicaidID" ,"EnrollEffdate" ,"EnrollTermDate","EnrollLastUpdate" ,"EnrollCovEffDate" ,"EnrollCovTermDate" ,"EnrollCovlastUpdate" ,"referralid" ,"enrollid" ,"AuthType" ,"AuthCode" ,"AuthCategory","AuthAdmitdate" ,"AuthDischDate","AuthEffDate" ,"AuthTermDate" ,"AuthStatus" ,"AuthNumber" ,"Program" ,"programid","Benefit" ,"Planid","rtrim(diagcode) diagcode" ,"'null' MemType","riskgroup","ReceivedBy","acuity","case when AdmitCount is null then 1 else AdmitCount end as AdmitCount","DischargeFacility","'null' IsNetwork" ,"Gender","AgeCategory","Ethnicity","PrimaryLanguage","DischargeFacilityId","IsNetwrkAffiliationID","Appeal","AppealDate","Appealoutcome","'Molina Risk' RiskType","ApprovedDays","codeid CPT_HCPS","SVC_codegroup CPT_REV_CODE_GROUP","SVC_desc CPT_REV_DESC","DecisionDate","cast(dd_date_date as TIMESTAMP) DecisionDate_Cleaned","dispositiondesc Disposition"," cast(de_date_date as TIMESTAMP) ExtensionDate","highlight","referraldate Issue_date"," '' MedicalReviewer","nextreviewdate","updateid Nurse","aff_fedid patyto_fedid","aff_npi patyto_npi","paytoaffiliationid payto_affiliationid","aff_provid payto_provid","aff_fullname Payto_Provider_name"," '' PharmacyReviewer","ReceiptDate_ref ReceiptDate","ReceiptTime","referFROM_fedid ReferFrom_fedid","referFROM_fullname ReferFrom_Provider","fedid referto_fedid","npi referto_npi","AuthType Template","cast(totalunits as int) as totalunits","cast(UsedUnits as int) as UsedUnits","cast(DeniedDays as int) as DeniedDays","Priority","Created_by","Updated_by","Issued_by","datediff((case when DecisionDate in ('','MM/DD/YYYY') then null else to_date(from_unixtime(UNIX_TIMESTAMP(DecisionDate,'MM/dd/yyyy'))) end),ReceiptDate_ref) TAT_DAY","datediff((case when DecisionDate in ('','MM/DD/YYYY') then null else to_date(from_unixtime(UNIX_TIMESTAMP(DecisionDate,'MM/dd/yyyy'))) end),ReceiptDate_ref)*24 TAT_HOUR","datediff((case when de_date_date is not null then to_date(from_unixtime(UNIX_TIMESTAMP(ExtensionDate,'MM/dd/yyyy'))) when DecisionDate in ('','MM/DD/YYYY') then null else to_date(from_unixtime(UNIX_TIMESTAMP(DecisionDate,'MM/dd/yyyy'))) end),ReceiptDate_ref)  TAT_DAY_EXT","datediff ((case when de_date_date is not null then to_date(from_unixtime(UNIX_TIMESTAMP(ExtensionDate,'MM/dd/yyyy'))) when DecisionDate in ('','MM/DD/YYYY') then null else to_date(from_unixtime(UNIX_TIMESTAMP(DecisionDate,'MM/dd/yyyy'))) end),ReceiptDate_ref)*24  TAT_HOUR_EXT","status lstatus","ServCategory","CAST(SERVICE_DOS as TIMESTAMP) SERVICE_DOS","RevCategory","RegionName","IcdVersion","DeniedReason","'"+StateCode+"' Part_state","SUBSTR(AdmitMonth,1,4) Part_year","carriermemid","AuthTemp","plandesc","AS_sequence").distinct()


					println("----------------------------persist--------------------------")
					TEMP_LOAD_TX_df.persist()
					println("---------------------------persist----------------------------")

					val get_authcode_df1=sqlContext.table(databasename+"."+"authcode_txn").selectExpr("rtrim(codeid) AuthCode_final","rtrim(authcategory) AuthCategory_final","authtype Authtype_final","part_state" ).filter("part_state ='" + StateCode + "'").drop("part_state").filter(!isnull(col("AuthCode_final"))).distinct()
					val final_df=TEMP_LOAD_TX_df.join(get_authcode_df1,col("AuthCode_final")<=>col("AuthCode") && !isnull(col("AuthCode")),"inner")

					val AuthRn1_df=final_df.selectExpr("acuity","Disposition","AuthStatus","CPT_HCPS","AuthNumber authorizationid", "memberid memid", "AuthType_final",  "AuthAdmitdate AdmitDate", "AuthDischDate DischargeDate", "AuthCategory_final","diagcode primary_diag_code").filter("authstatus = 'CLOSED' AND AuthType_final <> 'Observation' and authtype_final = 'Inpatient' AND AuthCategory_final IN ('Med/Surg', 'Behavorial Heal') and (Disposition != 'Expired' or disposition is null) and ACUITY NOT IN ('Elective','Urgent')").filter(!isnull(col("primary_diag_code")) && !isnull(col("CPT_HCPS"))).filter("CPT_HCPS <>''").distinct()//,"primary_diag_css_grouper")
					val get_IL_DIM_HEDIS_CODES_df=sqlContext.table(databasename+"."+"il_dim_hedis_codes").selectExpr("rtrim(code) code","rtrim(Value_Set_Name) Value_Set_Name","rtrim(CODE_SYSTEM) CODE_SYSTEM").filter("Value_Set_Name IN ('Chemotherapy','Rehabilitation','Kidney Transplant','Bone Marrow Transplant','Organ Transplant Other Than Kidney') AND CODE_SYSTEM IN ('ICD10CM','ICD9CM')").select("code").filter(!isnull(col("code"))).distinct()
					val get_Primary_code_df=AuthRn1_df.join(get_IL_DIM_HEDIS_CODES_df,get_IL_DIM_HEDIS_CODES_df("code")<=>AuthRn1_df("primary_diag_code"),"leftouter").filter("code is null").drop("code")
					//Pradeepa : changed = to is
					val get_IL_DIM_HEDIS_CODES1_df=sqlContext.table(databasename+"."+"il_dim_hedis_codes").selectExpr("rtrim(code) code","rtrim(Value_Set_Name) Value_Set_Name","rtrim(CODE_SYSTEM) CODE_SYSTEM").filter("Value_Set_Name IN ('Chemotherapy','Rehabilitation','Kidney Transplant','Bone Marrow Transplant','Organ Transplant Other Than Kidney')AND CODE_SYSTEM IN ('UBREV','CPT','HCPCS')").select("code").filter(!isnull(col("code"))).distinct()

					val get_CPT_HCPS_df=get_Primary_code_df.join(get_IL_DIM_HEDIS_CODES1_df,get_IL_DIM_HEDIS_CODES1_df("code")<=>get_Primary_code_df("CPT_HCPS"),"leftouter").filter("code is null").drop("code")
					//Pradeepa : changed = to is
					val final_AuthRn1_df=get_CPT_HCPS_df.select("authorizationid", "memid", "AuthType_final",  "AdmitDate", "DischargeDate", "AuthCategory_final","primary_diag_code")
					val AuthRn_df=final_AuthRn1_df.withColumn("Rn",row_number().over(Window.partitionBy("memid").orderBy("DischargeDate")))
					val AuthRn_df1=AuthRn_df.selectExpr("authorizationid ar1_authorizationid", "memid ar1_memid", "AuthType_final ar1_AuthType",  "AdmitDate ar1_AdmitDate", "DischargeDate ar1_DischargeDate", "AuthCategory_final ar1_authcategory","primary_diag_code ar1_primary_diag_code","rn ar1_rn")
					val Readmit_df=AuthRn_df.join(AuthRn_df1,AuthRn_df1("ar1_memid")<=>AuthRn_df("memid") && !isnull(AuthRn_df1("ar1_memid")) && !isnull(AuthRn_df("memid")) && expr("ar1_rn-1")<=>AuthRn_df("rn"),"inner").distinct()

					println("-------------------------------Broadcast-----------------------")
					broadcast(Readmit_df)
					println("-------------------------------Broadcast-----------------------")


					val ReAdmit7_df= Readmit_df.filter("DATEDIFF(ar1_AdmitDate,DischargeDate) BETWEEN 1 AND 7 ").selectExpr("authorizationid readmit7_authorizationid", "memid readmit7_memid", "AdmitDate readmit7_AdmitDate", "DischargeDate readmit7_DischargeDate", "ar1_authorizationid readmit7_ReAd_authorizationid", "ar1_AdmitDate readmit7_Re_AdDate","ar1_DischargeDate readmit7_Re_DischargeDate").distinct()
					val Readmit30_df=Readmit_df.filter("DATEDIFF(ar1_AdmitDate,DischargeDate) BETWEEN 1 AND 29 ").selectExpr("authorizationid readmit30_authorizationid", "memid readmit30_memid", "AdmitDate readmit30_AdmitDate", "DischargeDate readmit30_DischargeDate", "ar1_authorizationid readmit30_ReAd_authorizationid", "ar1_AdmitDate readmit30_Re_AdDate","ar1_DischargeDate readmit30_Re_DischargeDate").distinct()
					val Readmits30DAYS_df=Readmit_df.filter("DATEDIFF(ar1_AdmitDate,DischargeDate) BETWEEN 1 AND 29 ").withColumn("DAYS_FROM_READMISSION_30DAYS", expr("DATEDIFF(ar1_AdmitDate,DischargeDate)")).selectExpr("authorizationid readmit30d_authorizationid", "memid readmit30d_memid", "AdmitDate readmit30d_admitdate", "DischargeDate readmit30d_DischargeDate", "ar1_authorizationid readmit30d_ReAd_authorizationid", "ar1_AdmitDate readmit30d_Re_AdDate","ar1_DischargeDate readmit30d_Re_DischargeDate","DAYS_FROM_READMISSION_30DAYS").distinct()
					val get_IL_DIM_HEDIS_CODES2_df=sqlContext.table(databasename+"."+"il_dim_hedis_codes").selectExpr("rtrim(code) code","rtrim(Value_Set_Name) Value_Set_Name").filter("Value_Set_Name IN ('Bipolar Disorder', 'Detoxification', 'Mental and Behavioral Disorders', 'Mental Health Diagnosis', 'Mental Illness', 'Psychosis', 'Schizophrenia')").select("code").filter(!isnull(col("code"))).distinct()
					val bh_reAdmit_df=Readmit_df.filter("DATEDIFF(ar1_AdmitDate,DischargeDate) BETWEEN 1 AND 29 ").selectExpr("primary_diag_code bh_reAdmit30_primary_diag_code","authorizationid bh_reAdmit30_authorizationid", "memid bh_reAdmit30_memid", "AdmitDate bh_reAdmit30_AdmitDate", "DischargeDate bh_reAdmit30_DischargeDate", "ar1_authorizationid bh_reAdmit30_ReAd_authorizationid", "ar1_AdmitDate bh_reAdmit30_Re_AdDate","ar1_DischargeDate bh_reAdmit30_Re_DischargeDate").distinct()
					val bh_reAdmite30_df=bh_reAdmit_df.join(get_IL_DIM_HEDIS_CODES2_df,bh_reAdmit_df("bh_reAdmit30_primary_diag_code")<=>get_IL_DIM_HEDIS_CODES2_df("code") || col("bh_reAdmit30_primary_diag_code")==="R45.851","inner").drop("bh_reAdmit30_primary_diag_code").drop("code")
					val bh_reAdmit1_df=Readmit_df.filter("DATEDIFF(ar1_AdmitDate,DischargeDate) BETWEEN 1 AND 59 ").selectExpr("primary_diag_code bh_reAdmit60_primary_diag_code ","authorizationid bh_reAdmit60_authorizationid", "memid bh_reAdmit60_memid", "AdmitDate bh_reAdmit60_AdmitDate", "DischargeDate bh_reAdmit60_DischargeDate", "ar1_authorizationid bh_reAdmit60_ReAd_authorizationid", "ar1_AdmitDate bh_reAdmit60_Re_AdDate","ar1_DischargeDate bh_reAdmit60_Re_DischargeDate").distinct()
					val bh_reAdmite60_df=bh_reAdmit1_df.join(get_IL_DIM_HEDIS_CODES2_df,bh_reAdmit1_df("bh_reAdmit60_primary_diag_code")<=>get_IL_DIM_HEDIS_CODES2_df("code") || col("bh_reAdmit60_primary_diag_code")==="R45.851","inner").drop("bh_reAdmit60_primary_diag_code").drop("code")
					val bh_reAdmit2_df=Readmit_df.filter("DATEDIFF(ar1_AdmitDate,DischargeDate) BETWEEN 1 AND 89 ").selectExpr("primary_diag_code bh_reAdmit90_primary_diag_code","authorizationid bh_reAdmit90_authorizationid", "memid bh_reAdmit90_memid", "AdmitDate bh_reAdmit90_AdmitDate", "DischargeDate bh_reAdmit90_DischargeDate", "ar1_authorizationid bh_reAdmit90_ReAd_authorizationid", "ar1_AdmitDate bh_reAdmit90_Re_AdDate","ar1_DischargeDate bh_reAdmit90_Re_DischargeDate").distinct()
					val bh_reAdmite90_df=bh_reAdmit2_df.join(get_IL_DIM_HEDIS_CODES2_df,bh_reAdmit2_df("bh_reAdmit90_primary_diag_code")<=>get_IL_DIM_HEDIS_CODES2_df("code") || col("bh_reAdmit90_primary_diag_code")==="R45.851","inner").drop("bh_reAdmit90_primary_diag_code").drop("code")


					val join_ReAdmit7_df=TEMP_LOAD_TX_df.join(ReAdmit7_df,col("readmit7_ReAd_authorizationid")<=>col("AuthNumber") && !isnull(col("AuthNumber")) && !isnull(col("readmit7_ReAd_authorizationid")),"leftouter").drop("readmit7_authorizationid").drop("readmit7_ReAd_authorizationid")
					val join_ReAdmit30_df=join_ReAdmit7_df.join(Readmit30_df,col("readmit30_ReAd_authorizationid")<=>col("AuthNumber") && !isnull(col("readmit30_ReAd_authorizationid")),"leftouter").drop("readmit30_authorizationid").drop("readmit30_ReAd_authorizationid")
					val join_BHReAdmit30_df=join_ReAdmit30_df.join(bh_reAdmite30_df,col("bh_reAdmit30_authorizationid")<=>col("AuthNumber") && !isnull(col("bh_reAdmit30_authorizationid")),"leftouter").drop("bh_reAdmit30_ReAd_authorizationid").drop("bh_reAdmit30_authorizationid")
					val join_BHReAdmit30Read_df=join_BHReAdmit30_df.join(bh_reAdmite30_df,col("bh_reAdmit30_ReAd_authorizationid")<=>col("AuthNumber") && !isnull(col("bh_reAdmit30_ReAd_authorizationid")),"leftouter")
					val join_BHReAdmit60_df=join_BHReAdmit30Read_df.join(bh_reAdmite60_df,col("bh_reAdmit60_ReAd_authorizationid")<=>col("AuthNumber") && !isnull(col("bh_reAdmit60_ReAd_authorizationid")),"leftouter")
					val join_BHReAdmit90_df=join_BHReAdmit60_df.join(bh_reAdmite90_df,col("bh_reAdmit90_ReAd_authorizationid")<=>col("AuthNumber") && !isnull(col("bh_reAdmit90_ReAd_authorizationid")),"leftouter")
					val join_ReAdmit7_ad_df=join_BHReAdmit90_df.join(ReAdmit7_df,col("readmit7_authorizationid")<=>col("AuthNumber") && !isnull(col("readmit7_authorizationid")),"leftouter")
					val join_ReAdmit30_ad_df=join_ReAdmit7_ad_df.join(Readmit30_df,col("readmit30_authorizationid")<=>col("AuthNumber") && !isnull(col("readmit30_authorizationid")),"leftouter")
					val join_ReAdmit30_days_df=join_ReAdmit30_ad_df.join(Readmits30DAYS_df,col("readmit30d_ReAd_authorizationid")<=>col("AuthNumber") && !isnull(col("readmit30d_ReAd_authorizationid")),"leftouter")


					val TEMP_LOAD_with_readmit_df=join_ReAdmit30_days_df.selectExpr("dispositionid","'"+StateCode+"' State","AdmitMonth","ratecode","referFROM","referTo","membername","memberid","dob","guardian","MedicaidID" ,"EnrollEffdate" ,"EnrollTermDate","EnrollLastUpdate" ,"EnrollCovEffDate" ,"EnrollCovTermDate" ,"EnrollCovlastUpdate" ,"referralid" ,"enrollid" ,"AuthType" ,"AuthCode" ,"AuthCategory","AuthAdmitdate" ,"AuthDischDate","AuthEffDate" ,"AuthTermDate" ,"AuthStatus" ,"AuthNumber" ,"Program" ,"programid","Benefit" ,"Planid","rtrim(diagcode) diagcode" ,"MemType","riskgroup","ReceivedBy","acuity","case when AdmitCount is null then 1 else AdmitCount end as AdmitCount","DischargeFacility","IsNetwork" ,"Gender","AgeCategory","Ethnicity","PrimaryLanguage","DischargeFacilityId","IsNetwrkAffiliationID","Appeal","AppealDate","Appealoutcome","'Molina Risk' RiskType","ApprovedDays","CPT_HCPS","CPT_REV_CODE_GROUP","CPT_REV_DESC","DecisionDate","DecisionDate_Cleaned","Disposition"," ExtensionDate","highlight","Issue_date"," '' MedicalReviewer","nextreviewdate","Nurse","patyto_fedid","patyto_npi","payto_affiliationid","payto_provid","Payto_Provider_name"," '' PharmacyReviewer","ReceiptDate","ReceiptTime","ReferFrom_fedid","ReferFrom_Provider","referto_fedid","referto_npi","Template","cast(totalunits as int) as totalunits","cast(UsedUnits as int) as UsedUnits","cast(DeniedDays as int) as DeniedDays","Priority","Created_by","Updated_by","Issued_by","TAT_DAY","TAT_HOUR","TAT_DAY_EXT","TAT_HOUR_EXT","'"+StateCode+"' Part_state","SUBSTR(AdmitMonth,1,4) Part_year","CASE WHEN readmit7_ReAd_authorizationid IS NOT NULL THEN 1 ELSE 0 END IP_readmit7day_Flag","CASE WHEN readmit30_ReAd_authorizationid IS NOT NULL THEN 1 ELSE 0 END IP_readmit30day_Flag","DAYS_FROM_READMISSION_30DAYS IP_days_from_readmission_30days_flag","CASE WHEN bh_reAdmit30_authorizationid IS NOT NULL THEN 1 ELSE 0 END IP_bhreadmit30day_initial_admit_flag","CASE WHEN bh_reAdmit30_ReAd_authorizationid IS NOT NULL THEN 1 ELSE 0 END IP_bhreadmit30day_flag","CASE WHEN bh_reAdmit60_ReAd_authorizationid IS NOT NULL THEN 1 ELSE 0 END IP_bhreadmit60day_flag","CASE WHEN bh_reAdmit90_ReAd_authorizationid IS NOT NULL THEN 1 ELSE 0 END IP_bhreadmit90day_flag","CASE WHEN readmit7_authorizationid IS NOT NULL THEN 1 ELSE 0 END IP_readmit7day_initial_admit_flag","CASE WHEN readmit30_authorizationid IS NOT NULL THEN 1 ELSE 0 END IP_readmit30day_initial_admit_flag","CASE  WHEN AdmitMonth >= '201801' AND acuity IN  ('Emergency') AND (DATEDIFF (DecisionDate_Cleaned,receiptdate)  <= 1 OR DATEDIFF (ExtensionDate,receiptdate)  <= 3) THEN 'Y'  WHEN AdmitMonth >= '201801' AND acuity IN  ('Emergency') AND (DATEDIFF (DecisionDate_Cleaned,receiptdate) > 1 OR DATEDIFF (ExtensionDate,receiptdate) > 3) THEN 'N' WHEN AdmitMonth >= '201801' AND acuity IN  ('Urgent') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),receiptdate)  <= 2 THEN 'Y'  WHEN AdmitMonth >= '201801' AND acuity IN  ('Urgent') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),receiptdate) > 2 THEN 'N' WHEN AdmitMonth >= '201801' AND acuity IN  ('Routine','Elective') AND (DATEDIFF (DecisionDate_Cleaned,receiptdate)  <= 4 OR DATEDIFF (ExtensionDate,receiptdate)  <= 8) THEN 'Y' WHEN AdmitMonth >= '201801' AND acuity IN  ('Routine','Elective') AND (DATEDIFF (DecisionDate_Cleaned,receiptdate)  > 4 OR DATEDIFF (ExtensionDate,receiptdate)  > 8) THEN 'N' WHEN acuity NOT  IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),receiptdate)  <= 3 THEN 'Y'  WHEN acuity NOT  IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),receiptdate) > 3 THEN 'N' WHEN acuity IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),receiptdate)  <= 10 THEN 'Y' WHEN acuity IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),receiptdate)  > 10 THEN 'N' ELSE  'Invalid' END KPITAT","CASE  WHEN  acuity NOT  IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)  BETWEEN 0 AND 1 THEN '0-1' WHEN  acuity NOT  IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)   BETWEEN 2 AND 3 THEN '2-3' WHEN  acuity NOT  IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)   BETWEEN 4 AND 10 THEN '4-10' WHEN  acuity NOT  IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)  BETWEEN 11 AND 20 THEN '11-20' WHEN  acuity NOT  IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)  > 20 THEN '>20'   END EXPEDITED_TAT","CASE  WHEN  acuity IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)  BETWEEN 0 AND 5 THEN '0-5'  WHEN  acuity IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)   BETWEEN 6 AND 10 THEN '6-10' WHEN  acuity IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)   BETWEEN 11 AND 20 THEN '11-20' WHEN  acuity IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)  BETWEEN 21 AND 30 THEN '21-30' WHEN  acuity IN  ('Emergency','Elective') AND DATEDIFF (COALESCE(ExtensionDate,DecisionDate_Cleaned),ReceiptDate)  > 30 THEN '>30'  END Routine_TAT","lstatus","ServCategory","SERVICE_DOS","RevCategory","RegionName","IcdVersion","DeniedReason","carriermemid","AuthTemp","plandesc","AS_sequence").distinct()

					val with_LOS =TEMP_LOAD_TX_df.selectExpr("AuthNumber ONETWODAYAUTH_Authorizationid","case when AuthCategory = 'Inpatient' and DATEDIFF(AuthEffDate, AuthTermDate)=0 then 1.0	 when AuthCategory = 'Inpatient' then DATEDIFF(AuthEffDate, AuthTermDate) else null end LOS","cast(ApprovedDays AS Int) ApprovedDays").distinct()

					val ONETWODAYAUTH_df=with_LOS.groupBy("ONETWODAYAUTH_Authorizationid", "LOS").agg(expr("sum(ApprovedDays) DAYS_AUTH")).filter("LOS BETWEEN 1 AND 2").distinct()
					val join_ONETWODAYAUTH_df=TEMP_LOAD_with_readmit_df.join(ONETWODAYAUTH_df,ONETWODAYAUTH_df("ONETWODAYAUTH_Authorizationid")<=>TEMP_LOAD_with_readmit_df("AuthNumber") && !isnull(ONETWODAYAUTH_df("ONETWODAYAUTH_Authorizationid")),"leftouter").distinct()
					val Authorization_df1=join_ONETWODAYAUTH_df.withColumn("IP_one_two_day_stay_flag",expr("case when DAYS_AUTH<=2 then 'Y' else 'N' end ")).drop("ONETWODAYAUTH_Authorizationid").drop("DAYS_AUTH")

					//Pradeepa :Additional fields from diagcode table 
					val get_diagcode_df=sqlContext.table(databasename+"."+"diagcode_txn").selectExpr("rtrim(codeid) dc_codeid","rtrim(icdversion) dc_icdversion","rtrim(description) pridxdesc","rtrim(part_state) part_state").filter("part_state ='" + StateCode + "'").drop("part_state")
					val get_ccsgrouper_df=sqlContext.table("semantic_qnxt_inb.IL_DIM_REF_CCS_DIAGNOSIS_GROUPER_tmp").selectExpr("rtrim(codeid) ccs_codeid","rtrim(icd_version) ccs_icdversion","rtrim(CSS_Diag_Grouper) primary_diag_CCS_grouper")
					
					val Authorization_df2=Authorization_df1.join(get_diagcode_df,col("dc_codeid") <=> col("diagcode") && col("dc_icdversion") <=> col("icdversion"),"leftouter")
					val Authorization_df3=Authorization_df2.join(get_ccsgrouper_df,regexp_replace(col("DiagCode"),".","") <=> regexp_replace(col("ccs_codeid"),".","") && substring(col("ccs_icdversion"),1,1) <=> col("icdversion"),"leftouter")
					val Authorization_df4=Authorization_df3.groupBy("dispositionid","State","AdmitMonth","ratecode","referFROM","referTo","membername","memberid","dob","guardian","MedicaidID","EnrollEffdate","EnrollTermDate","EnrollLastUpdate","EnrollCovEffDate","EnrollCovTermDate","EnrollCovlastUpdate","referralid","enrollid","AuthType","AuthCode","AuthCategory","AuthAdmitdate","AuthDischDate","AuthEffDate","AuthTermDate","AuthStatus","AuthNumber","Program","programid","Benefit","Planid","diagcode","MemType","riskgroup","ReceivedBy","acuity","AdmitCount","DischargeFacility","IsNetwork","Gender","AgeCategory","Ethnicity","PrimaryLanguage","DischargeFacilityId","IsNetwrkAffiliationID","Appeal","AppealDate","Appealoutcome","RiskType","ApprovedDays","CPT_HCPS","CPT_REV_CODE_GROUP","CPT_REV_DESC","DecisionDate","DecisionDate_Cleaned","Disposition","ExtensionDate","highlight","Issue_date","MedicalReviewer","nextreviewdate","Nurse","patyto_fedid","patyto_npi","payto_affiliationid","payto_provid","Payto_Provider_name","PharmacyReviewer","ReceiptDate","ReceiptTime","ReferFrom_fedid","ReferFrom_Provider","referto_fedid","referto_npi","Template","totalunits","UsedUnits","DeniedDays","Priority","Created_by","Updated_by","Issued_by","TAT_DAY","TAT_HOUR","TAT_DAY_EXT","TAT_HOUR_EXT","IP_readmit7day_Flag","IP_readmit30day_Flag","IP_days_from_readmission_30days_flag","IP_bhreadmit30day_initial_admit_flag","IP_bhreadmit30day_flag","IP_bhreadmit60day_flag","IP_bhreadmit90day_flag","IP_readmit7day_initial_admit_flag","IP_readmit30day_initial_admit_flag","KPITAT","EXPEDITED_TAT","Routine_TAT","IP_one_two_day_stay_flag","lstatus","ServCategory","SERVICE_DOS","RevCategory","RegionName","LOS","pridxdesc","primary_diag_CCS_grouper","part_state","part_year","carriermemid","AuthTemp","plandesc","AS_sequence").agg(expr("max(DeniedReason) DeniedReason"))
					
					
					val Authorization_df = Authorization_df4.select("dispositionid","State","AdmitMonth","ratecode","referFROM","referTo","membername","memberid","dob","guardian","MedicaidID","EnrollEffdate","EnrollTermDate","EnrollLastUpdate","EnrollCovEffDate","EnrollCovTermDate","EnrollCovlastUpdate","referralid","enrollid","AuthType","AuthCode","AuthCategory","AuthAdmitdate","AuthDischDate","AuthEffDate","AuthTermDate","AuthStatus","AuthNumber","Program","programid","Benefit","Planid","diagcode","MemType","riskgroup","ReceivedBy","acuity","AdmitCount","DischargeFacility","IsNetwork","Gender","AgeCategory","Ethnicity","PrimaryLanguage","DischargeFacilityId","IsNetwrkAffiliationID","Appeal","AppealDate","Appealoutcome","RiskType","ApprovedDays","CPT_HCPS","CPT_REV_CODE_GROUP","CPT_REV_DESC","DecisionDate","DecisionDate_Cleaned","Disposition","ExtensionDate","highlight","Issue_date","MedicalReviewer","nextreviewdate","Nurse","patyto_fedid","patyto_npi","payto_affiliationid","payto_provid","Payto_Provider_name","PharmacyReviewer","ReceiptDate","ReceiptTime","ReferFrom_fedid","ReferFrom_Provider","referto_fedid","referto_npi","Template","totalunits","UsedUnits","DeniedDays","Priority","Created_by","Updated_by","Issued_by","TAT_DAY","TAT_HOUR","TAT_DAY_EXT","TAT_HOUR_EXT","IP_readmit7day_Flag","IP_readmit30day_Flag","IP_days_from_readmission_30days_flag","IP_bhreadmit30day_initial_admit_flag","IP_bhreadmit30day_flag","IP_bhreadmit60day_flag","IP_bhreadmit90day_flag","IP_readmit7day_initial_admit_flag","IP_readmit30day_initial_admit_flag","KPITAT","EXPEDITED_TAT","Routine_TAT","IP_one_two_day_stay_flag","lstatus","ServCategory","SERVICE_DOS","RevCategory","RegionName","LOS","pridxdesc","primary_diag_CCS_grouper","DeniedReason","part_state","part_year","carriermemid","AuthTemp","plandesc","AS_sequence").distinct()
					//
					//Authorization_df.registerTempTable("temp_Authorization_df")

					Authorization_df.write.mode("overwrite").partitionBy("part_state","part_year").insertInto("semantic_db.authorization_enhanced")

					//sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
					//Authorization_df.write.mode("overwrite").saveAsTable("semantic_db.authorization_enhanced_tx")

					//Authorization_df.write.mode("overwrite").saveAsTable("semantic_db.authorizations_test_20181009_latest")
					//sqlContext.sql("""insert overwrite table semantic_db.authorizations partition(part_state, part_year) select * from temp_Authorization_df""")

		}catch {
		case e: Exception =>{
			e.printStackTrace
		}	}
	}
}