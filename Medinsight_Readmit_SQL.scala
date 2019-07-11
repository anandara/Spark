package com.molina

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import scala.collection.immutable.List
import scala.collection.Map

object Medinsight_Readmit_SQL {
	def main(args: Array[String]) {

		val conf = new SparkConf()

				val sc = new SparkContext(conf)
				val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

				sqlContext.setConf("hive.exec.dynamic.partition", "true")
				sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
				
		val stateCode =Map("FL" -> "FLORIDA",
"IL" -> "ILINOIS",
"ID" -> "IDAHO",
"MI" -> "MICHIGAN",
"NM" -> "NEW MEXICO",
"NY" -> "NEW YORK",
"OH" -> "OHIO",
"PR" -> "PUERTO RICO",
"SC" -> "SOUTH CAROLINA",
"TX" -> "TEXAS",
"UT" -> "UTAH",
"WI" -> "WISCONSIN",
"WA" -> "WASHINGTON",
"MI" -> "MISSISSIPI",
"CA" -> "CALIFORNIA"
		)
		
//		for( stateKey <- stateCode.keys){
		var stateKey="SC"//args(0)
		       println(stateKey+"\t-->\t"+stateCode(stateKey))		

				val VW_CLAIM_CA_df=sqlContext.table("semantic_db.medinsight_claim").filter("year_mo>201512 and part_state='"+stateKey+"'").selectExpr("YEAR_MO","HCG_YEAR","CL_TYPE","CLAIM_ID","PLANID","RATECODE","MEM_COUNTY","REGION","PAT_ID","AMT_PAID","PAID_COMP_FACTOR","STARTDATE","ENDDATE","MR_ADMITS_CASES_RAW" ,"CLAIM_IN_NETWORK","HCG_MR_LINE","PROV_PCP","PROV_PCP_IPA","PROV_NETWORK","PROC_CODE","MR_UNITS_DAYS_RAW","TRANSACTION_FEE" ,"RX_REBATE","RX_NDC","NDC_SUB","PROV_ATT","SV_LINE","ICD_DIAG_01","DRG_MS","PROV_FACILITY","REV_CODE","MEMBER_QUAL","ICDVERSION","SUBSTITUTION_TYPE" ,"EST_SUB_RX_SAVINGS", "MR_PROCS_RAW","DRG_APR","MOLINARISK","LOB_GL","PROD_GL", "AMT_ALLOWED","RATE_AID_CODE","MEDINSIGHT_MS_DRG").filter(""" ENDDATE BETWEEN '2012-01-01' AND CURRENT_timestamp() AND SUBSTR(HCG_MR_LINE,1,1) IN ('I') AND HCG_MR_LINE NOT IN ('I31') and PAT_ID NOT IN ('MPMedicareIPLA201708', 'CAL120617968095', 'CA15060795629','PMedicareIPRIV201708','onsPlusIPOTHER201708', 'NRABDDualIPSAC201708', 'MBC0045048', 'NRABDDualIPIMP201708')""")
								 
				val VW_MEMBMTHS_CA_df=sqlContext.table("semantic_db.medinsight_membermonths").filter("part_state='"+stateKey+"'").selectExpr("year_mo","PAT_ID","AGE","MEM_GENDER","DUAL_TYPE", "DELIVERY_SYSTEM", "substr(CCHG_CAT_CODE_AND_DESC,1,3) CCHG_CAT","CCHG_CAT_CODE_AND_DESC", "MEMBER_QUAL")

			  val provider_df  = sqlContext.sql("""select distinct
pro.entityid,
pro.fullname,
pro.provtype,
case when pro.part_state='MC' then concat('M',pro.provid) else pro.provid end provid,
pro.npi, 
pro.fedid,
pro.part_state as pro_part_state,
pro.createdate,
pro.lastupdate,
pro.state as pro_stateid,
pro.specialtycode,
pro.exthours,
pro.PROFDESIG,
pro.status 
from qnxt_db.provider_txn pro where pro.part_state in ('MC','"""+stateKey+"""')""").withColumn("pro_stateid", when(col("pro_stateid") === stateKey , lit(stateKey)).otherwise(col("pro_stateid")))
provider_df .registerTempTable("provider_df")

val provider1_df = sqlContext.sql("""select * from (select *, row_number() over (partition by provid,pro_part_state order by lastupdate desc) rnk from provider_df) a where rnk=1""").drop("rnk")
provider1_df.registerTempTable("provider_grp")

val provider_del_df = sqlContext.sql("select part_state as del_state,provid,createdate from qnxt_db.provider_del")
provider_del_df.registerTempTable("provider_del")

val leftJoin_DF = sqlContext.sql("SELECT provider_grp.*,provider_del.provid as del_provid FROM provider_grp left join provider_del ON provider_grp.pro_part_state = provider_del.del_state and provider_grp.provid = provider_del.provid and provider_grp.createdate = provider_del.createdate")

val PROVIDER_df = leftJoin_DF.filter(col("del_provid").isNull).drop(col("createdate")).drop(col("lastupdate")).drop(col("del_provid")).drop(col("pro_part_state")).distinct()
PROVIDER_df.registerTempTable("provider")

//PROVIDER_df.write.mode("overwrite").saveAsTable("eim_datamanagement_db.Medinsight_Provider_CA")

val speciality_df = sqlContext.sql(s"""select distinct
spe.description as description,
upper(trim(spe.specialtycode)) as sp_specialtycode,
spe.part_state as spe_part_state,
spe.createdate,
spe.lastupdate,
spe.state as spe_stateid
from qnxt_db.specialty_txn spe where spe.part_state in ('"""+stateKey+"""','MC') 
union all select 'NO SPECIALTY','','','','','"""+stateKey+"""'
union all select 'NO SPECIALTY','','','','','MC'
""").withColumn("spe_stateid", when(col("spe_stateid") === stateKey , lit(stateKey)).otherwise(col("spe_stateid")))
speciality_df.registerTempTable("speciality_df")

val speciality_df1 = sqlContext.sql("""select * from (select *, rank() over (partition by sp_specialtycode,spe_part_state order by lastupdate desc) rnk from speciality_df) a where rnk=1""").drop("rnk")
speciality_df1.registerTempTable("speciality_grp")

val speciality_del_df = sqlContext.sql("select part_state as del_state,specialtycode,createdate from qnxt_db.specialty_del")
speciality_del_df.registerTempTable("speciality_del")

val leftJoin_DF2 = sqlContext.sql("SELECT speciality_grp.*,speciality_del.specialtycode as del_specialtycode FROM speciality_grp left join speciality_del ON speciality_grp.spe_part_state = speciality_del.del_state and speciality_grp.sp_specialtycode = speciality_del.specialtycode and speciality_grp.createdate = speciality_del.createdate")

val SPECIALITY_df = leftJoin_DF2.filter(col("del_specialtycode").isNull).drop(col("createdate")).drop(col("lastupdate")).drop(col("del_specialtycode")).drop(col("spe_part_state")).distinct
SPECIALITY_df.registerTempTable("speciality")
       
val PROVIDER_CA_df1= sqlContext.sql("""SELECT PROVID, A.SPECIALTYCODE, B.DESCRIPTION, PROVTYPE, FEDID, FULLNAME, EXTHOURS, PROFDESIG, STATUS, B.DESCRIPTION AS PROV_SPEC_DESC 
FROM provider A 
INNER JOIN speciality B ON UPPER(A.SPECIALTYCODE)=UPPER(B.SP_SPECIALTYCODE) where spe_stateid='"""+stateKey+"""' and pro_stateid='"""+stateKey+"""'
UNION ALL SELECT PROVID AS PROVID, A.SPECIALTYCODE, B.DESCRIPTION, PROVTYPE, FEDID, FULLNAME, EXTHOURS, PROFDESIG, STATUS , B.DESCRIPTION AS PROV_SPEC_DESC 
FROM provider A 
INNER JOIN speciality B ON UPPER(A.SPECIALTYCODE)=UPPER(B.SP_SPECIALTYCODE) where spe_stateid='MC' and pro_stateid='MC' """)      

val PROVIDER_CA_df=PROVIDER_CA_df1.selectExpr("provid","provid FACILITY_ID","SPECIALTYCODE","DESCRIPTION","PROVTYPE","FEDID","FULLNAME","FULLNAME FACILITYNAME","EXTHOURS","PROFDESIG","STATUS","DESCRIPTION","DESCRIPTION PROV_SPEC_DESC")
			  val rft_mrline_tab_df=sqlContext.table("execdb_db.rft_mrline_tab").selectExpr("CODE_SET_YEAR","MR_LINE","MR_LINE_CODE_AND_DESC HCGDESC")
			  val CCSCODE_df =sqlContext.table("reference_db.ccscode").selectExpr("diagnosiscode","ccs_code")   /* changed the logic and table name for CCSCODE */
			  
			  PROVIDER_CA_df.registerTempTable("PROVIDER_CA")
			  VW_CLAIM_CA_df.registerTempTable("VW_CLAIM_CA")
			  var Max_year_mo=sqlContext.sql("""SELECT MAX(YEAR_MO)-300 FROM VW_CLAIM_CA""").first.get(0)
			  VW_MEMBMTHS_CA_df.registerTempTable("VW_MEMBMTHS_CA")
			  
			  val join_claim_withMRLINE_df=VW_CLAIM_CA_df.join(rft_mrline_tab_df,col("HCG_YEAR")===col("CODE_SET_YEAR") && col("HCG_MR_LINE")===col("MR_LINE"),"leftouter")
			  val Join_with_CCSCODE_df=join_claim_withMRLINE_df.join(CCSCODE_df,col("icd_diag_01")===col("diagnosiscode") && expr("ICDVERSION ='9'"),"leftouter")
			  val join_withProvider_df= Join_with_CCSCODE_df.join(PROVIDER_CA_df,col("FACILITY_ID")===col("PROV_FACILITY") ,"leftouter").filter("pat_id<>'BLANK_PAT_ID' AND amt_allowed<>'0'")
			  val Source1_df=join_withProvider_df.groupBy("YEAR_MO","CLAIM_ID","PAT_ID","STARTDATE","ENDDATE","ICD_DIAG_01","CCS_Code","FACILITYNAME","FACILITY_ID","HCGDESC").agg(expr("sum(MR_ADMITS_CASES_RAW)").alias("RAWADMITS"),expr("sum(AMT_PAID * PAID_COMP_FACTOR)").alias("AMOUNTPAID"),expr("sum(AMT_PAID)").alias("AMOUNTPAID_RAW")).withColumn("STATE",expr("'"+stateKey+"'"))
			  val ReversalClaims2_DF=Source1_df.filter("CLAIM_ID like '%R%'").withColumn("Claim_id_temp",expr("substr(claim_id,1,length(claim_id)-2)")).select("Claim_id_temp")
			  val Source_df=Source1_df.join(ReversalClaims2_DF,col("CLAIM_ID")===col("Claim_id_temp"),"leftouter")
			  val ReversalClaims_DF=Source_df.filter("CLAIM_ID like '%R%' or Claim_id_temp is not null")
			  
			  ReversalClaims_DF.registerTempTable("REVERSALCLAIMS")
 		  			  
			  val dataset_df=Source_df.filter("CLAIM_ID not like '%R%' and Claim_id_temp is null").groupBy("STATE","CLAIM_ID","PAT_ID","CCS_Code","FACILITYNAME","FACILITY_ID","HCGDESC").agg(expr("min(STARTDATE)").alias("STARTDATE"),expr("sum(AMOUNTPAID)").alias("AMOUNTPAID"),expr("sum(AMOUNTPAID_RAW)").alias("AMOUNTPAID_RAW"), expr("MAX(ENDDATE)").alias("ENDDATE"),expr("SUM(RAWADMITS)").alias("RAWADMITS"))
			  
			  val r1_df=dataset_df.withColumn("row",row_number().over(Window.partitionBy("PAT_ID").orderBy("STARTDATE","ENDDATE","AMOUNTPAID","claim_id"))).withColumn("year_mo",expr("concat(cast(year(STARTDATE) as string),case when length(cast(month(STARTDATE) as String))=2 then cast(month(STARTDATE) as String) else concat('0',cast(month(STARTDATE) as String)) end )"))
			  
			  r1_df.registerTempTable("r1")
			  val r22_df=sqlContext.sql("""SELECT B.* ,
CASE 
WHEN (C.STARTDATE IS NULL AND A.ENDDATE>B.ENDDATE) THEN CASE 
WHEN (datediff(B.ENDDATE,A.STARTDATE) <= 1 
AND A.ENDDATE >= B.ENDDATE ) THEN 0 ELSE 1 
END ELSE 
CASE 
WHEN (datediff(C.STARTDATE,B.ENDDATE) <= 1 
AND C.ENDDATE >= B.ENDDATE) THEN 0 ELSE 1 
END END AS ADMIT ,
CASE 
WHEN (datediff(C.STARTDATE,B.ENDDATE) <= 1 
AND C.ENDDATE >= B.ENDDATE) THEN NULL ELSE B.CLAIM_ID 
END AS ADMITCLAIMID ,CAST(
NULL AS FLOAT) AS ADMITAMOUNTPAID ,CAST(
NULL AS FLOAT) AS ADMITAMOUNTPAID_RAW 
FROM R1 B 
LEFT JOIN R1 C ON B.STATE = C.STATE 
AND B.PAT_ID = C.PAT_ID 
AND B.ROW = C.ROW-1 
LEFT JOIN R1 A ON B.STATE = A.STATE 
AND B.PAT_ID = A.PAT_ID 
AND B.ROW = A.ROW+1 
ORDER BY PAT_ID,STARTDATE,ENDDATE,ROW """)//.filter("pat_id='CAL113355025590'")
//r22_df.filter("pat_id='CAL113355025590'")

r22_df.registerTempTable("R22")

val distinct_pat_id=r22_df.filter("ADMITCLAIMID is not null").selectExpr("pat_id new_pat_id","row row_new","ADMITCLAIMID ADMITCLAIMID_new")
val join_withR22=r22_df.join(distinct_pat_id,col("pat_id")===col("new_pat_id") &&  expr("ADMITCLAIMID is null and row_new>row"),"leftouter")
val addCol=join_withR22.withColumn("rn",row_number().over(Window.partitionBy("pat_id","row").orderBy("row_new"))).filter("rn=1")
val R21=addCol.withColumn("ADMITCLAIMID1",coalesce(col("ADMITCLAIMID"),col("ADMITCLAIMID_new"))).drop("new_pat_id").drop("row_new").drop("ADMITCLAIMID_new").drop("ADMITCLAIMID").drop("rn").withColumnRenamed("ADMITCLAIMID1", "ADMITCLAIMID")
//R21.write.mode("overwrite").saveAsTable("eim_datamanagement_db.Medinsight_Readmit_tillLoop")
//val R2=sqlContext.table"eim_datamanagement_db.Medinsight_Readmit_tillLoop")
R21.write.mode("overwrite").parquet("/edl/transform/semantic/data/Medinsight_Readmit_intermediate")

//R2.filter("CLAIM_ID='18068316831'")
//FACILITY_ID='QMP000004136907'
  
  val R2=sqlContext.read.parquet("/edl/transform/semantic/data/Medinsight_Readmit_intermediate")
  R2.registerTempTable("R2")

//R21.registerTempTable("R2")
val R22=  sqlContext.sql("""WITH A AS ( 
SELECT PAT_ID ,ADMITCLAIMID ,SUM(AMOUNTPAID) AS ADMITAMOUNTPAID ,SUM(AMOUNTPAID_RAW) AS ADMITAMOUNTPAID_RAW 
FROM R2 
GROUP BY PAT_ID,ADMITCLAIMID ) 
SELECT R2.STATE,R2.YEAR_MO,R2.PAT_ID,R2.CLAIM_ID,R2.STARTDATE,R2.ENDDATE,R2.CCS_Code,R2.RAWADMITS, R2.FACILITYNAME,R2.FACILITY_ID,R2.HCGDESC ,R2.AMOUNTPAID,R2.AMOUNTPAID_RAW,R2.ROW,R2.ADMIT,R2.ADMITCLAIMID,A.ADMITAMOUNTPAID, A.ADMITAMOUNTPAID_RAW 
FROM R2 R2 
LEFT JOIN A ON R2.PAT_ID = A.PAT_ID 
AND R2.ADMITCLAIMID = A.ADMITCLAIMID 
AND R2.ADMIT = 1""")

R22.registerTempTable("R22")

val R3A_CA=sqlContext.sql(""" SELECT ROW_NUMBER() 
OVER(
PARTITION BY PAT_ID 
ORDER BY STARTDATE,ENDDATE ) AS RN ,* 
FROM R22 
WHERE ADMIT = 1""") 
R3A_CA.registerTempTable("R3A_CA")

val R3B_CA=sqlContext.sql("""SELECT ROW_NUMBER() 
OVER(
PARTITION BY PAT_ID 
ORDER BY STARTDATE,ENDDATE) AS RN ,* 
FROM R22 
WHERE ADMIT = 0""")

R3B_CA.registerTempTable("R3B_CA")

val TEMP_ADMIT_CA=sqlContext.sql("""WITH F AS ( 
SELECT RN,STATE,YEAR_MO,PAT_ID,CLAIM_ID,STARTDATE,ENDDATE,CCS_Code,RAWADMITS,ADMIT,DAYSDIFF ,FACILITYNAME,FACILITY_ID, HCGDESC,AMOUNTPAID,ADMITAMOUNTPAID,ADMITAMOUNTPAID_RAW , 
CASE 
WHEN DAYSDIFF 
BETWEEN 2 
AND 30 THEN 1 ELSE 0 
END AS READMIT 
FROM ( 
SELECT B.* ,datediff(B.STARTDATE,A.ENDDATE) AS DAYSDIFF 
FROM R3A_CA A 
INNER JOIN R3A_CA B ON A.STATE = B.STATE 
AND A.PAT_ID = B.PAT_ID 
AND A.RN = B.RN-1 
UNION ALL SELECT * ,0 AS DAYSDIFF 
FROM R3A_CA 
WHERE RN = 1 )I ) 
SELECT STATE,YEAR_MO AS MONTH,PAT_ID AS MEMBERID,FACILITYNAME,FACILITY_ID,HCGDESC,CLAIM_ID AS CLAIMID,AMOUNTPAID, 
CASE 
WHEN ADMIT = 1 THEN ADMITAMOUNTPAID ELSE 0 
END AS ADMITAMOUNTPAID , 
CASE 
WHEN ADMIT = 1 THEN ADMITAMOUNTPAID_RAW ELSE 0 
END AS ADMITAMOUNTPAID_RAW , STARTDATE,ENDDATE, ADMIT AS ISADMIT , READMIT AS ISREADMIT, ISANCHOR,LOSSDUETOANCHOR, LOSSDUETOANCHOR_RAW ,
CASE 
WHEN READMIT = 1 THEN ADMITAMOUNTPAID ELSE 0 
END AS READMIT_AMT_PAID ,
CASE 
WHEN READMIT = 1 THEN ADMITAMOUNTPAID_RAW ELSE 0 
END AS READMIT_AMT_PAID_RAW ,
CASE 
WHEN ISANCHOR = 1 THEN ADMITAMOUNTPAID ELSE 0 
END AS ANCHOR_AMT_PAID ,
CASE 
WHEN ISANCHOR = 1 THEN ADMITAMOUNTPAID_RAW ELSE 0 
END AS ANCHOR_AMT_PAID_RAW ,ANCHOR_FACILITYNAME,ANCHOR_FACILITY_ID,ANCHOR_CLAIM_ID 
FROM ( 
SELECT F2.* ,
CASE 
WHEN F3.READMIT = 1 THEN 1 ELSE 0 
END AS ISANCHOR ,
CASE 
WHEN F3.READMIT = 1 THEN F3.ADMITAMOUNTPAID ELSE 0 
END AS LOSSDUETOANCHOR ,
CASE 
WHEN F3.READMIT = 1 THEN F3.ADMITAMOUNTPAID_RAW ELSE 0 
END AS LOSSDUETOANCHOR_RAW ,
CASE 
WHEN F2.READMIT = 1 THEN F1.FACILITYNAME ELSE '' END AS ANCHOR_FACILITYNAME ,
CASE 
WHEN F2.READMIT = 1 THEN F1.FACILITY_ID ELSE '' END AS ANCHOR_FACILITY_ID ,
CASE 
WHEN F2.READMIT = 1 THEN F1.CLAIM_ID ELSE '' END AS ANCHOR_CLAIM_ID 
FROM F F2 
LEFT JOIN F F1 ON F2.STATE = F1.STATE 
AND F2.PAT_ID = F1.PAT_ID 
AND F2.RN-1 = F1.RN 
LEFT JOIN F F3 ON F2.STATE = F3.STATE 
AND F2.PAT_ID = F3.PAT_ID 
AND F2.RN+1 = F3.RN 
UNION ALL SELECT RN,STATE,YEAR_MO,PAT_ID,CLAIM_ID,STARTDATE,ENDDATE,CCS_Code,RAWADMITS,ADMIT,0,FACILITYNAME,FACILITY_ID,HCGDESC,AMOUNTPAID ,ADMITAMOUNTPAID,ADMITAMOUNTPAID_RAW,0,0,0,0,'','','' FROM R3B_CA 
UNION ALL SELECT 0,STATE,YEAR_MO,PAT_ID,CLAIM_ID,STARTDATE,ENDDATE,CCS_Code,RAWADMITS,0,0,FACILITYNAME,FACILITY_ID,HCGDESC,AMOUNTPAID ,AMOUNTPAID,AMOUNTPAID_RAW,0,0,0,0,'','','' FROM REVERSALCLAIMS )I """)

TEMP_ADMIT_CA.write.mode("overwrite").parquet("/edl/transform/semantic/data/Medinsight_Readmit_intermediate1")

//TEMP_ADMIT_CA.write.mode("overwrite").saveAsTable("eim_datamanagement_db.Medinsight_Readmit_Temp_admit")

//val TEMP_ADMIT_CA1=sqlContext.table("eim_datamanagement_db.Medinsight_Readmit_Temp_admit")//.filter("MEMBERID='CAL113355025590'")
//TEMP_ADMIT_CA.filter("MEMBERID='CAL171820065541' and claimid='18068316831' ")
  
val TEMP_ADMIT_CA1=sqlContext.read.parquet("/edl/transform/semantic/data/Medinsight_Readmit_intermediate1")

//TEMP_ADMIT_CA1.write.mode("overwrite").saveAsTable("semantic_db.TEMP_ADMIT_CA")

TEMP_ADMIT_CA1.registerTempTable("TEMP_ADMIT_CA")

val NDCCODE=sqlContext.table("execdb_db.NDCCODE")
NDCCODE.registerTempTable("NDCCODE")

val product_ca=sqlContext.sql("""SELECT PLANID,EFFDATE,PRODUCTNAME,STATE,AIDCODE 
FROM execdb_db.PRODUCT 
WHERE STATE = '"""+stateKey+"""'""")

product_ca.registerTempTable("product_ca")

val temp_ca_df = sqlContext.sql(""" SELECT DISTINCT CLAIM_ID,'Yes' AS ISEMERGENCY 
FROM VW_CLAIM_CA 
WHERE REV_CODE 
IN ('0450','0451','0452','0453','0454','0455','0456','0457','0458','0459') 
""")

temp_ca_df.registerTempTable("TEMP_CA")

val TEMP_REV_CA_df=sqlContext.sql("""SELECT CLAIM_ID,MAX(PROC_CODE) AS SEVERITY 
FROM VW_CLAIM_CA 
WHERE PROC_CODE 
IN ('99281','99282', '99283','99284', '99285') 
GROUP BY CLAIM_ID """)

TEMP_REV_CA_df.registerTempTable("TEMP_REV_CA")

val PCPSPECIALITY_CA_df=sqlContext.sql("""SELECT PROVID, SPECIALTYSTATUS,SPECIALTYCODE, DESCRIPTION, EFFDATE, TERMDATE 
FROM ( 
SELECT PROVID, SPECIALTYSTATUS,SP_SPECIALTYCODE SPECIALTYCODE, DESCRIPTION, EFFDATE, TERMDATE , ROW_NUMBER() 
OVER (
PARTITION BY PROVID 
ORDER BY SP_SPECIALTYCODE) AS RNK 
FROM ( 
SELECT A.PROVID, A.SPECIALTYSTATUS, B.SP_SPECIALTYCODE, B.DESCRIPTION, EFFDATE, TERMDATE 
FROM speciality B 
INNER JOIN qnxt_db.PROVSPECIALTY A ON UPPER(A.SPECIALTYCODE) = UPPER(B.SP_SPECIALTYCODE) and B.spe_stateid='"""+stateKey+"""'
WHERE UPPER(A.SPECTYPE) = 'PRIMARY' and B.spe_stateid='"""+stateKey+"""' UNION All SELECT coalesce(A.PROVID,'M') AS PROVID, A.SPECIALTYSTATUS, B.SP_SPECIALTYCODE, B.DESCRIPTION, EFFDATE, TERMDATE 
FROM speciality B 
INNER JOIN qnxt_db.PROVSPECIALTY A ON UPPER(A.SPECIALTYCODE) = UPPER(B.SP_SPECIALTYCODE) and B.spe_stateid='MC' 
WHERE UPPER(A.SPECTYPE) = 'PRIMARY' and B.spe_stateid='MC' ) A ) B 
WHERE RNK=1 """) 

PCPSPECIALITY_CA_df.registerTempTable("PCPSPECIALITY_CA")

val TEMP_IPADMITS_CA_df =sqlContext.sql("""SELECT CLAIM_ID ,
CASE 
WHEN SUM(MR_UNITS_DAYS_RAW) =0 THEN '0' WHEN SUM(MR_UNITS_DAYS_RAW) =1 THEN '01' WHEN SUM(MR_UNITS_DAYS_RAW) 
BETWEEN 2 
AND 3 THEN '02-03' WHEN SUM(MR_UNITS_DAYS_RAW) 
BETWEEN 4 
AND 5 THEN '04-05' WHEN SUM(MR_UNITS_DAYS_RAW) 
BETWEEN 6 
AND 10 THEN '06-10' WHEN SUM(MR_UNITS_DAYS_RAW) 
BETWEEN 11 
AND 15 THEN '11-15' WHEN SUM(MR_UNITS_DAYS_RAW) 
BETWEEN 16 
AND 30 THEN '16-30' WHEN SUM(MR_UNITS_DAYS_RAW) 
BETWEEN 31 
AND 90 THEN '31-90' WHEN SUM(MR_UNITS_DAYS_RAW) >=91 THEN '91+' ELSE 'NA' END AS IPADMITDISTRIBUTION 
FROM VW_CLAIM_CA AA 
WHERE SUBSTR(AA.HCG_MR_LINE,1,1) ='I' AND SUBSTRING(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
and AA.YEAR_MO> '"""+Max_year_mo+"""' 
GROUP BY CLAIM_ID """)

TEMP_IPADMITS_CA_df.registerTempTable("TEMP_IPADMITS_CA")
var HCG_YEAR= sqlContext.sql("SELECT MAX(HCG_YEAR) FROM VW_CLAIM_CA").first().get(0)

val join_df=sqlContext.sql("""Select 
AA.YEAR_MO , '"""+stateKey+"""' AS STATE , 
CASE 
WHEN AA.LOB_GL='' THEN 'Undefined' ELSE NVL(AA.LOB_GL,'Undefined') 
END AS JDE_LOB , 
CASE 
WHEN AA.PROD_GL='' THEN 'Undefined' ELSE NVL(AA.PROD_GL,'') 
END AS JDE_PRODUCT, 
CASE 
WHEN NVL(MPR.MEMBERREGION, AA.REGION,'') ='' THEN SUBSTR( 
CASE 
WHEN NVL(AA.MEM_COUNTY,'')='' THEN 'OTHER, """+stateCode(stateKey)+"""' ELSE concat(UPPER(TRIM(AA.MEM_COUNTY)),', """+stateCode(stateKey)+"""') END , 1 ,
locate(',' , CASE 
WHEN NVL(AA.MEM_COUNTY,'')='' THEN 'OTHER, """+stateCode(stateKey)+"""' ELSE concat(UPPER(TRIM(AA.MEM_COUNTY)),', """+stateCode(stateKey)+"""') END ) -1 ) ELSE (NVL(MPR.MEMBERREGION, AA.REGION)) 
END AS MEMBERREGION, 
AA.REGION, 
CASE WHEN NVL(AA.MEM_COUNTY,'')='' THEN 'OTHER, """+stateCode(stateKey)+"""' ELSE concat(UPPER(TRIM(AA.MEM_COUNTY)),', """+stateCode(stateKey)+"""') END  MEM_COUNTY , 
AA.PAT_ID AS PAT_ID, AA.CLAIM_ID AS CLAIM_ID, 
MR.MR_LINE , MR.MR_LINE_CODE_AND_DESC, 
NVL(AA.HCG_YEAR,'"""+HCG_YEAR+"""') AS HCG_YEAR , 
MR.MR_LINE_DESC2 AS HCG_LINE, MR.HCG_DESC_02 AS HCG_SETTING, MR.MR_LINE_DESC3 AS HCG_DETAIL, MR.HCG_SUPERGROUPER, AA.CL_TYPE , 
CASE 
WHEN BB.AGE 
BETWEEN 0 
AND 18 THEN '00-18' WHEN BB.AGE 
BETWEEN 19 
AND 24 THEN '19-24' WHEN BB.AGE 
BETWEEN 25 
AND 34 THEN '25-34' WHEN BB.AGE 
BETWEEN 35 
AND 54 THEN '35-54' WHEN BB.AGE 
BETWEEN 55 
AND 64 THEN '55-64' WHEN BB.AGE >=65 THEN '65+' ELSE 'Unknown' END AS AGE_CAT , 
CASE 
WHEN NVL(BB.DUAL_TYPE,'')='' OR TRIM(BB.DUAL_TYPE) = 'N/A' THEN 'NA' ELSE BB.DUAL_TYPE 
END AS DUAL_TYPE , 
CASE 
WHEN NVL(BB.DELIVERY_SYSTEM,'')='' THEN 'NA' ELSE BB.DELIVERY_SYSTEM 
END AS DELIVERY_SYSTEM , 
CASE 
WHEN ICD_DIAG_01 =' ' THEN 'Dx Code NA' WHEN aa.ICDVERSION='9' THEN ICD_DIAG_01 
WHEN aa.ICDVERSION='0' THEN NVL(B3.ICD9CODE,'ICD9 Code NA') else 'NA' END as PRIMARY_DIAGNOSIS_CODE, 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') THEN CAST(BB.CCHG_CAT AS INT) 
END AS CCHG, CCHG.CCHG_CAT_CODE_AND_DESC AS CCHG_DESCRIPTION, 
CASE WHEN NVL(NVL(H1.CCS_Code,H2.CCS_Code),'')='' THEN 'NA' ELSE NVL(H1.CCS_Code,H2.CCS_Code) END AS CCS_CODE , 
CASE WHEN AA.YEAR_MO<'201510' THEN CASE WHEN NVL(H1.CCS_Code_Desc,'')='' THEN B3.CCSDESC ELSE H1.CCS_Code_Desc END WHEN AA.YEAR_MO>='201510' THEN B3.CCSDESC ELSE 'NA' END AS CCS_CODE_DESCRIPTION, 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') THEN PROV_ATT ELSE 'NA' END AS PROVIDER , P.FULLNAME AS PROVIDER_NAME, NVL(P.PROV_SPEC_DESC,'NA') AS PROVIDERSPECIALTY , AA.PROV_PCP AS PCP, NVL(TRIM(PP.FULLNAME),'NA') AS PCPNAME, AA.PROV_PCP_IPA AS PAY_TO_PCP, P2.FULLNAME AS PAY_TO_PCPNAME, SPEC.SPECIALTYCODE AS PCP_SPECIALTY_CODE, NVL(SPEC.DESCRIPTION,'NA') AS PCP_SPECIALTY , 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') THEN NVL(B1.ISEMERGENCY,'No') ELSE 'NA' END AS ISEMERGENCY , 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') THEN B2.SEVERITY ELSE 'NA' END AS ER_SEVERITY , 
CASE 
WHEN B2.SEVERITY='99281' THEN 'ER Severity Level - 1' WHEN B2.SEVERITY='99282' THEN 'ER Severity Level - 2' WHEN B2.SEVERITY='99283' THEN 'ER Severity Level - 3' WHEN B2.SEVERITY='99284' THEN 'ER Severity Level - 4' WHEN B2.SEVERITY='99285' THEN 'ER Severity Level - 5' Else 
NULL END AS ER_Severity_Level, 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND UPPER(AA.CLAIM_IN_NETWORK)='Y' THEN 'Yes' WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND UPPER(AA.CLAIM_IN_NETWORK) <> 'Y' THEN 'No' ELSE 'NA' END AS ISINNETWORK , 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND SUBSTR(AA.HCG_MR_LINE,1,1) ='I' AND AMT_PAID >100000 THEN 'Yes' WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND SUBSTR(AA.HCG_MR_LINE,1,1) <>'I' AND AMT_PAID >10000 THEN 'Yes' WHEN SUBSTR(AA.YEAR_MO,1,4) 
NOT IN ('2017','2019','2018','2015','2016') THEN 'NA' ELSE 'No' END ISCATASTROPHIC , NVL(F.IPADMITDISTRIBUTION,'NA') AS IPADMITDISTRIBUTION , 
CASE 
WHEN NVL(BB.MEM_GENDER,'')='' OR TRIM(BB.MEM_GENDER) = 'U' THEN 'Unknown' ELSE BB.MEM_GENDER 
END AS MEM_GENDER , BB.AGE , 
Case 
when G.ISADMIT='1' then 'Y' when G.ISADMIT='0' then 'N' When (G.ISADMIT 
is null and Substring(MR.MR_LINE,1,1)='I' and MR.MR_LINE <> 'I31') then 'N' Else 'NA' End as ISADMIT, 
Case 
when G.ISREADMIT='1' then 'Y' when G.ISREADMIT='0' then 'N' When (G.ISREADMIT 
is null and Substring(MR.MR_LINE,1,1)='I' and MR.MR_LINE <> 'I31') then 'N' Else 'NA' End as ISREADMIT, 
Case 
when G.ISANCHOR='1' then 'Y' when G.ISANCHOR='0' then 'N' When (G.ISANCHOR 
is null and Substring(MR.MR_LINE,1,1)='I' and MR.MR_LINE <> 'I31') then 'N' Else 'NA' End as ISANCHOR, 
SUM(ADMITAMOUNTPAID) AS ADMIT_AMT_PAID, SUM(ADMITAMOUNTPAID_RAW) AS ADMIT_AMT_PAID_RAW, 
SUM(READMIT_AMT_PAID) AS READMIT_AMT_PAID, SUM(READMIT_AMT_PAID_RAW) AS READMIT_AMT_PAID_RAW, SUM(ANCHOR_AMT_PAID) AS ANCHOR_AMT_PAID, 
SUM(ANCHOR_AMT_PAID_RAW) AS ANCHOR_AMT_PAID_RAW, SUM(1) AS CLAIMCOUNT, 
SUM(1*PAID_COMP_FACTOR) AS CLAIM_COUNT_RAW, SUM(LOSSDUETOANCHOR) AS LOSSDUETOANCHOR , SUM(LOSSDUETOANCHOR_RAW) AS LOSSDUETOANCHOR_RAW , 
SUM(AA.RX_REBATE) AS RX_REBATE , SUM(AA.TRANSACTION_FEE) AS RX_TRANS_FEE , 
G.STARTDATE, G.ENDDATE, 'ODS_USER' CREATEID , CURRENT_TIMESTAMP CREATEDATE,AA.PLANID, AA.RATECODE,AA.ICDVERSION,AA.ICD_DIAG_01,G.ANCHOR_FACILITYNAME,G.ANCHOR_FACILITY_ID,
G.ANCHOR_CLAIM_ID,AA.MOLINARISK,AA.REV_CODE, AA.PROC_CODE,AA.DRG_APR,AA.RATE_AID_CODE , 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND SUBSTR(AA.HCG_MR_LINE,1,1) ='I' THEN MEDINSIGHT_MS_DRG ELSE 'NA' END AS MEDINSIGHT_MS_DRG   
from
TEMP_ADMIT_CA G
LEFT JOIN VW_CLAIM_CA AA ON TRIM(AA.CLAIM_ID)=TRIM(G.CLAIMID) AND AA.SV_LINE='1' 
LEFT JOIN TEMP_CA AS B1 ON AA.CLAIM_ID=B1.CLAIM_ID
LEFT JOIN TEMP_REV_CA AS B2 ON AA.CLAIM_ID=B2.CLAIM_ID
LEFT JOIN VW_MEMBMTHS_CA BB ON BB.YEAR_MO= AA.YEAR_MO AND BB.PAT_ID = AA.PAT_ID AND BB.MEMBER_QUAL=AA.MEMBER_QUAL
LEFT JOIN execdb_db.rft_mrline_tab MR ON MR.MR_LINE = AA.HCG_MR_LINE AND MR.CODE_SET_YEAR = AA.HCG_YEAR
LEFT JOIN NDCCODE NDC ON AA.RX_NDC=NDC.ndc_number
LEFT JOIN NDCCODE NDC1 ON AA.NDC_SUB =NDC1.ndc_number
LEFT JOIN PROVIDER_CA PP ON UPPER(AA.PROV_PCP) = UPPER(PP.PROVID)
AND SUBSTR(AA.YEAR_MO,1,4) IN ('2019','2018','2015','2016','2017')
LEFT JOIN PROVIDER_CA P ON UPPER(AA.PROV_ATT) = UPPER(P.PROVID) AND
SUBSTR(AA.YEAR_MO,1,4) IN ('2017','2019','2018','2015','2016')
LEFT JOIN PROVIDER_CA P2 ON UPPER(AA.PROV_PCP_IPA) = UPPER(P2.PROVID) AND SUBSTR(AA.YEAR_MO,1,4) IN ('2017','2019','2018','2015','2016')
LEFT JOIN PCPSPECIALITY_CA AS SPEC ON AA.PROV_PCP=SPEC.PROVID
LEFT JOIN TEMP_IPADMITS_CA F ON AA.CLAIM_ID=F.CLAIM_ID
LEFT JOIN eim_datamanagement_db.CCSCODE H1 ON H1.DiagnosisCode = AA.ICD_DIAG_01 AND H1.ICDVERSION = '9' AND AA.ICDVERSION = '9'
LEFT JOIN eim_datamanagement_db.CCSCODE H2 ON regexp_replace(H2.DiagnosisCode,'\\.','') = regexp_replace(AA.ICD_DIAG_01,'\\.','')
AND H2.ICDVERSION = '0' AND AA.ICDVERSION = '0'
LEFT JOIN eim_datamanagement_db.ICD10TOICD9 B3 ON TRANSLATE(ICD_DIAG_01,'.','') = B3.ICD10CODE
LEFT JOIN eim_datamanagement_db.MEDINSIGHTREGION MPR ON UPPER(MPR.STATE)='"""+stateKey+"""' AND UPPER(MPR.MEDINSIGHTREGION) = UPPER(AA.REGION)
LEFT JOIN eim_datamanagement_db.MEDINSIGHT_GLPRODUCT PROD ON AA.PROD_GL=PROD.PROD_GL AND PROD.STATE='"""+stateKey+"""'
LEFT JOIN medinsight_db.RFT_CCHG CCHG ON CAST(BB.CCHG_CAT AS INT) = CCHG.CCHG_CAT and CCHG.part_state='"""+stateKey+"""'
WHERE 
AA.YEAR_MO> '"""+Max_year_mo+"""' AND 
G.ISADMIT = '1'
group by
AA.YEAR_MO , '"""+stateKey+"""', 
CASE 
WHEN AA.LOB_GL='' THEN 'Undefined' ELSE NVL(AA.LOB_GL,'Undefined') 
END, 
CASE 
WHEN AA.PROD_GL='' THEN 'Undefined' ELSE NVL(AA.PROD_GL,'') 
END , 
CASE 
WHEN NVL(MPR.MEMBERREGION, AA.REGION,'') ='' THEN SUBSTR( 
CASE 
WHEN NVL(AA.MEM_COUNTY,'')='' THEN 'OTHER, """+stateCode(stateKey)+"""' ELSE concat(UPPER(TRIM(AA.MEM_COUNTY)),', """+stateCode(stateKey)+"""') END , 1 ,
locate(',' , CASE 
WHEN NVL(AA.MEM_COUNTY,'')='' THEN 'OTHER, """+stateCode(stateKey)+"""' ELSE concat(UPPER(TRIM(AA.MEM_COUNTY)),', """+stateCode(stateKey)+"""') END ) -1 ) ELSE (NVL(MPR.MEMBERREGION, AA.REGION)) 
END , 
AA.REGION, 
CASE WHEN NVL(AA.MEM_COUNTY,'')='' THEN 'OTHER, """+stateCode(stateKey)+"""' ELSE concat(UPPER(TRIM(AA.MEM_COUNTY)),', """+stateCode(stateKey)+"""') END, 
AA.PAT_ID, AA.CLAIM_ID, 
MR.MR_LINE , MR.MR_LINE_CODE_AND_DESC, 
NVL(AA.HCG_YEAR,'"""+HCG_YEAR+"""'), 
MR.MR_LINE_DESC2, MR.HCG_DESC_02, MR.MR_LINE_DESC3, MR.HCG_SUPERGROUPER, AA.CL_TYPE , 
CASE 
WHEN BB.AGE 
BETWEEN 0 
AND 18 THEN '00-18' WHEN BB.AGE 
BETWEEN 19 
AND 24 THEN '19-24' WHEN BB.AGE 
BETWEEN 25 
AND 34 THEN '25-34' WHEN BB.AGE 
BETWEEN 35 
AND 54 THEN '35-54' WHEN BB.AGE 
BETWEEN 55 
AND 64 THEN '55-64' WHEN BB.AGE >=65 THEN '65+' ELSE 'Unknown' END , 
CASE 
WHEN NVL(BB.DUAL_TYPE,'')='' OR TRIM(BB.DUAL_TYPE) = 'N/A' THEN 'NA' ELSE BB.DUAL_TYPE 
END, 
CASE 
WHEN NVL(BB.DELIVERY_SYSTEM,'')='' THEN 'NA' ELSE BB.DELIVERY_SYSTEM 
END, 
CASE 
WHEN ICD_DIAG_01 =' ' THEN 'Dx Code NA' WHEN aa.ICDVERSION='9' THEN ICD_DIAG_01 
WHEN aa.ICDVERSION='0' THEN NVL(B3.ICD9CODE,'ICD9 Code NA') else 'NA' END, 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') THEN CAST(BB.CCHG_CAT AS INT) 
END, CCHG.CCHG_CAT_CODE_AND_DESC, 
CASE WHEN NVL(NVL(H1.CCS_Code,H2.CCS_Code),'')='' THEN 'NA' ELSE NVL(H1.CCS_Code,H2.CCS_Code) END, 
CASE WHEN AA.YEAR_MO<'201510' THEN CASE WHEN NVL(H1.CCS_Code_Desc,'')='' THEN B3.CCSDESC ELSE H1.CCS_Code_Desc END WHEN AA.YEAR_MO>='201510' THEN B3.CCSDESC ELSE 'NA' END, 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') THEN PROV_ATT ELSE 'NA' END , P.FULLNAME, NVL(P.PROV_SPEC_DESC,'NA')  , AA.PROV_PCP , NVL(TRIM(PP.FULLNAME),'NA') , AA.PROV_PCP_IPA , P2.FULLNAME , SPEC.SPECIALTYCODE, NVL(SPEC.DESCRIPTION,'NA'), 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') THEN NVL(B1.ISEMERGENCY,'No') ELSE 'NA' END , 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') THEN B2.SEVERITY ELSE 'NA' END , 
CASE 
WHEN B2.SEVERITY='99281' THEN 'ER Severity Level - 1' WHEN B2.SEVERITY='99282' THEN 'ER Severity Level - 2' WHEN B2.SEVERITY='99283' THEN 'ER Severity Level - 3' WHEN B2.SEVERITY='99284' THEN 'ER Severity Level - 4' WHEN B2.SEVERITY='99285' THEN 'ER Severity Level - 5' Else 
NULL END , 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND UPPER(AA.CLAIM_IN_NETWORK)='Y' THEN 'Yes' WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND UPPER(AA.CLAIM_IN_NETWORK) <> 'Y' THEN 'No' ELSE 'NA' END , 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND SUBSTR(AA.HCG_MR_LINE,1,1) ='I' AND AMT_PAID >100000 THEN 'Yes' WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND SUBSTR(AA.HCG_MR_LINE,1,1) <>'I' AND AMT_PAID >10000 THEN 'Yes' WHEN SUBSTR(AA.YEAR_MO,1,4) 
NOT IN ('2017','2019','2018','2015','2016') THEN 'NA' ELSE 'No' END , NVL(F.IPADMITDISTRIBUTION,'NA') , 
CASE 
WHEN NVL(BB.MEM_GENDER,'')='' OR TRIM(BB.MEM_GENDER) = 'U' THEN 'Unknown' ELSE BB.MEM_GENDER 
END , BB.AGE , 
Case 
when G.ISADMIT='1' then 'Y' when G.ISADMIT='0' then 'N' When (G.ISADMIT 
is null and Substring(MR.MR_LINE,1,1)='I' and MR.MR_LINE <> 'I31') then 'N' Else 'NA' End, 
Case 
when G.ISREADMIT='1' then 'Y' when G.ISREADMIT='0' then 'N' When (G.ISREADMIT 
is null and Substring(MR.MR_LINE,1,1)='I' and MR.MR_LINE <> 'I31') then 'N' Else 'NA' End , 
Case 
when G.ISANCHOR='1' then 'Y' when G.ISANCHOR='0' then 'N' When (G.ISANCHOR 
is null and Substring(MR.MR_LINE,1,1)='I' and MR.MR_LINE <> 'I31') then 'N' Else 'NA' End , 
G.STARTDATE, G.ENDDATE, 'ODS_USER' , CURRENT_TIMESTAMP ,AA.PLANID, AA.RATECODE,AA.ICDVERSION,AA.ICD_DIAG_01,G.ANCHOR_FACILITYNAME,G.ANCHOR_FACILITY_ID,
G.ANCHOR_CLAIM_ID,AA.MOLINARISK,AA.REV_CODE, AA.PROC_CODE,AA.DRG_APR,AA.RATE_AID_CODE , 
CASE 
WHEN SUBSTR(AA.YEAR_MO,1,4) 
IN ('2017','2019','2018','2015','2016') 
AND SUBSTR(AA.HCG_MR_LINE,1,1) ='I' THEN MEDINSIGHT_MS_DRG ELSE 'NA' END"""    
)
join_df.registerTempTable("medinsight_readmit")
val Readmit_df=sqlContext.sql("select YEAR_MO YEAR_MONTH, STATE , JDE_LOB , JDE_PRODUCT , MEMBERREGION , REGION , MEM_COUNTY , PAT_ID , CLAIM_ID , MR_LINE , MR_LINE_CODE_AND_DESC , HCG_YEAR , HCG_LINE , HCG_SETTING , HCG_DETAIL , HCG_SUPERGROUPER , CL_TYPE , AGE_CAT , DUAL_TYPE , DELIVERY_SYSTEM , PRIMARY_DIAGNOSIS_CODE , CCHG , CCHG_DESCRIPTION , CCS_CODE , CCS_CODE_DESCRIPTION , PROVIDER , PROVIDER_NAME ,PROVIDERSPECIALTY PROVIDER_SPECIALTY , PCP , PCPNAME , PAY_TO_PCP , PAY_TO_PCPNAME , PCP_SPECIALTY_CODE , PCP_SPECIALTY ,ISEMERGENCY IS_EMERGENCY , ER_SEVERITY , ER_SEVERITY_LEVEL ,ISINNETWORK IS_INNETWORK ,ISCATASTROPHIC IS_CATASTROPHIC ,IPADMITDISTRIBUTION IP_ADMITDISTRIBUTION ,MEM_GENDER GENDER , AGE , ISADMIT , ISREADMIT , ISANCHOR , ADMIT_AMT_PAID , ADMIT_AMT_PAID_RAW ,READMIT_AMT_PAID , READMIT_AMT_PAID_RAW , ANCHOR_AMT_PAID , ANCHOR_AMT_PAID_RAW , CLAIM_COUNT_RAW CLAIM_COUNT,CLAIMCOUNT CLAIM_COUNT_RAW ,LOSSDUETOANCHOR LOSS_DUE_TO_ANCHOR ,LOSSDUETOANCHOR_RAW LOSS_DUE_TO_ANCHOR_RAW , RX_REBATE , RX_TRANS_FEE , STARTDATE , ENDDATE , CREATEID , CREATEDATE,PLANID PLAN_ID, RATECODE ,ICDVERSION,ICD_DIAG_01,ANCHOR_FACILITYNAME,ANCHOR_FACILITY_ID,ANCHOR_CLAIM_ID,MOLINARISK MOLINA_RISK,REV_CODE, PROC_CODE, DRG_APR,RATE_AID_CODE,MEDINSIGHT_MS_DRG from medinsight_readmit")

val DIAG=sqlContext.sql("""select DISTINCT upper(aa.PRIMARY_DIAGNOSIS_CODE) as PRIMARY_DIAGNOSIS_CODE,max(D.Description) as Description,max(E.ICD9Desc) as ICD9Desc 
FROM medinsight_readmit AA 
LEFT JOIN eim_datamanagement_db.DIAGCODE D ON UPPER(AA.PRIMARY_DIAGNOSIS_CODE)= UPPER(D.CODEID) 
LEFT JOIN eim_datamanagement_db.ICD10TOICD9 E ON AA.PRIMARY_DIAGNOSIS_CODE =E.ICD9Code 
where AA.STATE='"""+stateKey+"""' group by upper(aa.PRIMARY_DIAGNOSIS_CODE)""")

DIAG.registerTempTable("DIAG")

val PRIMARY_DIAGNOSIS_DESCRIPTION_1=sqlContext.sql("""select A.*,CASE 
WHEN A.PRIMARY_DIAGNOSIS_CODE = 'Dx Code NA' then 'Dx Code NA' WHEN A.PRIMARY_DIAGNOSIS_CODE = 'ICD9 Code NA' then 'ICD9 Code NA' else nvl(S.Description,S.ICD9Desc) 
end PRIMARY_DIAGNOSIS_DESCRIPTION_1 from  medinsight_readmit A 
left join DIAG S on UPPER(S.PRIMARY_DIAGNOSIS_CODE) = UPPER(A.PRIMARY_DIAGNOSIS_CODE) 
where A.STATE = '"""+stateKey+"""'""").drop("PRIMARY_DIAGNOSIS_DESCRIPTION_1").withColumnRenamed("PRIMARY_DIAGNOSIS_DESCRIPTION_1", "PRIMARY_DIAGNOSIS_DESCRIPTION")

val final_df=PRIMARY_DIAGNOSIS_DESCRIPTION_1.withColumn("MEMBERREGION_temp",expr("""case 
when UPPER(MEMBERREGION)='XX' then 'Undefined' when UPPER(MEMBERREGION)='' then 'Undefined' when UPPER(MEMBERREGION)='UNKNOWN' then 'Undefined' when UPPER(MEMBERREGION) 
is null then 'Undefined' when UPPER(MEMBERREGION) 
in ('HEALTHY FAMILIES', 'MMO', 'MMOP', 'MMP MEDICARE') then 'Undefined' when UPPER(MEMBERREGION)='IMPERIAL' then 'IMP' when UPPER(MEMBERREGION)='LOS ANGELES' then 'LA' when UPPER(MEMBERREGION)='RIVERSIDE' then 'RIV' when UPPER(MEMBERREGION)='SACRAMENTO' then 'SAC' when UPPER(MEMBERREGION)='SAN BERNARDINO' then 'SB' Else UPPER(MEMBERREGION) end""")).drop("MEMBERREGION").withColumnRenamed("MEMBERREGION_temp","MEMBERREGION")

final_df.withColumn("part_state",expr("'"+stateKey+"'")).write.mode("overwrite").partitionBy("part_state").insertInto("semantic_db_stg.medinsight_readmit")
//		}
	  
	} 
}