--Get NCQA BCS CCS PPC
create table  eim_qa_db.OH_NCQA_QSI_Member_Detail
as
 
WITH CTE_OH_NCQA_BCS_Member_Detail AS
( 
	 SELECT 
	 a.MemberKey,
	a.MemberName,
	a.DateOfBirth,
	a.MemberID,
	a.MeasureID,
	a.MeasureKey,
	a.SubMeasureID,
	a.Submeasurekey,
	a.SubMeasureSort,
	a.MeasureJoiner,
	a.DENOMCNT,
	a.NUMERCNT,
	a.NUMERCNT_TYPE,
	a.NONCOMPLIANT,
	a.SPNUMCNT,
	a.SUPPDENOM,
	a.SUPPNUMER,
	a.PCP_ProviderKey,
	a.FlowchartRunID,
	a.DATEKEY,
	a.rundatetime,
	a.FLOWCHARTRUNNAME,
	a.AGE_DOS,
	a.DateOfService,
	a.SupplementalType,
	a.DOS_Year_Month_Number
 ,STRLEFT(cast(replace(RUNDATETIME,'-','') as VARCHAR(6)),6)  as Runmonth
 , CASE 
			WHEN Age_DOS >= 71 THEN '71+'
			WHEN Age_DOS >= 61 THEN '61-70'
			WHEN Age_DOS >= 51 THEN '51-60'
			WHEN Age_DOS >= 41 THEN '41-50'
			WHEN Age_DOS >= 31 THEN '31-40'
			WHEN Age_DOS >= 21 THEN '21-30'
			WHEN Age_DOS >= 0  THEN '0-20'
			ELSE NULL END  AS Age_Cat
,CASE 
			WHEN Age_DOS >= 71 THEN 7
			WHEN Age_DOS >= 61 THEN 6
			WHEN Age_DOS >= 51 THEN 5
			WHEN Age_DOS >= 41 THEN 4
			WHEN Age_DOS >= 31 THEN 3
			WHEN Age_DOS >= 21 THEN 2
			WHEN Age_DOS >= 0  THEN 1
			ELSE NULL END  AS Age_Cat_Sort
 
 FROM eim_qa_db.OH_NCQA_BCS_Member_Detail AS a
) 
,CTE_OH_NCQA_CCS_Member_Detail as
(

 SELECT 
 a.MemberKey,
a.MemberName,
a.DateOfBirth,
a.MemberID,
a.MeasureID,
a.MeasureKey,
a.SubMeasureID,
a.Submeasurekey,
a.SubMeasureSort,
a.MeasureJoiner,
a.DENOMCNT,
a.NUMERCNT,
a.NUMERCNT_TYPE,
a.NONCOMPLIANT,
a.SPNUMCNT,
a.SUPPDENOM,
a.SUPPNUMER,
a.PCP_ProviderKey,
a.FlowchartRunID,
a.DATEKEY,
a.rundatetime,
a.FLOWCHARTRUNNAME,
a.AGE_DOS,
a.DateOfService,
a.SupplementalType,
a.DOS_Year_Month_Number
 ,STRLEFT(cast(replace(RUNDATETIME,'-','') as VARCHAR(6)),6)  as Runmonth
 , CASE 
			WHEN Age_DOS >= 71 THEN '71+'
			WHEN Age_DOS >= 61 THEN '61-70'
			WHEN Age_DOS >= 51 THEN '51-60'
			WHEN Age_DOS >= 41 THEN '41-50'
			WHEN Age_DOS >= 31 THEN '31-40'
			WHEN Age_DOS >= 21 THEN '21-30'
			WHEN Age_DOS >= 0  THEN '0-20'
			ELSE NULL END  aS Age_Cat
,CASE 
			WHEN Age_DOS >= 71 THEN 7
			WHEN Age_DOS >= 61 THEN 6
			WHEN Age_DOS >= 51 THEN 5
			WHEN Age_DOS >= 41 THEN 4
			WHEN Age_DOS >= 31 THEN 3
			WHEN Age_DOS >= 21 THEN 2
			WHEN Age_DOS >= 0  THEN 1
			ELSE NULL END  as Age_Cat_Sort
 FROM eim_qa_db.OH_NCQA_CCS_Member_Detail AS a
) 
,CTE_OH_NCQA_PPC_Member_Detail AS
(
 
 SELECT 
 a.MemberKey,
a.MemberName,
a.DateOfBirth,
a.MemberID,
a.MeasureID,
a.MeasureKey,
a.SubMeasureID,
a.Submeasurekey,
a.SubMeasureSort,
a.MeasureJoiner,
a.DENOMCNT,
a.NUMERCNT,
a.NUMERCNT_TYPE,
a.NONCOMPLIANT,
a.SPNUMCNT,
a.SUPPDENOM,
a.SUPPNUMER,
a.PCP_ProviderKey,
a.FlowchartRunID,
a.DATEKEY,
a.rundatetime,
a.FLOWCHARTRUNNAME,
a.AGE_DOS,
a.DateOfService,
a.SupplementalType,
a.DOS_Year_Month_Number
 ,STRLEFT(cast(replace(RUNDATETIME,'-','') as VARCHAR(6)),6)  as Runmonth
 ,CASE 
			WHEN Age_DOS >= 71 THEN '71+'
			WHEN Age_DOS >= 61 THEN '61-70'
			WHEN Age_DOS >= 51 THEN '51-60'
			WHEN Age_DOS >= 41 THEN '41-50'
			WHEN Age_DOS >= 31 THEN '31-40'
			WHEN Age_DOS >= 21 THEN '21-30'
			WHEN Age_DOS >= 0  THEN '0-20'
			ELSE NULL END  as Age_Cat
,CASE 
			WHEN Age_DOS >= 71 THEN 7
			WHEN Age_DOS >= 61 THEN 6
			WHEN Age_DOS >= 51 THEN 5
			WHEN Age_DOS >= 41 THEN 4
			WHEN Age_DOS >= 31 THEN 3
			WHEN Age_DOS >= 21 THEN 2
			WHEN Age_DOS >= 0  THEN 1
			ELSE NULL END  as Age_Cat_Sort
 FROM eim_qa_db.OH_NCQA_PPC_Member_Detail AS a
)  
select * from(
select MemberKey,MemberName,DateOfBirth,MemberID,MeasureID,MeasureKey,cast(SubMeasureID as  string) as SubMeasureID,Submeasurekey,SubMeasureSort,MeasureJoiner,
DENOMCNT,NUMERCNT,NUMERCNT_TYPE,NONCOMPLIANT,SPNUMCNT,SUPPDENOM,SUPPNUMER,PCP_ProviderKey,FlowchartRunID,DATEKEY,rundatetime,FLOWCHARTRUNNAME,
AGE_DOS,DateOfService,SupplementalType,DOS_Year_Month_Number,Runmonth,Age_Cat,Age_Cat_Sort 
from CTE_OH_NCQA_BCS_Member_Detail 
union all
select MemberKey,MemberName,DateOfBirth,MemberID,MeasureID,MeasureKey,cast(SubMeasureID as  string) as SubMeasureID,Submeasurekey,SubMeasureSort,MeasureJoiner,
DENOMCNT,NUMERCNT,NUMERCNT_TYPE,NONCOMPLIANT,SPNUMCNT,SUPPDENOM,SUPPNUMER,PCP_ProviderKey,FlowchartRunID,DATEKEY,rundatetime,FLOWCHARTRUNNAME,
AGE_DOS,DateOfService,SupplementalType,DOS_Year_Month_Number,Runmonth,Age_Cat,Age_Cat_Sort  from CTE_OH_NCQA_CCS_Member_Detail
union all
select MemberKey,MemberName,DateOfBirth,MemberID,MeasureID,MeasureKey,cast(SubMeasureID as  string) as SubMeasureID,Submeasurekey,SubMeasureSort,MeasureJoiner,
DENOMCNT,NUMERCNT,NUMERCNT_TYPE,NONCOMPLIANT,SPNUMCNT,SUPPDENOM,SUPPNUMER,PCP_ProviderKey,FlowchartRunID,DATEKEY,rundatetime,FLOWCHARTRUNNAME,
AGE_DOS,DateOfService,SupplementalType,DOS_Year_Month_Number,Runmonth,Age_Cat,Age_Cat_Sort  from CTE_OH_NCQA_PPC_Member_Detail)a
 
-- select * from QM.OH_NCQA_QSI_Member_Detail where dateofservice is not null and numercnt = 0
--SELECT * FROM [ DC01HDSDBPC06\SQL6].[QSFI_MHOH_QSI_2019_PROSPECTIVE].[dbo].[INTERFACE_MeasureProfile_OutputFlaggedEvent_PPC19_Expanded_VIEW]
--  WHERE CATEGORY LIKE 'U_N%' 
--  AND flowchartrunID =6
--  and RIGHT(Category,LEN(Category)-4) <> 'ALL' AND MEMBERKEY IN (
--   'OHI131645838658',
--'OHI131645858501',
--'OHI151526335861',
--'OHI163365162164'
--)

--ALTER TABLE QM.OH_QBA_QSI_Member_Detail_w_PROV
--ADD 
--CurrentNCQAMbr VARCHAR(100), 
--NCQA_DateOfService datetime,
--NCQA_Numerator int
--GO
 

create table eim_qa_db.OH_QBA_QSI_Member_Detail_w_PROV_update1 as
 (

  select distinct   
  a.*, CASE 
			WHEN A.MemberKey  =  B.memberkey1 THEN 'Yes'
			else 'No'
		    END as CurrentNCQAMbr,
		 B.dateofservice1  as NCQA_DateOfService ,
		 CASE WHEN B.dateofservice1 is NOT NULL THEN 1 ELSE 0 END as NCQA_Numerator
		 
from (select distinct * from  eim_qa_db.OH_QBA_QSI_Member_Detail_w_PROV ) as A   --  70284
left join (select   memberkey as memberkey1,max(dateofservice) as dateofservice1,					 
					concat(MeasureKey,isnull(SubMeasureKey,'')) as Measuresubmeasurekey
			from eim_qa_db.OH_NCQA_QSI_Member_Detail
			group by memberkey,concat(MeasureKey,isnull(SubMeasureKey,''))  ) as B
on A.MemberKey=B.memberkey1 AND concat(a.MeasureKey,isnull(a.SubMeasureKey,'')) = b.Measuresubmeasurekey
 
 )


/* FOR BCS measure, map submeasure as NULL since QBA benchmarks have NULL value */

/*Update A
Set A.CurrentNCQAMbr=
			CASE 
			WHEN A.MemberKey  =  B.MemberKey THEN 'Yes'
			else 'No'
		    END,
	A.NCQA_DateOfService = B.DateOfService,
	A.NCQA_Numerator = CASE WHEN B.DateOfService is NOT NULL THEN 1 ELSE 0 END			  

From QM.OH_QBA_QSI_Member_Detail_w_PROV(NOLOCK) as A left join QM.OH_NCQA_QSI_Member_Detail as B
on A.MemberKey=B.MemberKey AND A.MeasureKey+isnull(A.SubMeasureKey,'')=B.MeasureKey+isnull(B.SubMeasureKey,'')  collate SQL_Latin1_General_CP1_CI_AS

/* FOR BCS measure, map submeasure as NULL since QBA benchmarks have NULL value */

Update A
Set A.Submeasurekey=null,
	A.MeasureJoiner = A.Measurekey + 'NULL'		  
From QM.OH_QBA_QSI_Member_Detail_w_PROV(NOLOCK) A
WHERE A.Measurekey = 'BCS'
*/

create table eim_qa_db.OH_QBA_QSI_Member_Detail_w_PROV_update2 as
select a.memberid,memberkey,membername,dateofbirth,age_current,memberfirstname,membermiddlename,memberlastname,
sex,qhpstate,memberenrollmentqhpstate,memberstate,membercounty,membercity,memberzip,memberaddress1,memberaddress2,pcp_facilitycode,pcp_facilitydescription,
pcp_managementgroupcode,pcp_managementgroupdescription,payer,payercode,pcp_providerkey,pcp_providernpi,pcp_providerdeacode,pcp_providername,
pcp_languages,pcp_locationkey,pcp_providerstate,pcp_providercity,pcp_providerzip,pcp_provideraddress1,pcp_provideraddress2,pcp_providerphone1,pcp_providerphone2,
pcp_provideraltid1,pcp_provideraltid2,pcp_provideraltid3,pcp_provideraltid4,pcp_provideraltid5,pcp_provideraltid6,pcp_provideraltid7,pcp_provideraltid8,
pcp_provideraltid9,pcp_provideraltid10,planmarketingname,productcode,product,plancode,
plan,cmsplanid,snpenrolleetype,effectivedate,terminationdate,medicarecontractcode,medicarecontract,membergroupcode,membergroup,date4,
date6,clssdt,mentalhealthambulflag,mentalhealthinpatientflag,mentalhealthdaynightflag,chemdependambulflag,chemdependinpatientflag,chemdependdaynightflag,hospiceflag,
severityfactor,metallevel,esrdflag,citizenshipstatus,countyfipscode,sexualorientation,sexassignedatbirth,currentgenderidentity,memberdisabilitystatus,measureid,
measurekey,submeasureid,submeasuresort,denomcnt,numercnt,numercnt_type,noncompliant,spnumcnt,suppdenom,suppnumer,
flowchartrunid,datekey,rundatetime,flowchartrunname,age_dos,dateofservice,supplementaltype,dos_year_month_number,runmonth,age_cat_current,age_cat_sort_current,
age_cat_dos,age_cat_sort_dos,custom_ethnicity,medinsight_prov_tin,medinsight_prov_type,medinsight_prov_pcp_ipa,medinsight_ipa_npi,medinsight_ipa_name,medinsight_ipa_spec_desc,
medinsight_ipa_clinic_zip,medinsight_ipa_clinic_state,medinsight_ipa_clinic_county,medinsight_ipa_clinic_latitude,medinsight_ipa_clinic_longitude,medinsight_prov_pcp,
medinsight_pcp_npi,medinsight_pcp_clinic_zip,medinsight_pcp_clinic_state,medinsight_pcp_clinic_county,medinsight_pcp_clinic_latitude,medinsight_pcp_clinic_longitude,
medinsight_pcp_dob,medinsight_pcp_lname,medinsight_pcp_fname,medinsight_pcp_mname,medinsight_pcp_gender,medinsight_pcp_lang,medinsight_pcp_spec_desc,medinsight_year_mo,
medinsight_tot_month_with_pcp,medinsight_total_num_months_with_molina,medinsight_num_gap_months,memberzip5digit,geo_memberstate,geo_membercounty,
geo_memberlatitude,geo_memberlongitude,pcp_provider_state,pcp_provider_county,pcp_provider_latitude,pcp_provider_longitude,currentncqambr,ncqa_dateofservice,ncqa_numerator
,case when measurekey ='BCS' then null else a.Submeasurekey end as Submeasurekey,
case when measurekey ='BCS' then concat(A.Measurekey,'NULL') else A.Measurekey	end as MeasureJoiner
from eim_qa_db.OH_QBA_QSI_Member_Detail_w_PROV_update1 a
 
 /* FOR BCS measure, map submeasure as NULL since QBA benchmarks have NULL value */
 
drop table eim_qa_db.OH_QBA_QSI_Member_Detail_w_PROV
drop table eim_qa_db.OH_QBA_QSI_Member_Detail_w_PROV_update1

alter table  eim_qa_db.OH_QBA_QSI_Member_Detail_w_PROV_update2 rename to  eim_qa_db.OH_QBA_QSI_Member_Detail_w_PROV
