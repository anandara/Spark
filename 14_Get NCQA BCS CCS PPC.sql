--Get NCQA BCS CCS PPC
SET                FMTONLY ON;
SET                FMTONLY OFF;
                         /*****************************************************
 OBJECTIVE: COMBINE QSI FLOWCHART DETAIL FOR OH Quality-Based Auto-Assignment (NCQA) HEDIS 
 Measures BCS, CCS and PPC
            
 RUN FROM: [DC01DSSDBPC09\SQL9].[ExecDB_Stg]
 ****************************************************/



 --IF OBJECT_ID('QM.OH_NCQA_QSI_Member_Detail', 'U') IS NOT NULL
 -- DROP TABLE QM.OH_NCQA_QSI_Member_Detail
INSERT INTO  QM.OH_NCQA_QSI_Member_Detail
(
MemberKey,
MemberName,
DateOfBirth,
MemberID,
MeasureID,
MeasureKey,
SubMeasureID,
Submeasurekey,
SubMeasureSort,
MeasureJoiner,
DENOMCNT,
NUMERCNT,
NUMERCNT_TYPE,
NONCOMPLIANT,
SPNUMCNT,
SUPPDENOM,
SUPPNUMER,
PCP_ProviderKey,
FlowchartRunID,
DATEKEY,
rundatetime,
FLOWCHARTRUNNAME,
AGE_DOS,
DateOfService,
SupplementalType,
DOS_Year_Month_Number,
Runmonth,
Age_Cat,
Age_Cat_Sort)

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
 ,LEFT(CONVERT(VARCHAR, RUNDATETIME,112),6) as Runmonth
 ,Age_Cat = CASE 
			WHEN Age_DOS >= 71 THEN '71+'
			WHEN Age_DOS >= 61 THEN '61-70'
			WHEN Age_DOS >= 51 THEN '51-60'
			WHEN Age_DOS >= 41 THEN '41-50'
			WHEN Age_DOS >= 31 THEN '31-40'
			WHEN Age_DOS >= 21 THEN '21-30'
			WHEN Age_DOS >= 0  THEN '0-20'
			ELSE NULL END 
,Age_Cat_Sort = CASE 
			WHEN Age_DOS >= 71 THEN 7
			WHEN Age_DOS >= 61 THEN 6
			WHEN Age_DOS >= 51 THEN 5
			WHEN Age_DOS >= 41 THEN 4
			WHEN Age_DOS >= 31 THEN 3
			WHEN Age_DOS >= 21 THEN 2
			WHEN Age_DOS >= 0  THEN 1
			ELSE NULL END 
 --INTO QM.OH_NCQA_QSI_Member_Detail
 FROM ##OH_NCQA_BCS_Member_Detail AS a
 UNION ALL
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
 ,LEFT(CONVERT(VARCHAR, RUNDATETIME,112),6) as Runmonth
 ,Age_Cat = CASE 
			WHEN Age_DOS >= 71 THEN '71+'
			WHEN Age_DOS >= 61 THEN '61-70'
			WHEN Age_DOS >= 51 THEN '51-60'
			WHEN Age_DOS >= 41 THEN '41-50'
			WHEN Age_DOS >= 31 THEN '31-40'
			WHEN Age_DOS >= 21 THEN '21-30'
			WHEN Age_DOS >= 0  THEN '0-20'
			ELSE NULL END 
,Age_Cat_Sort = CASE 
			WHEN Age_DOS >= 71 THEN 7
			WHEN Age_DOS >= 61 THEN 6
			WHEN Age_DOS >= 51 THEN 5
			WHEN Age_DOS >= 41 THEN 4
			WHEN Age_DOS >= 31 THEN 3
			WHEN Age_DOS >= 21 THEN 2
			WHEN Age_DOS >= 0  THEN 1
			ELSE NULL END 
 FROM ##OH_NCQA_CCS_Member_Detail AS a
 UNION ALL
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
 ,LEFT(CONVERT(VARCHAR, RUNDATETIME,112),6) as Runmonth
 ,Age_Cat = CASE 
			WHEN Age_DOS >= 71 THEN '71+'
			WHEN Age_DOS >= 61 THEN '61-70'
			WHEN Age_DOS >= 51 THEN '51-60'
			WHEN Age_DOS >= 41 THEN '41-50'
			WHEN Age_DOS >= 31 THEN '31-40'
			WHEN Age_DOS >= 21 THEN '21-30'
			WHEN Age_DOS >= 0  THEN '0-20'
			ELSE NULL END 
,Age_Cat_Sort = CASE 
			WHEN Age_DOS >= 71 THEN 7
			WHEN Age_DOS >= 61 THEN 6
			WHEN Age_DOS >= 51 THEN 5
			WHEN Age_DOS >= 41 THEN 4
			WHEN Age_DOS >= 31 THEN 3
			WHEN Age_DOS >= 21 THEN 2
			WHEN Age_DOS >= 0  THEN 1
			ELSE NULL END 
 FROM QM.##OH_NCQA_PPC_Member_Detail AS a
 
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

Update A
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

