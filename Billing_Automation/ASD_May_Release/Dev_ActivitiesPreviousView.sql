-- #Activities
CREATE TABLE #ActivityCredit(
	[ActivityName] [varchar](200) NOT NULL,
	[Credits] [int] NOT NULL,
	[ActivityIndex] [int] NOT NULL,
 CONSTRAINT [pk_ACConstraint1] PRIMARY KEY CLUSTERED 
(
	[ActivityName] ASC
))

-- Insert Activity list
INSERT INTO  #ActivityCredit Values ('BAT 2.0',1,0) --
INSERT INTO  #ActivityCredit Values ('BMA',2,0) -- 
INSERT INTO  #ActivityCredit Values ('Growth Plan Builder (Marketing plan with BC)',2,0) --
INSERT INTO  #ActivityCredit Values ('Sucession/M&A Plan Create/Review',1,0) -- 
INSERT INTO  #ActivityCredit Values ('Advisor Success Plan Creation',1,0) -- 
INSERT INTO  #ActivityCredit Values ('Premier Advisor Meeting',1,0) -- 
INSERT INTO  #ActivityCredit Values ('Mastery Meeting Attendance',1,0) --
INSERT INTO  #ActivityCredit Values ('Study Group Meeting',1,0) --
INSERT INTO  #ActivityCredit Values ('Business Builder Webinar: attendance and feedback',1,0)
INSERT INTO  #ActivityCredit Values ('Premier Service Series',1,0)
INSERT INTO  #ActivityCredit Values ('RedTail',1,0) -- 
INSERT INTO  #ActivityCredit Values ('Business Review',2,0) --
INSERT INTO  #ActivityCredit Values ('Fall Firm Level NPS Survey',1,0) --
INSERT INTO  #ActivityCredit Values ('Operations Advisory Board Participation',2,0)


--Select * from #ActivityCredit
--#Advisor
select * into #advisor1 from  (select C.BenefitId__c as PrimaryAdvisorIndentifier,C.id as SFDCID, 0 as Activitylist,c.SSOGUID__c as SSOGUID,
case when Setup_Date__c is null then First_Sale_Date__c else Setup_Date__c end as Advisorstartdate
from  Salesforce..Contact C 
inner join Salesforce..Account A on A.Id = C.Advisor__c
where C.RecordTypeId in ('012600000009BHyAAM') and C.BenefitId__c is not null and A.BenefitId__c is not null) advisor


select PrimaryAdvisorIndentifier,SFDCID,SSOGUID, ActivityName,Credits ,Advisorstartdate
into  #advisorActivity from #advisor1 left join #ActivityCredit  AC on Activitylist = [ActivityIndex]
--select * from #ActivityCredit
--select top 30 * from #advisorActivity order by PrimaryAdvisorIndentifier 


-- BMA
select * into #BMA from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
LastupdatedDate as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.0000000' else CreatedDate end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.0000000' else LastupdatedDate end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  SFDCID,min(ecosystems_svcs__lastUpdated__c) as CreatedDate,'BMA' as ActivityName,
max(ecosystems_svcs__lastUpdated__c) as LastupdatedDate,sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select ecosystems_svcs__Contact__c as SFDCID,ecosystems_svcs__lastUpdated__c,
case when year(ecosystems_svcs__lastUpdated__c) = 2022 then 1 else 0 end as #Previousyear,
case when year(ecosystems_svcs__lastUpdated__c) = 2023 then 1 else 0 end as #Currentyear from Salesforce..ecosystems_svcs__IBC__c 
where ecosystems_svcs__documentType__c = 'Business Model Assessment' and ecosystems_svcs__salesPlay__c = 'Customer signed-off'  
)E 
group by SFDCID
) BMA on BMA.SFDCID = C.SFDCID and C.ActivityName = BMA.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName = 'BMA'
) Activity1

--select * from #BMA where LastCompletionDate is not null

--#BAT 2.0,Growth Plan Builder,Marketing Plan,Succession / M&A Planning
select * into #Opportunity from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
LastupdatedDate as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.0000000' else CreatedDate end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.0000000' else LastupdatedDate end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  SFDCID,min(EndDate__c) as CreatedDate, ActivityName,
max(EndDate__c) as LastupdatedDate,sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select ContactId as SFDCID,EndDate__c,
case when P.Name = 'Business Assessment/ VMI' then 'BAT 2.0'
when  P.Name in ('Growth Plan Builder', 'Marketing Plan') then 'Growth Plan Builder (Marketing plan with BC)'
when P.Name in ('Succession / M&A Planning','Success Planning') then 'Sucession/M&A Plan Create/Review'
end as ActivityName,
case when year(EndDate__c) = 2022 then 1 else 0 end as #Previousyear,
case when year(EndDate__c) = 2023 then 1 else 0 end as #Currentyear 
from 
Salesforce.dbo.Opportunity O inner join Salesforce.dbo.OpportunityLineItem OL on O.Id = OL.OpportunityId
inner join Salesforce.dbo.Product2 P on OL.Product2Id = P.Id
where O.RecordTypeId = '012600000009WEDAA2'  and  P.Name in ('Business Assessment/ VMI','Growth Plan Builder', 'Marketing Plan', 
'Succession / M&A Planning') and ol.EndDate__c is not null and ContactId is not null --and o.ContactId = '0030e00002DF0jsAAD'
 
) E 
group by SFDCID,ActivityName
) Opp on Opp.SFDCID = C.SFDCID and C.ActivityName = Opp.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName in ('BAT 2.0','Growth Plan Builder (Marketing plan with BC)', 'Sucession/M&A Plan Create/Review') 
) Activity2

--select top 10 * from #Opportunity where LastCompletionDate is not null

----#OpportunityAdvisorSuccessPlan
select * into #OpportunityAdvisorSuccessPlan from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
LastupdatedDate as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.0000000' else CreatedDate end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.0000000' else LastupdatedDate end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  SFDCID,min(CreatedDate) as CreatedDate, ActivityName,
max(CreatedDate) as LastupdatedDate,sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select ContactId as SFDCID,CreatedDate, 'Advisor Success Plan Creation' as  ActivityName,
case when year(CreatedDate) = 2022 then 1 else 0 end as #Previousyear,
case when year(CreatedDate) = 2023 then 1 else 0 end as #Currentyear  
from Salesforce..Opportunity o 
where o.SFAOpportunityURL__c is not null  and CreatedDate is not null and ContactId is not null
 
) E 
group by SFDCID,ActivityName
) Opp on Opp.SFDCID = C.SFDCID and C.ActivityName = Opp.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName in ('Advisor Success Plan Creation') 
) Activity5

--select * from #OpportunityAdvisorSuccessPlan where LastCompletionDate is not null

--#CampaignMember - Premier Advisor,Study Group Meeting,Investment Mastery, Business Consulting Mastery
select * into #CampaignMember from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
LastupdatedDate as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.0000000' else CreatedDate end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.0000000' else LastupdatedDate end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  SFDCID,min(StartDate) as CreatedDate, ActivityName,
max(EndDate) as LastupdatedDate,sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select ContactId as SFDCID,
CP.RecordTypeId, 
CP.Type, 
CP.EventType__c, 
CP.EndDate, 
CP.StartDate , 
CM.Status,
case when CP.EventType__c = 'Premier Advisor' then 'Premier Advisor Meeting'
when  CP.EventType__c = 'Study Group' then 'Study Group Meeting'
when CP.EventType__c in ('Investment Mastery','Business Consulting Mastery') then 'Mastery Meeting Attendance'
end as ActivityName,
case when year(EndDate) = 2022 then 1 else 0 end as #Previousyear,
case when year(EndDate) = 2023 then 1 else 0 end as #Currentyear 
from Salesforce..CampaignMember CM 
inner join Salesforce..Campaign CP on CP.Id = CM.CampaignId
where   EventType__c in ('Premier Advisor','Study Group Meeting','Investment Mastery','Business Consulting Mastery') and 
CM.Status = 'Attended' and CP.RecordTypeId = '012600000009KZaAAM' and CP.EndDate is not null and CP.StartDate is not null 
and ContactId is not null

) E 
group by SFDCID,ActivityName
) Opp on Opp.SFDCID = C.SFDCID and C.ActivityName = Opp.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName in ('Premier Advisor Meeting','Study Group Meeting', 'Mastery Meeting Attendance') 
) Activity8

--select * from #CampaignMember where LastCompletionDate is not null


----------#ShareofWallet
select * into #ShareofWallet from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
LastupdatedDate as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.0000000' else CreatedDate end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.0000000' else LastupdatedDate end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  SFDCID,min(CreatedDate) as CreatedDate, ActivityName,
max(CreatedDate) as LastupdatedDate,sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select Contact__c as SFDCID,CreatedDate, 'Business Review' as ActivityName,
case when year(CreatedDate) = 2022 then 1 else 0 end as #Previousyear,
case when year(CreatedDate) = 2023 then 1 else 0 end as #Currentyear 
from salesforce.dbo.Share_of_Wallet__c
where Contact__c is not null and CreatedDate is not null

) E 
group by SFDCID,ActivityName
) Opp on Opp.SFDCID = C.SFDCID and C.ActivityName = Opp.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName in ('Business Review') 
) Activity9

--select * from #ShareofWallet where LastCompletionDate is not null

----------#FallFirmLevelNPSSurvey
select * into #FallFirmLevelNPSSurvey from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
LastupdatedDate as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.0000000' else CreatedDate end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.0000000' else LastupdatedDate end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  SFDCID,min(CreatedDate) as CreatedDate, ActivityName,
max(CreatedDate) as LastupdatedDate,sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select Contact__c as SFDCID,CreatedDate, 'Fall Firm Level NPS Survey' as ActivityName,
case when year(CreatedDate) = 2022 then 1 else 0 end as #Previousyear,
case when year(CreatedDate) = 2023 then 1 else 0 end as #Currentyear 
from Salesforce.dbo.[NetPromoterScore__c]
where Contact__c is not null and CreatedDate is not null

) E 
group by SFDCID,ActivityName
) Opp on Opp.SFDCID = C.SFDCID and C.ActivityName = Opp.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName in ('Fall Firm Level NPS Survey') 
) Activity10

--select * from #FallFirmLevelNPSSurvey where LastCompletionDate is not null

-------------#RedTail
select * into #RedTail from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
cast(LastupdatedDate as datetime) as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.000' else cast(CreatedDate as datetime) end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.000' else cast(LastupdatedDate as datetime) end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  SSOGUIDID,min(ModifiedDate) as CreatedDate, ActivityName,
max(ModifiedDate) as LastupdatedDate,sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select u.SSOGUIDID ,ModifiedDate,FirstActivityDate ,'Redtail' as ActivityName,
case when year(ModifiedDate) = year(FirstActivityDate) and year(FirstActivityDate) = 2022 then 1 else 0 end as #Previousyear,
case when year(ModifiedDate) = year(FirstActivityDate) and year(FirstActivityDate) = 2023 then 1 else 0 end as #Currentyear 
from  AM..user_preference u left join (
select SSOGUIDID,min(ModifiedDate) as FirstActivityDate
from  AM..user_preference u where u.SSOGUIDID is not null and  
u.[key] = 'RedtailUserKey'
group by SSOGUIDID) M  on M.SSOGUIDID = u.SSOGUIDID
where u.SSOGUIDID is not null and  
u.[key] = 'RedtailUserKey'  

) E 
group by SSOGUIDID,ActivityName
) Opp on Opp.SSOGUIDID = C.SSOGUID and C.ActivityName = Opp.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName in ('Redtail') 
) Activity11

--select * from #RedTail where LastCompletionDate is not null
-----------------

select * into #OperationsAdvisory from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
LastupdatedDate as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.000' else CreatedDate end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.000' else LastupdatedDate end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  BenefitId__c,min(OABTermStartDate__c) as CreatedDate,
'Operations Advisory Board Participation' as ActivityName,
max(LastupdatedDate) as LastupdatedDate,
sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select *,
case when
(year(OABTermStartDate__c) <= 2022 and   OABTermEndDate__c is null) or
(year(OABTermStartDate__c) <= 2022 and experiationDT >= '2022-12-31 00:00:00.0000000' 
) then  1 else 0 end as #Previousyear,
case when OABTermEndDate__c is null then 1  
when year(OABTermStartDate__c) = 2023 and experiationDT >= '2023-03-06 00:00:00.0000000' then 1
else 0 end as #Currentyear,
case when experiationDT > '2023-03-06 00:00:00.0000000' then 
DATEADD(year, -1 , experiationDT) 
else experiationDT end as LastupdatedDate
from (
select Id,BenefitId__c,OABMembershipStatus__c, OABTermStartDate__c, OABTermEndDate__c, 
case when OABTermEndDate__c is null then DATEADD(year, (2023 - year(OABTermStartDate__c)), OABTermStartDate__c) 
else OABTermEndDate__c end as experiationDT
from salesforce..Contact 
where OABMembershipStatus__c = 'Current Member' and BenefitId__c is not null
and OABTermStartDate__c is not null 
) E 
) F
group by BenefitId__c
) Opp on Opp.BenefitId__c = C.PrimaryAdvisorIndentifier and C.ActivityName = Opp.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName in ('Operations Advisory Board Participation') 
) Activity12

--select * from #OperationsAdvisory where LastCompletionDate is not null

select * into #CampaignMemberParentid from (
select 
PrimaryAdvisorIndentifier,
c.ActivityName,
LastupdatedDate as LastCompletionDate,
#ActivityCompletePreviousyear,
#ActivityCompleteCurrentyear,
#ActivityCompletePreviousyear * AC.Credits as CreditsEarnedPriorYear,
#ActivityCompleteCurrentyear * AC.Credits as CreditsEarnedCurrentYear,
case when CreatedDate is null then '2023-03-06 00:00:00.0000000' else CreatedDate end as CreatedDate,
case when LastupdatedDate is null then '2023-03-06 00:00:00.0000000' else LastupdatedDate end as ModifiedDate,Advisorstartdate
 from #advisorActivity C 
left join 
(
select  SFDCID,min(StartDate) as CreatedDate, ActivityName,
max(EndDate) as LastupdatedDate,sum(#Previousyear) as #ActivityCompletePreviousyear, sum(#Currentyear) as #ActivityCompleteCurrentyear
from 
(
select ContactId as SFDCID,
CP.RecordTypeId, 
CP.Type, 
CP.EventType__c, 
CP.EndDate, 
CP.StartDate , 
CM.Status,
case when CP.ParentId = '7017V000001wZ14QAE' then 'Business Builder Webinar: attendance and feedback'
when  CP.ParentId = '7010e00000128q2AAA' then 'Premier Service Series'
end as ActivityName,
case when year(EndDate) = 2022 then 1 else 0 end as #Previousyear,
case when year(EndDate) = 2023 then 1 else 0 end as #Currentyear 
from Salesforce..CampaignMember CM 
inner join Salesforce..Campaign CP on CP.Id = CM.CampaignId
where CM.Status = 'Attended' and CP.EndDate is not null and CP.ParentId in ('7017V000001wZ14QAE','7010e00000128q2AAA')
and ContactId is not null
) E 
group by SFDCID,ActivityName
) Opp on Opp.SFDCID = C.SFDCID and C.ActivityName = Opp.ActivityName 
inner join #ActivityCredit AC on Ac.ActivityName = C.ActivityName 
where C.ActivityName in ('Business Builder Webinar: attendance and feedback','Premier Service Series') 
) Activity13

--select * from #CampaignMemberParentid where #ActivityCompleteCurrentyear <> 0 order by PrimaryAdvisorIndentifier
---------------------


--------------
select case when Advisorstartdate <= '2022-12-31 00:00:00.0000000'  then 2 else 0 end as CreditsEarnedPriorYear_Grandfather,* from ( 
select * from #BMA
union all 
select * from #Opportunity
union all 
select * from #OpportunityAdvisorSuccessPlan 
union all 
select * from #CampaignMember
union all
select * from #ShareofWallet
union all 
select * from #FallFirmLevelNPSSurvey
union all 
select * from #RedTail 
union all
select * from #OperationsAdvisory
union all
select * from #CampaignMemberParentid
) a 
order by PrimaryAdvisorIndentifier desc



