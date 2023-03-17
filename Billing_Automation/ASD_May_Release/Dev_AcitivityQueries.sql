--BMA
select c.BenefitId__c,e.ecosystems_svcs__Contact__c as SFDCID,e.ecosystems_svcs__salesPlay__c,e.ecosystems_svcs__documentType__c,e.ecosystems_svcs__lastUpdated__c
from Salesforce..ecosystems_svcs__IBC__c e
inner join salesforce..contact c on e.ecosystems_svcs__Contact__c = c.Id
where ecosystems_svcs__documentType__c = 'Business Model Assessment' and ecosystems_svcs__salesPlay__c = 'Customer signed-off' and c.BenefitId__c is not null and e.ecosystems_svcs__Contact__c is not null

--BAT 2.0,Growth Plan Builder,Marketing Plan,Succession / M&A Planning
select c.BenefitId__c,ContactId as SFDCID,EndDate__c,
case when P.Name = 'Business Assessment/ VMI' then 'BAT 2.0'
when  P.Name in ('Growth Plan Builder', 'Marketing Plan') then 'Growth Plan Builder (Marketing plan with BC)'
when P.Name in ('Succession / M&A Planning','Success Planning') then 'Sucession/M&A Plan Create/Review'
end as ActivityName
from 
Salesforce.dbo.Opportunity O inner join Salesforce.dbo.OpportunityLineItem OL on O.Id = OL.OpportunityId
inner join Salesforce.dbo.Product2 P on OL.Product2Id = P.Id
inner join Salesforce..Contact c on c.Id = o.ContactId
where O.RecordTypeId = '012600000009WEDAA2'  and  P.Name in ('Business Assessment/ VMI','Growth Plan Builder', 'Marketing Plan', 
'Succession / M&A Planning') and ol.EndDate__c is not null and ContactId is not null
 
 --OpportunityAdvisorSuccessPlan
select c.BenefitId__c,o.ContactId as SFDCID,o.CreatedDate, 'Advisor Success Plan Creation' as ActivityName
from Salesforce..Opportunity o inner join Salesforce..Contact c on c.id = o.ContactId
where o.SFAOpportunityURL__c is not null  and o.CreatedDate is not null and o.ContactId is not null

--CampaignMember - Premier Advisor,Study Group Meeting,Investment Mastery, Business Consulting Mastery
select c.BenefitId__c,ContactId as SFDCID,
CP.RecordTypeId, 
CP.Type, 
CP.EventType__c, 
CP.EndDate, 
CP.StartDate , 
CM.Status,
case when CP.EventType__c = 'Premier Advisor' then 'Premier Advisor Meeting'
when  CP.EventType__c = 'Study Group' then 'Study Group Meeting'
when CP.EventType__c in ('Investment Mastery','Business Consulting Mastery') then 'Mastery Meeting Attendance'
end as ActivityName
from Salesforce..CampaignMember CM 
inner join Salesforce..Campaign CP on CP.Id = CM.CampaignId
inner join Salesforce..Contact c on c.id = cm.ContactId
where   EventType__c in ('Premier Advisor','Study Group Meeting','Investment Mastery','Business Consulting Mastery') and 
CM.Status = 'Attended' and CP.RecordTypeId = '012600000009KZaAAM' and CP.EndDate is not null and CP.StartDate is not null 
and ContactId is not null and c.BenefitId__c is not null

--ShareofWallet
select c.benefitid__c,Contact__c as SFDCID,w.CreatedDate, 'Business Review' as ActivityName
from salesforce.dbo.Share_of_Wallet__c w inner join Salesforce..Contact c on c.id = w.Contact__c
where Contact__c is not null and w.CreatedDate is not null and c.BenefitId__c is not null

--FallFirmLevelNPSSurvey
select c.benefitid__c,Contact__c as SFDCID,w.CreatedDate, 'Fall Firm Level NPS Survey' as ActivityName
from Salesforce.dbo.[NetPromoterScore__c] w inner join Salesforce..Contact c on c.id = w.Contact__c
where Contact__c is not null and w.CreatedDate is not null and c.BenefitId__c is not null

--RedTail
select c.benefitid__c,u.SSOGUIDID,u.ModifiedDate as FirstActivityDate
from  AM..user_preference u inner join salesforce..contact c on c.SSOGUID__c = u.SSOGUIDID 
where u.SSOGUIDID is not null and u.[key] = 'RedtailUserKey' and c.BenefitId__c is not null
