--Advisor List
select * into #advisor1 from  (select C.BenefitId__c as PrimaryAdvisorIdentifier,C.id as SFDCID from  Salesforce..Contact C 
inner join Salesforce..Account A on A.Id = C.Advisor__c
where C.RecordTypeId in ('012600000009BHyAAM') and C.BenefitId__c is not null and A.BenefitId__c is not null) advisor

--Defining run date
DECLARE @getdate AS DATETIME = SYSDATETIME()
--AccountProduct curation query
select ad.PrimaryAdvisorIdentifier as PrimaryAdvisorIdentifier, a.ProductName__c as BenefitProductName, a.Category__c as Category, a.LicenseStartDate__c as SubscriptionStartDate, a.LicenseType__c as LicenseType, a.PricingModel__c as PricingModel, a.HasLicenseAccommodation__c as HasLicenseAccommodation, a.PricingModelDetails__c as PricingModelDetails, a.LicenseTerminationDate__c as LicenseTerminationDate, cast(NULL as varchar(50)) as IsLatestOffer, a.CreatedDate as CreatedDate, cast(NULL as varchar(50)) as OfferType, a.LastModifiedDate as ModifiedDate
from Salesforce..AccountProduct__c a
inner join Salesforce..Contact c on a.Contact__c=c.Id
inner join Salesforce..Account aa on aa.Id = c.Advisor__c
inner join #advisor1 ad on c.BenefitId__c = ad.PrimaryAdvisorIdentifier
where c.RecordTypeId in ('012600000009BHyAAM')
and c.BenefitId__c is not null and aa.BenefitId__c is not null and a.LastModifiedDate<=@getdate


--FinancialPlanning curation query
select ad.PrimaryAdvisorIdentifier as PrimaryAdvisorIdentifier, f.OfferDescription__c as BenefitProductName, 'Financial Planning' as Category, f.SubscriptionStartDate__c as SubscriptionStartDate, cast(NULL as varchar(50)) as LicenseType, f.OfferType__c as PricingModel, cast(NULL as varchar(50)) as HasLicenseAccommodation, cast(NULL as varchar(50)) as PricingModelDetails, cast(NULL as varchar(50)) as LicenseTerminationDate, f.IsLatestOffer__c as IsLatestOffer, f.OfferType__c as OfferType, f.CreatedDate as CreatedDate, f.LastModifiedDate as ModifiedDate
from Salesforce..FinancialPlanning__c f
inner join Salesforce..Contact c on c.BenefitId__c=f.benefitId__c
inner join Salesforce..Account A on A.Id = c.Advisor__c
inner join #advisor1 ad on c.BenefitId__c = ad.PrimaryAdvisorIdentifier
where C.RecordTypeId in ('012600000009BHyAAM')
and c.BenefitId__c is not null and A.BenefitId__c is not null and f.IsLatestOffer__c = 'TRUE' and f.LastModifiedDate<=@getdate 