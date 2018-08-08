# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 10:52:49 2017

@author: AustinPeel
"""

from qualtrics import version3 
from pandas.io.json import json_normalize


#get Distrubtion summary by Survey
jsonData = version3.qualtrics(survey="suvrey_id").getListDistributions()

#gets the distribution summary for only one distrubtion ID. (kind of pointless)
jsonData = version3.qualtrics(survey="surveyID").getDistributions(distributionId="distributionID")


#get df of all contacts for a distributionId. the status column is bogus 
df2 = version3.qualtrics().getAllContacts(distributionId="DistributionID")

# this function isnt done the output shows multiple email history per person, we need to reconcile that before dumping it into a df. 
jsonData = version3.qualtrics().getContacts("ML_ID")

#download data and extract it from zip
#paramters=(lastResponseId,startDateendDate,limit,includedQuestionIds,useLabels,useLocalTime)
#defaults to csv(fileFormat="csv")
version3.qualtrics(survey="SurveyID").downloadExtractZip(lastResponseId="lastResponseID")
version3.qualtrics(survey="SurveyID").downloadExtractZip()


