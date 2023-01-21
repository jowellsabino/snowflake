SELECT patient.PAT_MRN_ID ,PAT_ENC_HSP.*
FROM ads_epic_stage.ods.PAT_ENC_HSP AS PAT_ENC_HSP
JOIN ads_epic_stage.ods.patient  AS patient
  ON PATIENT.PAT_ID  = PAT_ENC_HSP.PAT_ID 
JOIN ads_epic_stage.ods.HSP_ACCT_CVG_LIST AS HSP_ACCT_CVG_LIST
  ON HSP_ACCT_CVG_LIST.HSP_ACCOUNT_ID  = PAT_ENC_HSP.HSP_ACCOUNT_ID 
JOIN ads_epic_stage.ods.V_COVERAGE_PAYOR_PLAN V_COVERAGE_PAYOR_PLAN
  ON V_COVERAGE_PAYOR_PLAN.COVERAGE_ID  = HSP_ACCT_CVG_LIST.COVERAGE_ID 
WHERE ((PAT_ENC_HSP.ADT_PAT_CLASS_C = '2') 
        OR (PAT_ENC_HSP.ADT_PAT_CLASS_C = '4')  
    /*    OR (PAT_ENC_HSP.ADT_PAT_CLASS_C = '5') */
        OR (PAT_ENC_HSP.ADT_PAT_CLASS_C = '11') ) 
AND PAT_ENC_HSP.EXP_ADMISSION_TIME between current_date() and current_date()+14--( today to 14 days in the future)
and PAT_ENC_HSP.ADMIT_CONF_STAT_C = 1 -- (Confirmed)
and ((HSP_ACCT_CVG_LIST.HSP_ACCOUNT_ID IS NULL) 
     or HSP_ACCT_CVG_LIST.LINE = 1 
     OR  HSP_ACCT_CVG_LIST.LINE = 2)
and (V_COVERAGE_PAYOR_PLAN.TERM_DATE IS NULL 
      or V_COVERAGE_PAYOR_PLAN.TERM_DATE >= PAT_ENC_HSP.EXP_ADMISSION_TIME) --(coverage level has active coverage or not)
--and (HSP_ADMIT_DIAG.LINE IS null 
  --     or HSP_ADMIT_DIAG.LINE} = 1) --(has Admit diag or not)
--(Inpatient, Observation, Day Surgery, and T-CBAT Account Class)