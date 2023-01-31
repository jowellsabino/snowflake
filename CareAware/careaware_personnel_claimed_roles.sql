SELECT personnel.* 
 --    , prsnl.*
 --   , personnel_role.*
     , claimed_role.care_position_name
     , claimed_role.assignment_start_dttm
     , claimed_role.assignment_end_dttm
  FROM /* Millennium table */
       BOSTONCHILDRENS_PROD.PRSNL prsnl
  JOIN /* Millennium aliases */
       BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_D_PERSONNEL_ALIAS personnel_alias
    ON personnel_alias.alias_id = prsnl.username
   AND personnel_alias.alias_context = 'USERNAME'
   AND personnel_alias.alias_issuer = 'CERNER_MILLENNIUM'
  JOIN /* Main table, PERSONNEL_ID as main identifier, buut also has CCMP_ID and DIRECTORY_ENTRY_ID */     
       BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_D_CONNECT_PERSONNEL personnel
    ON personnel.personnel_id =  personnel_alias.personnel_id
--JOIN /* The role records every position change! */
--     BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_D_PERSONNEL_ROLE personnel_role
--  ON personnel.personnel_id =  personnel_role.personnel_id
  -- the latest record in terms of rpt_extract_dttm, rpt_last_processed_dttm, or rpt_version 
  -- is the current role
 JOIN BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_F_HOSPITALOPS_PERSONNEL_ASSIGNMENT  claimed_role
   ON claimed_role.personnel_id = personnel.personnel_id
 WHERE prsnl.username = 'CH121199' -- f4ba19f6-50f0-4bc6-a159-e402e0a9af78
--where personnel.personnel_id = 'ce86fa41-f015-46b7-9c3f-9701e588c371'
--where personnel.extension = '788183'
-- where connect_personnel.directory_entry_id = '7d91e554-2c1e-469a-9d81-45a5f73723b7'
-- where care_position_profile_id = '7d91e554-2c1e-469a-9d81-45a5f73723b7'
 order by claimed_role.assignment_start_dttm desc, assignment_end_dttm desc
LIMIT 100;
