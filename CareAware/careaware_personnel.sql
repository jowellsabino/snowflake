WITH careaware_personnel AS (
SELECT prsnl.person_id
     , prsnl.name_full_formatted
     , prsnl.username
     , prsnl.active_ind
     , prsnl.position_cd
     , prsnl.physician_ind
     , prsnl.contributor_system_cd
     , prsnl.ods_update_dt_tm as prsnl_ods_update_dttm
     , personnel.personnel_id
     , personnel.delete_ind
     , connect_personnel.ccmp_id
     , connect_personnel.directory_entry_id
     , connect_personnel.rpt_ssid as connect_personnel_rpt_ssid
     , connect_personnel.rpt_extract_dttm as connect_personnel_rpt_extract_dttm
     , connect_personnel.rpt_last_processed_dttm as connect_personnel_rpt_last_processed_dttm
     , connect_personnel.rpt_version as connect_personnel_rpt_version
     , personnel_role.*
     , personnel_role.rpt_ssid as personnel_role_rpt_ssid
     , personnel_role.rpt_extract_dttm as personnel_role_rpt_extract_dttm
     , personnel_role.rpt_last_processed_dttm as personnel_role_rpt_last_processed_dttm
     , personnel_role.rpt_version as personnel_role_rpt_version
     , row_number() over (partition by personnel_role.personnel_id order by personnel_role.rpt_version desc) as current_rolenum
  --   , personnel.*
  --   , connect_personnel.*
  --   , personnel_role.*
  FROM /* Millennium table */
       BOSTONCHILDRENS_PROD.PRSNL prsnl
  JOIN /* Millennium aliases */
       BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_D_PERSONNEL_ALIAS personnel_alias
    ON personnel_alias.alias_id = prsnl.username
   AND personnel_alias.alias_context = 'USERNAME'
   AND personnel_alias.alias_issuer = 'CERNER_MILLENNIUM'
  JOIN /* Main table, PERSONNEL_ID as main identifier, but also has CCMP_ID and DIRECTORY_ENTRY_ID      
          CCMP_ID seems to be the careaware connect messenger profile id, 
          which takes the form 'f4ba19f6-50f0-4bc6-a159-e402e0a9af78@chld_ma.p647.cloud_prod.caconnect.com'
          This ID is associated with Connect Messenger and Voice sessions
          
          NOTE: THERE IS A DIFFERENCE BETWEEN THIS TABLE AND CA_D_CONNECT_PERSONNEL!!!!!!
                Need more investigation
        */
       BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_D_PERSONNEL personnel
    ON personnel.personnel_id =  personnel_alias.personnel_id
  LEFT
  JOIN /* Connect ID's */
       BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_D_CONNECT_PERSONNEL connect_personnel
    ON connect_personnel.personnel_id =  personnel.personnel_id    
  JOIN /* "Role" in CareAware is "Position" in Millennium
        When we talk about "claiming a role in CA,  we mean "claiming a profile" 
        The role/position recorded in the CA DB has a record every position change! 
        so need to the the latest one. The latest record in terms of rpt_extract_dttm, 
        rpt_last_processed_dttm, or rpt_version is the current role
        
        The role has a corresponding role_id in CA, which parallels the position_cd in Millennium
        The role display is role_name.    
       */
       BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_D_PERSONNEL_ROLE personnel_role
    ON personnel.personnel_id =  personnel_role.personnel_id
) 

select careaware_personnel.name_full_formatted
     , count(connect_session.session_id) as session_number
  from careaware_personnel 
 LEFT 
 JOIN
      BOSTONCHILDRENS_IBCL_CHLD_MA_P647.CA_F_CONNECT_SESSION connect_session
   ON connect_session.user_id = careaware_personnel.ccmp_id   
 where careaware_personnel.current_rolenum = 1 /* the latest role/position only */
   AND connect_session.create_dttm > dateadd(month, -9, sysdate()) -- dateadd(year, -1, sysdate())
--   AND careaware_personnel.username = 'CH121199'  -- 'AKINS_L' -- 
   AND connect_session.session_id is null
 group by careaware_personnel.name_full_formatted
 -- having count(connect_session.session_id) =  1
 --  order by careaware_personnel.name_full_formatted, connect_session.create_dttm desc