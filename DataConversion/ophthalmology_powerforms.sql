/* Store mapping of Cerner-to-Epic concepts here */
CREATE OR REPLACE TEMP TABLE ec_ophtho_map (
--epic_description varchar(40),
--epic_concept varchar(40),
--cerner_form (varchar(40),
--cerner_section varchar(40),
--cerner_concept varchar(40),
cerner_event_code varchar(40),
epic_concept varchar(40)
);

/* See https://community.snowflake.com/s/article/how-to-load-a-few-columns-from-a-csv-file-into-a-new-snowflake-table */
/* CSV data format */
CREATE OR REPLACE TEMP FILE FORMAT csv_ophtho
    TYPE = CSV
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ',' 
 	RECORD_DELIMITER = '\n' 
 	SKIP_HEADER = 1
 	FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
 	TRIM_SPACE = FALSE 
 	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
 	ESCAPE = 'NONE' 
 	ESCAPE_UNENCLOSED_FIELD = '\134' 
 	DATE_FORMAT = 'mm/dd/yyyy' 
 	TIMESTAMP_FORMAT = 'AUTO' 
 	NULL_IF = ('NULL', 'null', '\N') 
 	COMMENT = 'parse comma-delimited, double-quoted data';


 /* Need to put the file contents on personal staging area in Snowflake */
 PUT file://~/Documents/Epic/ec_ophtho_map.csv @~/ec_ophtho_map.csv;

/* Copy contents in personal staging area to table */
 COPY INTO ec_ophtho_map FROM @~/ec_ophtho_map.csv 
  FILE_FORMAT = csv_ophtho;

 /* Another option to insert contenst to tenp table */
 /*
 INSERT INTO ec_ophtho_map (cerner_event_code,epic_concept) 
  VALUES (
	34567,4563
 	);
*/
 
 /*Verify load */
 --SELECT *
 --FROM ec_ophtho_map;
 

WITH ophthalmology_form AS (
SELECT ce_form.PERSON_ID AS person_id,
       ce_form.ENCNTR_ID AS encntr_id,
       --ce_form.event_id,
       --ce_form.EVENT_CD,
       ce_form.EVENT_TITLE_TEXT AS form_name, 
       --ce_form.EVENT_END_DT_TM AS form_datetime,
       --ce_form.RESULT_VAL AS form_result, /* Empty */
       --ce_sect.ENCNTR_ID AS section_encntr_id,
       --ce_sect.EVENT_ID, 
       --ce_sect.event_cd,
       ce_sect.EVENT_TITLE_TEXT AS section_name,
       --ce_sect.EVENT_END_DT_TM AS section_datetime,
       --ce_sect.RESULT_VAL AS section_result /* Empty */
       ce_docm.EVENT_END_DT_TM AS result_datetime,
       ce_docm.EVENT_TITLE_TEXT AS result_text,
       ce_docm.EVENT_CD,
       ce_docm.RESULT_VAL AS result_val
FROM clinical_event ce_form 
JOIN clinical_event ce_sect
  ON ce_sect.parent_event_id = ce_form.event_id /* equal to dfc.parent_entity_id */
 AND ce_sect.valid_until_dt_tm > current_date() /* XAK2 */
 AND ce_sect.result_status_cd in (25,34,35)  /* Add this so we do ot include uncharted sections */
 AND ce_sect.event_id != ce_form.event_id
JOIN clinical_event ce_docm
  ON ce_docm.parent_event_id = ce_sect.event_id
 AND ce_docm.valid_until_dt_tm > current_date() 
 AND ce_docm.result_status_cd in (25,34,35) /* Include this so we do not include uncharted EC/DTA's */
WHERE ce_form.event_cd IN (
      905600649.00,	        --  	Ophthalmology New - Form	
      905475781.00,	        --		Optical Rx (Gas Permeable Contacts)-Form	
      905468207.00,	        -- 		Optical Rx (Glasses) - Form	
      905470919.00	        --		Optical Rx (Soft Contacts) - Form	  
      )
  AND ce_form.event_end_dt_tm >= dateadd(MONTH,-1,current_date()) /* One year's worth of data */
  AND ce_form.valid_until_dt_tm > current_date() /* XAK2 */
  AND ce_form.result_status_cd IN (25,34,35) /* Auth, Modified,Modified */
),
ophthalmology_extract AS (
SELECT pa.alias AS MRN,
       ea.alias AS CSN,
       ophthalmology_form.*
from ophthalmology_form
JOIN person p 
  ON p.person_id = ophthalmology_form.person_id
JOIN person_alias pa
  ON pa.person_id = ophthalmology_form.person_id
 AND pa.person_alias_type_cd = 10
 AND pa.end_effective_dt_tm > current_date()
 AND pa.alias_pool_cd = 3110551.00  /* CHB MRN only */
 AND pa.active_ind = 1
JOIN encntr_alias ea
  ON ea.encntr_id = ophthalmology_form.encntr_id
 AND ea.end_effective_dt_tm > current_date()
 AND ea.encntr_alias_type_cd = 1077 /* CSN */
 )

SELECT mrn,csn,encntr_id,
       form_name,
       section_name, 
       result_datetime,result_text, event_cd,result_val
FROM ophthalmology_extract
ORDER BY mrn,csn,form_name,section_name;