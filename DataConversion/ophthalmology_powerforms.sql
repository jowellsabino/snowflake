/* The Cerner-to-Epic concept mappings will come in the form of a CSV file
 *  with a column for Cerner event code mapped to another column of Epic concept 
 *
 *  See https://community.snowflake.com/s/article/how-to-load-a-few-columns-from-a-csv-file-into-a-new-snowflake-table 
 *  CSV data format 
 */
CREATE OR REPLACE TEMP FILE FORMAT csv_ophtho
    TYPE = CSV
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ',' 
 	RECORD_DELIMITER = '\n' 
 	SKIP_HEADER = 1
 	FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
 	TRIM_SPACE = FALSE 
 	ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
 	ESCAPE = 'NONE' 
 	ESCAPE_UNENCLOSED_FIELD = '\134' 
 	DATE_FORMAT = 'mm/dd/yyyy' 
 	TIMESTAMP_FORMAT = 'AUTO' 
 	NULL_IF = ('NULL', 'null', '\N') 
 	COMMENT = 'parse comma-delimited, double-quoted data';


 /* Need to put the file contents on personal staging area in Snowflake 
    Alias-mapped omv6/NAS to /Users/jowell/Docuemnts/NAS */
 PUT file:///Users/jowell/Documents/Epic/ec_ophtho_map.csv @~/ec_ophtho_map.csv AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

/* Create a remporary mapping of Cerner-to-Epic concepts 
 * Cerner Opthalmology Code	
 * Cerner Opthalmology Name	
 * Usage in Cerner	
 * % of Total Usage	
 * In Scope?	
 * Epic FDC ID (Item .1) 
 * Epic FDC Name (Item .2) 	
 * Epic FDC MPI ID (Item 5001)	
 * HL7 Assigning Coding System (IIT Item 630)	
 * Epic FLO ID .1 (FDC Item 100)	
 * Epic FLO Name .2 (FDC Item .100)	
 * Comments	
 * Tech Column - VitalsCode	
 * Tech Column - VitalsCodeIIT
 */
 /*
  * Exam Area	
  * Data Element	
  * Comments	
  * BCH Form	
  * BCH Section	
  * BCH Data Element Display	
  * BCH Data Element Event Code
  */
CREATE OR REPLACE TEMP TABLE ec_ophtho_map (
cerner_form varchar(40),
cerner_section varchar(40),
cerner_event_display varchar(60),
cerner_event_code varchar(40),
epic_exam_area varchar(40),
epic_data_element varchar(40),
epic_cui varchar(40),
epic_format varchar(40),
comment varchar(80)
--cerner_usage float,
--cerner_percent float,
--cerner_scope varchar(2),
--epic_fdc_id varchar(40),
--epic_fdc_name varchar(40),
--epic_fdc_mpi_id varchar(40),
--hl7_assigning_codeing_system varchar(40),
--epic_flo_id varchar(40),
--epic_flo_name varchar(40),
--comments varchar(40),
--tech_column_vitalscode varchar(40),
--tech_column_vitalscodeIIT  varchar(40)
);

/* Copy contents in personal staging area to table */
 COPY INTO ec_ophtho_map FROM @~/ec_ophtho_map.csv 
  FILE_FORMAT = csv_ophtho,
  ON_ERROR = CONTINUE;

 /* Another option to insert contenst to temp table */
 /*
 INSERT INTO ec_ophtho_map (cerner_event_code,epic_concept) 
  VALUES (
	34567,4563
 	);
*/
 
 /*Verify load */
 --SELECT *
 --FROM ec_ophtho_map;
 

WITH ec_map as (
SELECT epic_exam_area,
       epic_data_element,
       epic_cui,
       epic_format,  /* String, 0 or 1, custom */
       cerner_form,
       cerner_section,
       cerner_event_display,
       cerner_event_code
from ec_ophtho_map
where cerner_event_code is not null
  and epic_data_element is not null
  --and cerner_event_code = '2820876'
),
ophthalmology_form AS (
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
       ce_docm.event_id,
       ce_docm.EVENT_END_DT_TM AS result_datetime,
       ce_docm.EVENT_TITLE_TEXT AS result_text, /* event_cd display */
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
WHERE ce_form.event_end_dt_tm >= dateadd(DAY,-365,current_date()) /* One year's worth of data */
  AND ce_form.valid_until_dt_tm > current_date() /* XAK2 */
  AND ce_form.result_status_cd IN (25,34,35) /* Auth, Modified,Modified */
),
ophthalmology_extract AS (
SELECT pa.alias AS MRN,
       ea.alias AS CSN,
       ec_map.epic_exam_area,
       ec_map.epic_data_element,
       ec_map.epic_cui,
       ec_map.epic_format,
       ophthalmology_form.*
from ophthalmology_form
JOIN ec_map 
  ON ec_map.cerner_event_code = ophthalmology_form.event_cd /* This is the mapping table */
 AND ec_map.cerner_form = ophthalmology_form.form_name
 AND ec_map.cerner_section = ophthalmology_form.section_name
JOIN person p 
  ON p.person_id = ophthalmology_form.person_id
AND p.name_last_key not like 'SYSTEMTEST%'
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
 ),
ophthalmology_transform as (
/* Transformation */
SELECT mrn,csn,
       form_name,
       section_name, 
       result_datetime,
       result_text, 
       event_cd,
       result_val,
       epic_exam_area,
       epic_data_element,
       epic_cui,
       epic_format,
       /* Transformation rules */
       case 
          /* 0 or 1 values in Epic */
          when result_text = 'Nystagmus' 
               then iff (result_val = 'Absent','0','1') 
          when epic_data_element in ('Right Eye Type','Left Eye Type','Type')       
               then case when form_name in ('Optical Rx (Soft Contacts)','Optical Rx (Specialty Contacts)')
                              then 'Contacts'
                    else 'Glasses' 
               end
          /* Date reformatting in YYYY-MM-DD HH:MM:SS */
          /* In CCL this would have been (https://community.cerner.com/t5/CCL-Discern-Explorer-Client-and-Cerner-Collaboration/Format-Date-in-Result-val-in-Clinical-event-table/m-p/772807)
                 format(cnvtdatetime(cnvtdate2(substring(3,8,result_val),"yyyymmdd"),
                                     cnvttime2(substring(11,6,result_val),"HHMMSS")),"mm/dd/yyyy hh:mm:ss;;d") */
          /* Date only */
          when result_text in ('Vision Correction Expiration Date GP',
                               'Vision Correction Expiration Date SCL')
               then substr(result_val,3,4) || '-' || substr(result_val,7,2) || '-' || substr(result_val,9,2) 
          /* Time only */     
          when result_text in ('Tonometry Time of Day')
              then substr(result_val,11,2) || ':' || substr(result_val,13,2) || ':' || substr(result_val,15,2)           
          when result_text in ('Vision Correction Substitutions SCL','Vision Correction Substitutions GP' )   
              then 'Substitution: ' || result_val
          when result_text in ('Vision Correction Disposal Schedule SCL','Vision Correction Disposal Schedule GP')
              then 'Disposal: '  || result_val
          when result_text in ('Vision Correction Contact Refill SCL','Vision Correction Contact Refill GP')
              then 'Refill: ' || result_val      
          when result_text in ('Vision Correction Ordering Provider SCL','Vision Correction Ordering Provider GP','Vision Correction Ordering Provider GL')
              then 'Ordered by: ' || result_val      
          when result_text in ('Vision Correction Reason For SCL','Vision Correction Reason For GP','Vision Correction Reason For GL')
              then 'Correction for: ' || result_val  
          when result_text in ('Vision Correction OD Contact Color GP','Vision Correction OS Contact Color GP')
              then 'Contact color: ' || result_val   
          when result_text in ('Vision Correction Type GL')
              then 'Glass Type: ' || result_val  
          when result_text in ('Vision Correction Prism Type GL')
              then 'Prism Type: ' || result_val  
          when result_text in ('Vision Correction Expiration Date GL')
              then 'Expiration: ' || substr(result_val,3,4) || '-' || substr(result_val,7,2) || '-' || substr(result_val,9,2) 
          when result_text in ('Vision Correct Recommend Lens Enhance GL')
              then 'Rec enhancement: ' || result_val
          when result_text in ('Correct Recommend Lens Enhance Sub GL')
              then 'Rec enhancement subs: ' || result_val
          else result_val
       end as epic_data_val
     --  listagg(result_val,',') within group(order by result_val)   
FROM ophthalmology_extract
ORDER BY mrn,csn,result_datetime,epic_exam_area,epic_data_element
),
ophthalmology_aggregation as (
 /* Fancy listagg of rows that occur more than once: https://stackoverflow.com/questions/68974553/snowflake-sql-concat-values-from-multiple-rows-based-on-shared-key */
 select mrn,csn,result_datetime,epic_exam_area,epic_data_element,epic_cui,
       listagg(epic_data_val,'; ') within group(order by result_text) as epic_data_val 
from ophthalmology_transform
group by mrn,csn,result_datetime,epic_exam_area,epic_data_element,epic_cui
ORDER BY mrn,csn,result_datetime,epic_exam_area,epic_data_element,epic_cui
)

/* Report aggregates, too */
select mrn,csn,result_datetime,epic_exam_area,epic_data_element,epic_cui,epic_data_val
from ophthalmology_aggregation
;
 