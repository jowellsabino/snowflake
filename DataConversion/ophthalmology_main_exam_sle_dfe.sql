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
 -- PUT file:///Users/jowell/Documents/Epic/ec_ophtho_map.csv @~/ec_ophtho_map.csv AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
 PUT file://Z:/Epic/Conversion/oph_map_sle_dfe.csv @~/ec_ophtho_map_sle_dfe.csv AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

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
 COPY INTO ec_ophtho_map FROM @~/ec_ophtho_map_sle_dfe.csv 
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
       ce_docm.VERIFIED_DT_TM AS result_datetime,
       --ce_docm.EVENT_END_DT_TM AS result_datetime,
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
WHERE ce_form.event_end_dt_tm >= dateadd(DAY,-10,current_date()) /* One year's worth of data */
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
       /* Transformation rules 
       case 
          /* 0 or 1 values in Epic 
          when result_text = 'Normal External Exam' 
               then iff (result_val = 'External exam unremarkable','1','0') 
          when result_text =  'Anterior Chamber OD' AND epic_data_element = 'Right Eye AC Normal'
               then iff(result_val = 'Unremarkable depth, no cells or flare','1','0') 
          when result_text =  'Anterior Chamber OS' AND epic_data_element = 'Left Eye AC Normal'
               then iff(result_val = 'Unremarkable depth, no cells or flare','1','0') 
          when result_text =  'Anterior Vitreous OD' AND epic_data_element = 'Right Eye Virteous Normal'
               then iff(result_val = 'Clear','1','0') 
          when result_text =  'Anterior Vitreous OS' AND epic_data_element = 'Left Eye Vitreous Normal'
               then iff(result_val = 'Clear','1','0') 
          when result_text =  'Conjunctiva/Sclera OD' AND epic_data_element = 'Right Eye Conjunctiva Normal'
               then iff(result_val = 'White and quiet','1','0') 
          when result_text =  'Conjunctiva/Sclera OS' AND epic_data_element = 'Left Eye Conjunctiva Normal'
               then iff(result_val = 'White and quiet','1','0') 
          when result_text =  'Cornea OD' AND epic_data_element = 'Right Eye Cornea Normal'
               then iff(result_val = 'Unremarkable tear film, epithelium, stroma','1','0') 
          when result_text =  'Cornea OS' AND epic_data_element = 'Left Eye Cornea Normal'
               then iff(result_val = 'Unremarkable tear film, epithelium, stroma','1','0') 
          when result_text =  'Disc OD' AND epic_data_element = 'Right Eye Disc Normal'
               then iff(result_val = 'Pink, flat, sharp margins','1','0') 
          when result_text =  'Disc OS' AND epic_data_element = 'Left Eye Disc Normal'
               then iff(result_val = 'Pink, flat, sharp margins','1','0') 
          when result_text =  'Iris OD' AND epic_data_element = 'Right Eye Iris Normal'
               then iff(result_val = 'Unremarkable','1','0') 
          when result_text =  'Iris OS' AND epic_data_element = 'Left Eye Iris Normal'
               then iff(result_val = 'Unremarkable','1','0') 
          when result_text =  'Lens OD' AND epic_data_element = 'Right Eye Lens Normal'
               then iff(result_val = 'Clear, with normal capsule, cortex, and nucleus','1','0') 
          when result_text =  'Lens OS' AND epic_data_element = 'Left Eye Lens Normal'
               then iff(result_val = 'Clear, with normal capsule, cortex, and nucleus','1','0') 
          when result_text =  'Lids, Lashes OD' AND epic_data_element = 'Right Eye Lids Normal'
               then iff(result_val = 'Unremarkable','1','0') 
          when result_text =  'Lids, Lashes OS' AND epic_data_element = 'Left Eye Lids Normal'
               then iff(result_val = 'Unremarkable','1','0') 
          when result_text =  'Macula OD' AND epic_data_element = 'Right Eye Macula Normal'
               then iff(result_val = 'Unremarkable','1','0') 
          when result_text =  'Macula OS' AND epic_data_element = 'Left Eye Macula Normal'
               then iff(result_val = 'Unremarkable','1','0') 
          when result_text =  'Periphery OD' AND epic_data_element = 'Right Eye Periphery Normal'
               then iff(result_val = 'Unremarkable','1','0') 
          when result_text =  'Periphery OS' AND epic_data_element = 'Left Eye Periphery Normal'
               then iff(result_val = 'Unremarkable','1','0') 
          when result_text =  'Vessels OD' AND epic_data_element = 'Right Eye Vessels Normal'
               then iff(result_val = 'Unremarkable course and caliber','1','0') 
          when result_text =  'Vessels OS' AND epic_data_element = 'Left Eye Vessels Normal'
               then iff(result_val = 'Unremarkable course and caliber','1','0') 
          when result_text =  'Vitreous OD' AND epic_data_element = 'Right Eye Vitreous Normal'
               then iff(result_val = 'Clear','1','0') 
          when result_text =  'Vitreous OS' AND epic_data_element = 'Left Eye Vitreous Normal'
               then iff(result_val = 'Clear','1','0') 
          when result_text = 'Iris Color OD'
              then 'Iris Color Right:' || result_val
          when result_text = 'Iris Color OS'
              then 'Iris Color Left:' || result_val

          /* 
          when epic_data_element in ('Right Eye Type','Left Eye Type','Type')       
               then case when form_name in ('Optical Rx (Soft Contacts)','Optical Rx (Specialty Contacts)')
                              then 'Contacts'
                    else 'Glasses' 
               end
          /* Date reformatting in YYYY-MM-DD HH:MM:SS */
          /* In CCL this would have been (https://community.cerner.com/t5/CCL-Discern-Explorer-Client-and-Cerner-Collaboration/Format-Date-in-Result-val-in-Clinical-event-table/m-p/772807)
                 format(cnvtdatetime(cnvtdate2(substring(3,8,result_val),"yyyymmdd"),
                                     cnvttime2(substring(11,6,result_val),"HHMMSS")),"mm/dd/yyyy hh:mm:ss;;d") 
          /* Date only 
          when result_text in ('Vision Correction Expiration Date GP',
                               'Vision Correction Expiration Date SCL')
               then substr(result_val,3,4) || '-' || substr(result_val,7,2) || '-' || substr(result_val,9,2) 
          /* Time only      
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
       end as epic_data_val */
       result_val as epic_data_val
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
),
ophthalmology_prepivot as (
/* Report aggregates, too */
select mrn,csn,result_datetime, replace(replace(epic_exam_area,' ','_'),'/','_') || '_' || replace(epic_data_element, ' ', '_') || '_' || replace(epic_cui,'#','_') as pivot_column, epic_data_val 
from ophthalmology_aggregation
)
select mrn,csn,result_datetime,pivot_column, epic_data_val
       -- case when pivot_column = 'Main Exam_Right_Eye_Conjunctiva_EPIC#OPH1012' then epic_data_val end as "Main Exam_Right_Eye_Conjunctiva_EPIC#OPH1012"  
from ophthalmology_prepivot
-- group by mrn,csn,result_datetime;




--ophthalmology_pivot(
/* Get comma-separated list of poivot column values */
--select '\'' || listagg(distinct pivot_column, '\', \'')
--  within group (order by pivot_column) || '\''
--from table(result_scan(last_query_id(-1)));
--select * 
--from table(result_scan(last_query_id(-1)))
--pivot(epic_data_val for pivot_column in ('EPIC#OPH1010', 'EPIC#OPH1011', 'EPIC#OPH1012', 'EPIC#OPH1013', 'EPIC#OPH1014', 'EPIC#OPH1015', 'EPIC#OPH1016', 'EPIC#OPH1017', 'EPIC#OPH1018', 'EPIC#OPH1019', 'EPIC#OPH1020', 'EPIC#OPH1021', 'EPIC#OPH1022', 'EPIC#OPH1023', 'EPIC#OPH1024', 'EPIC#OPH1051', 'EPIC#OPH1052', 'EPIC#OPH1053', 'EPIC#OPH1054', 'EPIC#OPH1055', 'EPIC#OPH1056', 'EPIC#OPH1057', 'EPIC#OPH1058', 'EPIC#OPH1090', 'EPIC#OPH1091'
--));