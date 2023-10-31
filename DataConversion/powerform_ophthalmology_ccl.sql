sselect 
      form_name=trim(dfr.description,3) 
    , form_def=trim(dfr.definition,3) 
    , section=trim(dsr.description,3) 
    , section_def=trim(dsr.definition,3) 
    , event_code_display=uar_get_code_display(dta.event_cd)
    , dta.event_cd
    , DTA.task_assay_cd
    , dta_mnemonic=dta.mnemonic
;    , nvp.merge_id, dta.task_assay_cd, nvp.parent_entity_id, dta.*
from 
      dcp_forms_ref dfr 
    , dcp_forms_def dfd 
    , dcp_section_ref dsr 
    , dcp_input_ref dir 
    , name_value_prefs nvp 
    , discrete_task_assay dta
plan dfr 
    where dfr.active_ind = 1
      and dfr.beg_effective_dt_tm < sysdate
      and dfr.end_effective_dt_tm > sysdate
      and dfr.definition = "Optical Rx*"
join dfd 
    where dfd.dcp_form_instance_id = dfr.dcp_form_instance_id 
    and dfd.active_ind = 1
join dsr 
    where dsr.dcp_section_ref_id = dfd.dcp_section_ref_id 
    and dsr.active_ind = 1
    and dsr.beg_effective_dt_tm < sysdate
    and dsr.end_effective_dt_tm > sysdate
join dir 
    where dir.dcp_section_instance_id = dsr.dcp_section_instance_id 
    and dir.active_ind = 1 
    and dir.input_type not in (0,1,21) /* ?? */
join nvp /* Why??? */
    where nvp.parent_entity_id = dir.dcp_input_ref_id 
    and nvp.parent_entity_name = "DCP_INPUT_REF" 
    and nvp.merge_name = "DISCRETE_TASK_ASSAY"
    and nvp.merge_id > 0
join dta 
   ; where dta.mnemonic = "Vision Correction OD Sph GL" 
    where dta.task_assay_cd = nvp.merge_id
order by form_name, form_def,section, section_def,event_code_display