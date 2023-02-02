WITH CAPMAN_ATTRIBUTE AS (
SELECT location.institution_display as organization,
       location.building_display as building,
       location.floor_display as floor_level,
       location.unit_display as nurse_unit,
       location.room_display as room,
       location.bed_display as bed,
       capman_reference_attribute.display as location_attribute
  FROM  BOSTONCHILDRENS_CHLDMA_PROD1.CA_D_LOCATION location  
  LEFT
  JOIN BOSTONCHILDRENS_CHLDMA_PROD1.CA_F_CAPMAN_LOCATION_ATTRIBUTE capman_location_attribute
    ON location.location_id = capman_location_attribute.location_id
  LEFT
  JOIN BOSTONCHILDRENS_CHLDMA_PROD1.CA_D_CAPMAN_REFERENCE_ATTRIBUTE capman_reference_attribute
    ON capman_reference_attribute.reference_attribute_id = capman_location_attribute.reference_attribute_id    
    where location.delete_ind = false
     and location.active_ind = 1
      and location.location_type = 'Bed'
      and capman_location_attribute.end_dttm is null   
  order by 
       location.institution_display,
       location.building_display,
       location.floor_display,
       location.unit_display,
       location.room_display,
       location.bed_display
)

select 
       organization,
       building,
       floor_level,
       nurse_unit,
       room,
       bed,
       listagg(location_attribute,', ') as location_attributes
from capman_attribute
where floor_level = '1st Floor'
group by organization,
       building,
       floor_level,
       nurse_unit,
       room,
       bed
order by organization,
       building,
       floor_level,
       nurse_unit,
       room,
       bed
;