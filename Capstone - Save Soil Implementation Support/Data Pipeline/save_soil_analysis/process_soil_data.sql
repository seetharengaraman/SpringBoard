CREATE OR REPLACE PROCEDURE `my-gcpproject-123445.soil_om_analysis.process_soil_data`()
/* Processing raw agriculture and soil data for storing organic content incrementally every specific period of time thus monitoring the increase in organic content in areas/states/countries more involved in organic farming practices. This can further help in determining best practices*/
BEGIN
    /* Collecting USA statewise soil organic matter profile incrementally along with total acres of land in state
    that uses organic farming methods  */
    IF EXISTS(SELECT 1 FROM `soil_om_analysis.USA_STATEWISE_SOIL_ORGANIC_MATTER`) THEN
        INSERT INTO `soil_om_analysis.USA_STATEWISE_SOIL_ORGANIC_MATTER`
        SELECT current_date('America/New_York') AS ingestion_date,som.state_code,som.state_name,ulrd_total.year census_year,
        COALESCE(ulrd_total.Value,0.0) AS total_land_in_acres,ulrd_organic.year organic_census_year,COALESCE(ulrd_organic.Value,0.0) organic_land_in_acres,OMRVg_p05,OMRVg_p10,OMRVg_p50,ROUND(OMRVg_mean,2) AS OMRVg_mean,OMRVg_p90,OMRVg_p95
        FROM `soil_raw_dataset.US_STATE_SOIL_ORGANIC_MATTER_PROFILE` som
        INNER JOIN `soil_raw_dataset.US_AGRI_LAND_RAW_DATA` ulrd_total
        ON som.state_code = ulrd_total.state_alpha
        AND ulrd_total.short_desc = 'AG LAND, CROPLAND - ACRES'
        INNER JOIN `soil_raw_dataset.US_AGRI_LAND_RAW_DATA` ulrd_organic
        ON som.state_code = ulrd_organic.STATE_ALPHA
        AND ulrd_organic.short_desc = 'AG LAND, CROPLAND, ORGANIC - ACRES'
        WHERE ulrd_organic.year = (SELECT max(year) FROM `soil_raw_dataset.US_AGRI_LAND_RAW_DATA` WHERE short_desc = 'AG LAND, CROPLAND, ORGANIC - ACRES')
        AND ulrd_total.year = (SELECT max(year) FROM `soil_raw_dataset.US_AGRI_LAND_RAW_DATA` WHERE short_desc = 'AG LAND, CROPLAND - ACRES');
    ELSE
        CREATE TABLE IF NOT EXISTS `soil_om_analysis.USA_STATEWISE_SOIL_ORGANIC_MATTER`
        AS
        SELECT current_date('America/New_York') AS ingestion_date,som.state_code,som.state_name,ulrd_total.year census_year,
        COALESCE(ulrd_total.Value,0.0) AS total_land_in_acres,ulrd_organic.year organic_census_year,COALESCE(ulrd_organic.Value,0.0) organic_land_in_acres,OMRVg_p05,OMRVg_p10,OMRVg_p50,ROUND(OMRVg_mean,2) AS OMRVg_mean,OMRVg_p90,OMRVg_p95
        FROM `soil_raw_dataset.US_STATE_SOIL_ORGANIC_MATTER_PROFILE` som
        INNER JOIN `soil_raw_dataset.US_AGRI_LAND_RAW_DATA` ulrd_total
        ON som.state_code = ulrd_total.state_alpha
        AND ulrd_total.short_desc = 'AG LAND, CROPLAND - ACRES'
        INNER JOIN `soil_raw_dataset.US_AGRI_LAND_RAW_DATA` ulrd_organic
        ON som.state_code = ulrd_organic.STATE_ALPHA
        AND ulrd_organic.short_desc = 'AG LAND, CROPLAND, ORGANIC - ACRES'
        WHERE ulrd_organic.year = (SELECT max(year) FROM `soil_raw_dataset.US_AGRI_LAND_RAW_DATA` WHERE short_desc = 'AG LAND, CROPLAND, ORGANIC - ACRES')
        AND ulrd_total.year = (SELECT max(year) FROM `soil_raw_dataset.US_AGRI_LAND_RAW_DATA` WHERE short_desc = 'AG LAND, CROPLAND - ACRES');
    END IF;

    /* Collecting survey area map geography where organic content was measured in the US states */
    CREATE OR REPLACE TABLE `soil_om_analysis.USA_STATE_SURVEY_MAP_AREA`
    AS
    SELECT DISTINCT somr.state_name,usma.area_symbol,usma.legend_key,usma.survey_area_geometry
    FROM `soil_raw_dataset.US_SOIL_ORGANIC_MATTER_RAW` somr
    INNER JOIN `soil_raw_dataset.US_SURVEY_MAP_AREA` usma
    ON somr.area_symbol = usma.area_symbol;

    ALTER TABLE `soil_om_analysis.USA_STATE_SURVEY_MAP_AREA` ADD COLUMN IF NOT EXISTS geometry GEOGRAPHY;

    UPDATE `soil_om_analysis.USA_STATE_SURVEY_MAP_AREA`
    SET geometry = ST_GEOGFROMTEXT(survey_area_geometry,make_valid => TRUE)
    WHERE 1=1;

    ALTER TABLE `soil_om_analysis.USA_STATE_SURVEY_MAP_AREA` DROP COLUMN survey_area_geometry;

    /* Converting World countries map and World Soil map geometry from string to geography datatype */

    ALTER TABLE `soil_raw_dataset.HARMONIZED_WORLD_SOIL_MAP` ADD COLUMN IF NOT EXISTS map_unit_geometry GEOGRAPHY;
    ALTER TABLE `soil_raw_dataset.WORLD_COUNTRIES_MAP` ADD COLUMN IF NOT EXISTS country_geometry GEOGRAPHY;

    UPDATE `soil_raw_dataset.HARMONIZED_WORLD_SOIL_MAP`
    SET map_unit_geometry = ST_GEOGFROMTEXT(geometry,make_valid => TRUE)
    WHERE 1=1;

    UPDATE `soil_raw_dataset.WORLD_COUNTRIES_MAP`
    SET country_geometry = ST_GEOGFROMTEXT(geometry,make_valid => TRUE)
    WHERE 1=1;

    /* Adding Soil Map geography to soil mapping units across the world along with associating country to the mapping units using countries map geometry with ST_INTERSECT Big Query Geography Function */
    CREATE OR REPLACE TABLE `soil_om_analysis.HARMONIZED_WORLD_SOIL_MAPPING_UNITS`
    AS
    SELECT wm.iso,wm.country,MU_GLOBAL AS global_map_unit_id,hs.SU_SYMBOL AS map_unit_symbol, 
    ds.VALUE AS map_unit_description, dc.VALUE AS coverage,hm.map_unit_geometry
    FROM `soil_raw_dataset.HWSD_SMU` hs 
    INNER JOIN `soil_raw_dataset.D_COVERAGE` dc ON dc.CODE = hs.COVERAGE
    INNER JOIN `soil_raw_dataset.D_SYMBOL` ds ON ds.SYMBOL = hs.SU_SYMBOL
    LEFT JOIN `soil_raw_dataset.HARMONIZED_WORLD_SOIL_MAP` hm ON hm.DN = hs.MU_GLOBAL
    INNER JOIN `soil_raw_dataset.WORLD_COUNTRIES_MAP` wm ON ST_INTERSECTS(hm.map_unit_geometry,wm.country_geometry) = TRUE;

/* Storing soil characteristics across the world expanding codes into descriptions using the configuration reference data */
    CREATE OR REPLACE TABLE `soil_om_analysis.HARMONIZED_WORLD_SOIL_DATA`
    AS
    WITH world_soil_mapping_units AS
    (SELECT DISTINCT country,iso ,coverage,map_unit_symbol,map_unit_description,global_map_unit_id
    FROM `soil_om_analysis.HARMONIZED_WORLD_SOIL_MAPPING_UNITS`)
    SELECT hwsd.MU_GLOBAL AS global_map_unit_id,hwsmu.country,hwsmu.iso ,hwsmu.coverage,hwsmu.map_unit_symbol,hwsmu.map_unit_description,
    CASE WHEN hwsd.ISSOIL = 0 THEN
      "Non-Soil"
    ELSE
      "Soil"
    END is_soil,
    ds74.VALUE soil_unit_name_74,ds74.SYMBOL soil_unit_symbol_74,ds85.VALUE soil_unit_name_85,ds85.SYMBOL soil_unit_symbol_85,
    ds90.VALUE soil_unit_name_90,ds90.SYMBOL soil_unit_symbol_90,hwsd.SEQ soil_unit_sequence,hwsd.SHARE soil_unit_share,
    dt.VALUE generic_texture,dd.VALUE drainage,hwsd.REF_DEPTH reference_depth_of_soil_unit,da.VALUE available_water_storage_capacity_mm_per_m,
    dp1.VALUE land_use_capability_phase_1,dp2.VALUE land_use_capability_phase_2,dr.VALUE obstacle_to_roots,di.VALUE impermeable_layer,ds.VALUE soil_water_regime,
    dap.VALUE agriculture_use_property,STRUCT(dutct.VALUE AS usda_texture_class,hwsd.T_GRAVEL AS gravel_percent,hwsd.T_SAND AS sand_percent,hwsd.T_SILT AS silt_percent,
    hwsd.T_CLAY AS clay_percent,hwsd.T_REF_BULK_DENSITY AS reference_bulk_density,hwsd.T_OC AS organic_carbon,hwsd.T_PH_H2O AS in_water_pH,hwsd.T_CEC_CLAY AS cation_exchange_clay,
    hwsd.T_CEC_SOIL AS cation_exchange,hwsd.T_BS AS base_saturation,hwsd.T_TEB AS total_exchangeable_bases,hwsd.T_CACO3 AS lime_content,hwsd.T_CASO4 AS gypsum_content,
    hwsd.T_ESP AS exchangeable_sodium_percent,hwsd.T_ECE AS electrical_conductivity) AS top_soil,
    STRUCT(dutcs.VALUE AS usda_texture_class,hwsd.S_GRAVEL AS gravel_percent,hwsd.S_SAND AS sand_percent,hwsd.S_SILT AS silt_percent,
    hwsd.S_CLAY AS clay_percent,hwsd.S_REF_BULK_DENSITY AS reference_bulk_density,hwsd.S_OC AS organic_carbon,hwsd.S_PH_H2O AS in_water_pH,hwsd.S_CEC_CLAY AS cation_exchange_clay,
    hwsd.S_CEC_SOIL AS cation_exchange,hwsd.S_BS AS base_saturation,hwsd.S_TEB AS total_exchangeable_bases,hwsd.S_CACO3 AS lime_content,hwsd.S_CASO4 AS gypsum_content,
    hwsd.S_ESP AS exchangeable_sodium_percent,hwsd.S_ECE AS electrical_conductivity) AS sub_soil
    FROM `soil_raw_dataset.HWSD_DATA` hwsd
    INNER JOIN world_soil_mapping_units hwsmu ON hwsd.MU_GLOBAL = hwsmu.global_map_unit_id
    LEFT JOIN `soil_raw_dataset.D_USDA_TEX_CLASS` dutct ON dutct.CODE = hwsd.T_USDA_TEX_CLASS
    LEFT JOIN `soil_raw_dataset.D_USDA_TEX_CLASS` dutcs ON dutcs.CODE = hwsd.S_USDA_TEX_CLASS
    LEFT JOIN `soil_raw_dataset.D_SYMBOL90` ds90 ON ds90.SYMBOL = hwsd.SU_SYM90
    LEFT JOIN `soil_raw_dataset.D_SYMBOL74` ds74 ON ds74.SYMBOL = hwsd.SU_SYM74
    LEFT JOIN `soil_raw_dataset.D_SYMBOL85` ds85 ON ds85.SYMBOL = hwsd.SU_SYM85
    LEFT JOIN `soil_raw_dataset.D_ADD_PROP` dap ON dap.CODE = hwsd.ADD_PROP
    LEFT JOIN `soil_raw_dataset.D_AWC` da ON da.CODE = hwsd.AWC_CLASS
    LEFT JOIN `soil_raw_dataset.D_DRAINAGE` dd ON dd.CODE = hwsd.DRAINAGE
    LEFT JOIN `soil_raw_dataset.D_IL` di ON di.CODE = hwsd.IL
    LEFT JOIN `soil_raw_dataset.D_PHASE` dp1 ON dp1.CODE = hwsd.PHASE1
    LEFT JOIN `soil_raw_dataset.D_PHASE` dp2 ON dp2.CODE = hwsd.PHASE2
    LEFT JOIN `soil_raw_dataset.D_ROOTS` dr ON dr.CODE = hwsd.ROOTS
    LEFT JOIN `soil_raw_dataset.D_SWR` ds ON ds.CODE = hwsd.SWR
    LEFT JOIN `soil_raw_dataset.D_TEXTURE` dt ON dt.CODE = hwsd.T_TEXTURE
    ORDER BY hwsmu.country,hwsmu.global_map_unit_id,hwsd.SEQ,hwsd.SHARE;

/* Collecting Country based soil organic matter profile incrementally to determine countries with best organic farming practices */
  IF EXISTS(SELECT 1 FROM `soil_om_analysis.WORLD_COUNTRYWISE_SOIL_ORGANIC_CARBON`) THEN
    INSERT INTO `soil_om_analysis.WORLD_COUNTRYWISE_SOIL_ORGANIC_CARBON`
    SELECT DISTINCT current_date('America/New_York') AS ingestion_date,iso,country,
    ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.05) OVER(PARTITION BY country),2) AS TOC_p05, 
    ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.1) OVER(PARTITION BY country),2) AS TOC_p10, 
    ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.5) OVER(PARTITION BY country),2) AS TOC_p50, 
    ROUND(AVG(top_soil.organic_carbon) OVER(PARTITION BY country),2) AS TOC_mean, 
    ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.9) OVER(PARTITION BY country),2) AS TOC_p90, 
    ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.95) OVER(PARTITION BY country),2) AS TOC_p95, 
    ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.05) OVER(PARTITION BY country),2) AS SOC_p05, 
    ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.1) OVER(PARTITION BY country),2) AS SOC_p10, 
    ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.5) OVER(PARTITION BY country),2) AS SOC_p50, 
    ROUND(AVG(sub_soil.organic_carbon) OVER(PARTITION BY country),2) AS SOC_mean, 
    ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.9) OVER(PARTITION BY country),2) AS SOC_p90, 
    ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.95) OVER(PARTITION BY country),2) AS SOC_p95
    FROM `soil_om_analysis.HARMONIZED_WORLD_SOIL_DATA`;
  ELSE
    CREATE OR REPLACE TABLE `soil_om_analysis.WORLD_COUNTRYWISE_SOIL_ORGANIC_CARBON`
    AS
      SELECT DISTINCT current_date('America/New_York') AS ingestion_date,iso,country,
      ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.05) OVER(PARTITION BY country),2) AS TOC_p05, 
      ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.1) OVER(PARTITION BY country),2) AS TOC_p10, 
      ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.5) OVER(PARTITION BY country),2) AS TOC_p50, 
      ROUND(AVG(top_soil.organic_carbon) OVER(PARTITION BY country),2) AS TOC_mean, 
      ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.9) OVER(PARTITION BY country),2) AS TOC_p90, 
      ROUND(PERCENTILE_CONT(top_soil.organic_carbon,0.95) OVER(PARTITION BY country),2) AS TOC_p95, 
      ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.05) OVER(PARTITION BY country),2) AS SOC_p05, 
      ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.1) OVER(PARTITION BY country),2) AS SOC_p10, 
      ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.5) OVER(PARTITION BY country),2) AS SOC_p50, 
      ROUND(AVG(sub_soil.organic_carbon) OVER(PARTITION BY country),2) AS SOC_mean, 
      ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.9) OVER(PARTITION BY country),2) AS SOC_p90, 
      ROUND(PERCENTILE_CONT(sub_soil.organic_carbon,0.95) OVER(PARTITION BY country),2) AS SOC_p95
      FROM `soil_om_analysis.HARMONIZED_WORLD_SOIL_DATA`;
  END IF;      

END;