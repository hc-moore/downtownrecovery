# Databricks notebook source
import pandas as pd
import numpy as np
import requests
import json
import re

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

can_city_index = get_table_as_pandas_df("can_city_index_da_0406")
can_census_data = get_table_as_pandas_df("canadian_census_data_0626_csv")
can_cbp_data = get_table_as_pandas_df("cpb2016da__2011__csv")

# COMMAND ----------

can_city_index

# COMMAND ----------

can_city_index["downtown_da"] = can_city_index["downtown_da"].apply(lambda x: re.findall(r'[0-9]+', x))
can_city_index["ccs_da"] = can_city_index["ccs_da"].apply(lambda x: re.findall(r'[0-9]+', x))

# COMMAND ----------

can_census_data

# COMMAND ----------

can_cbp_data

# COMMAND ----------

can_cbp_data["naics_11"] = can_cbp_data[[ '111110_-_Soybean_farming',
 '111120_-_Oilseed__except_soybean__farming',
 '111130_-_Dry_pea_and_bean_farming',
 '111140_-_Wheat_farming',
 '111150_-_Corn_farming',
 '111160_-_Rice_farming',
 '111190_-_Other_grain_farming',
 '111211_-_Potato_farming',
 '111219_-_Other_vegetable__except_potato__and_melon_farming',
 '111310_-_Orange_groves',
 '111320_-_Citrus__except_orange__groves',
 '111330_-_Non-citrus_fruit_and_tree_nut_farming',
 '111411_-_Mushroom_production',
 '111419_-_Other_food_crops_grown_under_cover',
 '111421_-_Nursery_and_tree_production',
 '111422_-_Floriculture_production',
 '111910_-_Tobacco_farming',
 '111920_-_Cotton_farming',
 '111930_-_Sugar_cane_farming',
 '111940_-_Hay_farming',
 '111993_-_Fruit_and_vegetable_combination_farming',
 '111994_-_Maple_syrup_and_products_production',
 '111999_-_All_other_miscellaneous_crop_farming',
 '112110_-_Beef_cattle_ranching_and_farming__including_feedlots',
 '112120_-_Dairy_cattle_and_milk_production',
 '112210_-_Hog_and_pig_farming',
 '112310_-_Chicken_egg_production',
 '112320_-_Broiler_and_other_meat-type_chicken_production',
 '112330_-_Turkey_production',
 '112340_-_Poultry_hatcheries',
 '112391_-_Combination_poultry_and_egg_production',
 '112399_-_All_other_poultry_production',
 '112410_-_Sheep_farming',
 '112420_-_Goat_farming',
 '112510_-_Aquaculture',
 '112910_-_Apiculture',
 '112920_-_Horse_and_other_equine_production',
 '112930_-_Fur-bearing_animal_and_rabbit_production',
 '112991_-_Animal_combination_farming',
 '112999_-_All_other_miscellaneous_animal_production',
 '113110_-_Timber_tract_operations',
 '113210_-_Forest_nurseries_and_gathering_of_forest_products',
 '113311_-_Logging__except_contract_',
 '113312_-_Contract_logging',
 '114113_-_Salt_water_fishing',
 '114114_-_Freshwater_fishing',
 '114210_-_Hunting_and_trapping',
 '115110_-_Support_activities_for_crop_production',
 '115210_-_Support_activities_for_animal_production',
 '115310_-_Support_activities_for_forestry']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data["naics_21"] = can_cbp_data[['211113_-_Conventional_oil_and_gas_extraction',
 '211114_-_Non-conventional_oil_extraction',
 '212114_-_Bituminous_coal_mining',
 '212115_-_Subbituminous_coal_mining',
 '212116_-_Lignite_coal_mining',
 '212210_-_Iron_ore_mining',
 '212220_-_Gold_and_silver_ore_mining',
 '212231_-_Lead-zinc_ore_mining',
 '212232_-_Nickel-copper_ore_mining',
 '212233_-_Copper-zinc_ore_mining',
 '212291_-_Uranium_ore_mining',
 '212299_-_All_other_metal_ore_mining',
 '212314_-_Granite_mining_and_quarrying',
 '212315_-_Limestone_mining_and_quarrying',
 '212316_-_Marble_mining_and_quarrying',
 '212317_-_Sandstone_mining_and_quarrying',
 '212323_-_Sand_and_gravel_mining_and_quarrying',
 '212326_-_Shale__clay_and_refractory_mineral_mining_and_quarrying',
 '212392_-_Diamond_mining',
 '212393_-_Salt_mining',
 '212394_-_Asbestos_mining',
 '212395_-_Gypsum_mining',
 '212396_-_Potash_mining',
 '212397_-_Peat_extraction',
 '212398_-_All_other_non-metallic_mineral_mining_and_quarrying',
 '213111_-_Oil_and_gas_contract_drilling',
 '213117_-_Contract_drilling__except_oil_and_gas_',
 '213118_-_Services_to_oil_and_gas_extraction',
 '213119_-_Other_support_activities_for_mining']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data["naics_22"] = can_cbp_data[['221111_-_Hydro-electric_power_generation',
 '221112_-_Fossil-fuel_electric_power_generation',
 '221113_-_Nuclear_electric_power_generation',
 '221119_-_Other_electric_power_generation',
 '221121_-_Electric_bulk_power_transmission_and_control',
 '221122_-_Electric_power_distribution',
 '221210_-_Natural_gas_distribution',
 '221310_-_Water_supply_and_irrigation_systems',
 '221320_-_Sewage_treatment_facilities',
 '221330_-_Steam_and_air-conditioning_supply']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data["naics_23"] = can_cbp_data[['236110_-_Residential_building_construction',
 '236210_-_Industrial_building_and_structure_construction',
 '236220_-_Commercial_and_institutional_building_construction',
 '237110_-_Water_and_sewer_line_and_related_structures_construction',
 '237120_-_Oil_and_gas_pipeline_and_related_structures_construction',
 '237130_-_Power_and_communication_line_and_related_structures_construction',
 '237210_-_Land_subdivision',
 '237310_-_Highway__street_and_bridge_construction',
 '237990_-_Other_heavy_and_civil_engineering_construction',
 '238110_-_Poured_concrete_foundation_and_structure_contractors',
 '238120_-_Structural_steel_and_precast_concrete_contractors',
 '238130_-_Framing_contractors',
 '238140_-_Masonry_contractors',
 '238150_-_Glass_and_glazing_contractors',
 '238160_-_Roofing_contractors',
 '238170_-_Siding_contractors',
 '238190_-_Other_foundation__structure_and_building_exterior_contractors',
 '238210_-_Electrical_contractors_and_other_wiring_installation_contractors',
 '238220_-_Plumbing__heating_and_air-conditioning_contractors',
 '238291_-_Elevator_and_escalator_installation_contractors',
 '238299_-_All_other_building_equipment_contractors',
 '238310_-_Drywall_and_insulation_contractors',
 '238320_-_Painting_and_wall_covering_contractors',
 '238330_-_Flooring_contractors',
 '238340_-_Tile_and_terrazzo_contractors',
 '238350_-_Finish_carpentry_contractors',
 '238390_-_Other_building_finishing_contractors',
 '238910_-_Site_preparation_contractors',
 '238990_-_All_other_specialty_trade_contractors']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data["naics_31-33"] = can_cbp_data[['311111_-_Dog_and_cat_food_manufacturing',
 '311119_-_Other_animal_food_manufacturing',
 '311211_-_Flour_milling',
 '311214_-_Rice_milling_and_malt_manufacturing',
 '311221_-_Wet_corn_milling',
 '311224_-_Oilseed_processing',
 '311225_-_Fat_and_oil_refining_and_blending',
 '311230_-_Breakfast_cereal_manufacturing',
 '311310_-_Sugar_manufacturing',
 '311340_-_Non-chocolate_confectionery_manufacturing',
 '311351_-_Chocolate_and_chocolate__confectionery_manufacturing_from_cacao_beans',
 '311352_-_Confectionery_manufacturing_from_purchased_chocolate',
 '311410_-_Frozen_food_manufacturing',
 '311420_-_Fruit_and_vegetable_canning__pickling_and_drying',
 '311511_-_Fluid_milk_manufacturing',
 '311515_-_Butter__cheese__and_dry_and_condensed_dairy_product_manufacturing',
 '311520_-_Ice_cream_and_frozen_dessert_manufacturing',
 '311611_-_Animal__except_poultry__slaughtering',
 '311614_-_Rendering_and_meat_processing_from_carcasses',
 '311615_-_Poultry_processing',
 '311710_-_Seafood_product_preparation_and_packaging',
 '311811_-_Retail_bakeries',
 '311814_-_Commercial_bakeries_and_frozen_bakery_product_manufacturing',
 '311821_-_Cookie_and_cracker_manufacturing',
 '311824_-_Flour_mixes__dough__and_pasta_manufacturing_from_purchased_flour',
 '311830_-_Tortilla_manufacturing',
 '311911_-_Roasted_nut_and_peanut_butter_manufacturing',
 '311919_-_Other_snack_food_manufacturing',
 '311920_-_Coffee_and_tea_manufacturing',
 '311930_-_Flavouring_syrup_and_concentrate_manufacturing',
 '311940_-_Seasoning_and_dressing_manufacturing',
 '311990_-_All_other_food_manufacturing',
 '312110_-_Soft_drink_and_ice_manufacturing',
 '312120_-_Breweries',
 '312130_-_Wineries',
 '312140_-_Distilleries',
 '312210_-_Tobacco_stemming_and_redrying',
 '312220_-_Tobacco_product_manufacturing',
 '313110_-_Fibre__yarn_and_thread_mills',
 '313210_-_Broad-woven_fabric_mills',
 '313220_-_Narrow_fabric_mills_and_Schiffli_machine_embroidery',
 '313230_-_Nonwoven_fabric_mills',
 '313240_-_Knit_fabric_mills',
 '313310_-_Textile_and_fabric_finishing',
 '313320_-_Fabric_coating',
 '314110_-_Carpet_and_rug_mills',
 '314120_-_Curtain_and_linen_mills',
 '314910_-_Textile_bag_and_canvas_mills',
 '314990_-_All_other_textile_product_mills',
 '315110_-_Hosiery_and_sock_mills',
 '315190_-_Other_clothing_knitting_mills',
 '315210_-_Cut_and_sew_clothing_contracting',
 '315220_-_Men_s_and_boys__cut_and_sew_clothing_manufacturing',
 '315241_-_Infants__cut_and_sew_clothing_manufacturing',
 '315249_-_Women_s_and_girls__cut_and_sew_clothing_manufacturing',
 '315281_-_Fur_and_leather_clothing_manufacturing',
 '315289_-_All_other_cut_and_sew_clothing_manufacturing',
 '315990_-_Clothing_accessories_and_other_clothing_manufacturing',
 '316110_-_Leather_and_hide_tanning_and_finishing',
 '316210_-_Footwear_manufacturing',
 '316990_-_Other_leather_and_allied_product_manufacturing',
 '321111_-_Sawmills__except_shingle_and_shake_mills_',
 '321112_-_Shingle_and_shake_mills',
 '321114_-_Wood_preservation',
 '321211_-_Hardwood_veneer_and_plywood_mills',
 '321212_-_Softwood_veneer_and_plywood_mills',
 '321215_-_Structural_wood_product_manufacturing',
 '321216_-_Particle_board_and_fibreboard_mills',
 '321217_-_Waferboard_mills',
 '321911_-_Wood_window_and_door_manufacturing',
 '321919_-_Other_millwork',
 '321920_-_Wood_container_and_pallet_manufacturing',
 '321991_-_Manufactured__mobile__home_manufacturing',
 '321992_-_Prefabricated_wood_building_manufacturing',
 '321999_-_All_other_miscellaneous_wood_product_manufacturing',
 '322111_-_Mechanical_pulp_mills',
 '322112_-_Chemical_pulp_mills',
 '322121_-_Paper__except_newsprint__mills',
 '322122_-_Newsprint_mills',
 '322130_-_Paperboard_mills',
 '322211_-_Corrugated_and_solid_fibre_box_manufacturing',
 '322212_-_Folding_paperboard_box_manufacturing',
 '322219_-_Other_paperboard_container_manufacturing',
 '322220_-_Paper_bag_and_coated_and_treated_paper_manufacturing',
 '322230_-_Stationery_product_manufacturing',
 '322291_-_Sanitary_paper_product_manufacturing',
 '322299_-_All_other_converted_paper_product_manufacturing',
 '323113_-_Commercial_screen_printing',
 '323114_-_Quick_printing',
 '323115_-_Digital_printing',
 '323116_-_Manifold_business_forms_printing',
 '323119_-_Other_printing',
 '323120_-_Support_activities_for_printing',
 '324110_-_Petroleum_refineries',
 '324121_-_Asphalt_paving_mixture_and_block_manufacturing',
 '324122_-_Asphalt_shingle_and_coating_material_manufacturing',
 '324190_-_Other_petroleum_and_coal_product_manufacturing',
 '325110_-_Petrochemical_manufacturing',
 '325120_-_Industrial_gas_manufacturing',
 '325130_-_Synthetic_dye_and_pigment_manufacturing',
 '325181_-_Alkali_and_chlorine_manufacturing',
 '325189_-_All_other_basic_inorganic_chemical_manufacturing',
 '325190_-_Other_basic_organic_chemical_manufacturing',
 '325210_-_Resin_and_synthetic_rubber_manufacturing',
 '325220_-_Artificial_and_synthetic_fibres_and_filaments_manufacturing',
 '325313_-_Chemical_fertilizer__except_potash__manufacturing',
 '325314_-_Mixed_fertilizer_manufacturing',
 '325320_-_Pesticide_and_other_agricultural_chemical_manufacturing',
 '325410_-_Pharmaceutical_and_medicine_manufacturing',
 '325510_-_Paint_and_coating_manufacturing',
 '325520_-_Adhesive_manufacturing',
 '325610_-_Soap_and_cleaning_compound_manufacturing',
 '325620_-_Toilet_preparation_manufacturing',
 '325910_-_Printing_ink_manufacturing',
 '325920_-_Explosives_manufacturing',
 '325991_-_Custom_compounding_of_purchased_resins',
 '325999_-_All_other_miscellaneous_chemical_product_manufacturing',
 '326111_-_Plastic_bag_and_pouch_manufacturing',
 '326114_-_Plastic_film_and_sheet_manufacturing',
 '326121_-_Unlaminated_plastic_profile_shape_manufacturing',
 '326122_-_Plastic_pipe_and_pipe_fitting_manufacturing',
 '326130_-_Laminated_plastic_plate__sheet__except_packaging___and_shape_manufacturing',
 '326140_-_Polystyrene_foam_product_manufacturing',
 '326150_-_Urethane_and_other_foam_product__except_polystyrene__manufacturing',
 '326160_-_Plastic_bottle_manufacturing',
 '326191_-_Plastic_plumbing_fixture_manufacturing',
 '326193_-_Motor_vehicle_plastic_parts_manufacturing',
 '326196_-_Plastic_window_and_door_manufacturing',
 '326198_-_All_other_plastic_product_manufacturing',
 '326210_-_Tire_manufacturing',
 '326220_-_Rubber_and_plastic_hose_and_belting_manufacturing',
 '326290_-_Other_rubber_product_manufacturing',
 '327110_-_Pottery__ceramics_and_plumbing_fixture_manufacturing',
 '327120_-_Clay_building_material_and_refractory_manufacturing',
 '327214_-_Glass_manufacturing',
 '327215_-_Glass_product_manufacturing_from_purchased_glass',
 '327310_-_Cement_manufacturing',
 '327320_-_Ready-mix_concrete_manufacturing',
 '327330_-_Concrete_pipe__brick_and_block_manufacturing',
 '327390_-_Other_concrete_product_manufacturing',
 '327410_-_Lime_manufacturing',
 '327420_-_Gypsum_product_manufacturing',
 '327910_-_Abrasive_product_manufacturing',
 '327990_-_All_other_non-metallic_mineral_product_manufacturing',
 '331110_-_Iron_and_steel_mills_and_ferro-alloy_manufacturing',
 '331210_-_Iron_and_steel_pipes_and_tubes_manufacturing_from_purchased_steel',
 '331221_-_Cold-rolled_steel_shape_manufacturing',
 '331222_-_Steel_wire_drawing',
 '331313_-_Primary_production_of_alumina_and_aluminum',
 '331317_-_Aluminum_rolling__drawing__extruding_and_alloying',
 '331410_-_Non-ferrous_metal__except_aluminum__smelting_and_refining',
 '331420_-_Copper_rolling__drawing__extruding_and_alloying',
 '331490_-_Non-ferrous_metal__except_copper_and_aluminum__rolling__drawing__extruding_and_alloying',
 '331511_-_Iron_foundries',
 '331514_-_Steel_foundries',
 '331523_-_Non-ferrous_die-casting_foundries',
 '331529_-_Non-ferrous_foundries__except_die-casting_',
 '332113_-_Forging',
 '332118_-_Stamping',
 '332210_-_Cutlery_and_hand_tool_manufacturing',
 '332311_-_Prefabricated_metal_building_and_component_manufacturing',
 '332314_-_Concrete_reinforcing_bar_manufacturing',
 '332319_-_Other_plate_work_and_fabricated_structural_product_manufacturing',
 '332321_-_Metal_window_and_door_manufacturing',
 '332329_-_Other_ornamental_and_architectural_metal_product_manufacturing',
 '332410_-_Power_boiler_and_heat_exchanger_manufacturing',
 '332420_-_Metal_tank__heavy_gauge__manufacturing',
 '332431_-_Metal_can_manufacturing',
 '332439_-_Other_metal_container_manufacturing',
 '332510_-_Hardware_manufacturing',
 '332611_-_Spring__heavy_gauge__manufacturing',
 '332619_-_Other_fabricated_wire_product_manufacturing',
 '332710_-_Machine_shops',
 '332720_-_Turned_product_and_screw__nut_and_bolt_manufacturing',
 '332810_-_Coating__engraving__cold_and_heat_treating_and_allied_activities',
 '332910_-_Metal_valve_manufacturing',
 '332991_-_Ball_and_roller_bearing_manufacturing',
 '332999_-_All_other_miscellaneous_fabricated_metal_product_manufacturing',
 '333110_-_Agricultural_implement_manufacturing',
 '333120_-_Construction_machinery_manufacturing',
 '333130_-_Mining_and_oil_and_gas_field_machinery_manufacturing',
 '333245_-_Sawmill_and_woodworking_machinery_manufacturing',
 '333246_-_Rubber_and_plastics_industry_machinery_manufacturing',
 '333247_-_Paper_industry_machinery_manufacturing',
 '333248_-_All_other_industrial_machinery_manufacturing',
 '333310_-_Commercial_and_service_industry_machinery_manufacturing',
 '333413_-_Industrial_and_commercial_fan_and_blower_and_air_purification_equipment_manufacturing',
 '333416_-_Heating_equipment_and_commercial_refrigeration_equipment_manufacturing',
 '333511_-_Industrial_mould_manufacturing',
 '333519_-_Other_metalworking_machinery_manufacturing',
 '333611_-_Turbine_and_turbine_generator_set_unit_manufacturing',
 '333619_-_Other_engine_and_power_transmission_equipment_manufacturing',
 '333910_-_Pump_and_compressor_manufacturing',
 '333920_-_Material_handling_equipment_manufacturing',
 '333990_-_All_other_general-purpose_machinery_manufacturing',
 '334110_-_Computer_and_peripheral_equipment_manufacturing',
 '334210_-_Telephone_apparatus_manufacturing',
 '334220_-_Radio_and_television_broadcasting_and_wireless_communications_equipment_manufacturing',
 '334290_-_Other_communications_equipment_manufacturing',
 '334310_-_Audio_and_video_equipment_manufacturing',
 '334410_-_Semiconductor_and_other_electronic_component_manufacturing',
 '334511_-_Navigational_and_guidance_instruments_manufacturing',
 '334512_-_Measuring__medical_and_controlling_devices_manufacturing',
 '334610_-_Manufacturing_and_reproducing_magnetic_and_optical_media',
 '335110_-_Electric_lamp_bulb_and_parts_manufacturing',
 '335120_-_Lighting_fixture_manufacturing',
 '335210_-_Small_electrical_appliance_manufacturing',
 '335223_-_Major_kitchen_appliance_manufacturing',
 '335229_-_Other_major_appliance_manufacturing',
 '335311_-_Power__distribution_and_specialty_transformers_manufacturing',
 '335312_-_Motor_and_generator_manufacturing',
 '335315_-_Switchgear_and_switchboard__and_relay_and_industrial_control_apparatus_manufacturing',
 '335910_-_Battery_manufacturing',
 '335920_-_Communication_and_energy_wire_and_cable_manufacturing',
 '335930_-_Wiring_device_manufacturing',
 '335990_-_All_other_electrical_equipment_and_component_manufacturing',
 '336110_-_Automobile_and_light-duty_motor_vehicle_manufacturing',
 '336120_-_Heavy-duty_truck_manufacturing',
 '336211_-_Motor_vehicle_body_manufacturing',
 '336212_-_Truck_trailer_manufacturing',
 '336215_-_Motor_home__travel_trailer_and_camper_manufacturing',
 '336310_-_Motor_vehicle_gasoline_engine_and_engine_parts_manufacturing',
 '336320_-_Motor_vehicle_electrical_and_electronic_equipment_manufacturing',
 '336330_-_Motor_vehicle_steering_and_suspension_components__except_spring__manufacturing',
 '336340_-_Motor_vehicle_brake_system_manufacturing',
 '336350_-_Motor_vehicle_transmission_and_power_train_parts_manufacturing',
 '336360_-_Motor_vehicle_seating_and_interior_trim_manufacturing',
 '336370_-_Motor_vehicle_metal_stamping',
 '336390_-_Other_motor_vehicle_parts_manufacturing',
 '336410_-_Aerospace_product_and_parts_manufacturing',
 '336510_-_Railroad_rolling_stock_manufacturing',
 '336611_-_Ship_building_and_repairing',
 '336612_-_Boat_building',
 '336990_-_Other_transportation_equipment_manufacturing',
 '337110_-_Wood_kitchen_cabinet_and_counter_top_manufacturing',
 '337121_-_Upholstered_household_furniture_manufacturing',
 '337123_-_Other_wood_household_furniture_manufacturing',
 '337126_-_Household_furniture__except_wood_and_upholstered__manufacturing',
 '337127_-_Institutional_furniture_manufacturing',
 '337213_-_Wood_office_furniture__including_custom_architectural_woodwork__manufacturing',
 '337214_-_Office_furniture__except_wood__manufacturing',
 '337215_-_Showcase__partition__shelving_and_locker_manufacturing',
 '337910_-_Mattress_manufacturing',
 '337920_-_Blind_and_shade_manufacturing',
 '339110_-_Medical_equipment_and_supplies_manufacturing',
 '339910_-_Jewellery_and_silverware_manufacturing',
 '339920_-_Sporting_and_athletic_goods_manufacturing',
 '339930_-_Doll__toy_and_game_manufacturing',
 '339940_-_Office_supplies__except_paper__manufacturing',
 '339950_-_Sign_manufacturing',
 '339990_-_All_other_miscellaneous_manufacturing']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data["naics_41-42"] = can_cbp_data[['411110_-_Live_animal_merchant_wholesalers',
 '411120_-_Oilseed_and_grain_merchant_wholesalers',
 '411130_-_Nursery_stock_and_plant_merchant_wholesalers',
 '411190_-_Other_farm_product_merchant_wholesalers',
 '412110_-_Petroleum_and_petroleum_products_merchant_wholesalers',
 '413110_-_General-line_food_merchant_wholesalers',
 '413120_-_Dairy_and_milk_products_merchant_wholesalers',
 '413130_-_Poultry_and_egg_merchant_wholesalers',
 '413140_-_Fish_and_seafood_product_merchant_wholesalers',
 '413150_-_Fresh_fruit_and_vegetable_merchant_wholesalers',
 '413160_-_Red_meat_and_meat_product_merchant_wholesalers',
 '413190_-_Other_specialty-line_food_merchant_wholesalers',
 '413210_-_Non-alcoholic_beverage_merchant_wholesalers',
 '413220_-_Alcoholic_beverage_merchant_wholesalers',
 '413310_-_Cigarette_and_tobacco_product_merchant_wholesalers',
 '414110_-_Clothing_and_clothing_accessories_merchant_wholesalers',
 '414120_-_Footwear_merchant_wholesalers',
 '414130_-_Piece_goods__notions_and_other_dry_goods_merchant_wholesalers',
 '414210_-_Home_entertainment_equipment_merchant_wholesalers',
 '414220_-_Household_appliance_merchant_wholesalers',
 '414310_-_China__glassware__crockery_and_pottery_merchant_wholesalers',
 '414320_-_Floor_covering_merchant_wholesalers',
 '414330_-_Linen__drapery_and_other_textile_furnishings_merchant_wholesalers',
 '414390_-_Other_home_furnishings_merchant_wholesalers',
 '414410_-_Jewellery_and_watch_merchant_wholesalers',
 '414420_-_Book__periodical_and_newspaper_merchant_wholesalers',
 '414430_-_Photographic_equipment_and_supplies_merchant_wholesalers',
 '414440_-_Sound_recording_merchant_wholesalers',
 '414450_-_Video_recording_merchant_wholesalers',
 '414460_-_Toy_and_hobby_goods_merchant_wholesalers',
 '414470_-_Amusement_and_sporting_goods_merchant_wholesalers',
 '414510_-_Pharmaceuticals_and_pharmacy_supplies_merchant_wholesalers',
 '414520_-_Toiletries__cosmetics_and_sundries_merchant_wholesalers',
 '415110_-_New_and_used_automobile_and_light-duty_truck_merchant_wholesalers',
 '415120_-_Truck__truck_tractor_and_bus_merchant_wholesalers',
 '415190_-_Recreational_and_other_motor_vehicles_merchant_wholesalers',
 '415210_-_Tire_merchant_wholesalers',
 '415290_-_Other_new_motor_vehicle_parts_and_accessories_merchant_wholesalers',
 '415310_-_Used_motor_vehicle_parts_and_accessories_merchant_wholesalers',
 '416110_-_Electrical_wiring_and_construction_supplies_merchant_wholesalers',
 '416120_-_Plumbing__heating_and_air-conditioning_equipment_and_supplies_merchant_wholesalers',
 '416210_-_Metal_service_centres',
 '416310_-_General-line_building_supplies_merchant_wholesalers',
 '416320_-_Lumber__plywood_and_millwork_merchant_wholesalers',
 '416330_-_Hardware_merchant_wholesalers',
 '416340_-_Paint__glass_and_wallpaper_merchant_wholesalers',
 '416390_-_Other_specialty-line_building_supplies_merchant_wholesalers',
 '417110_-_Farm__lawn_and_garden_machinery_and_equipment_merchant_wholesalers',
 '417210_-_Construction_and_forestry_machinery__equipment_and_supplies_merchant_wholesalers',
 '417220_-_Mining_and_oil_and_gas_well_machinery__equipment_and_supplies_merchant_wholesalers',
 '417230_-_Industrial_machinery__equipment_and_supplies_merchant_wholesalers',
 '417310_-_Computer__computer_peripheral_and_pre-packaged_software_merchant_wholesalers',
 '417320_-_Electronic_components__navigational_and_communications_equipment_and_supplies_merchant_wholesalers',
 '417910_-_Office_and_store_machinery_and_equipment_merchant_wholesalers',
 '417920_-_Service_establishment_machinery__equipment_and_supplies_merchant_wholesalers',
 '417930_-_Professional_machinery__equipment_and_supplies_merchant_wholesalers',
 '417990_-_All_other_machinery__equipment_and_supplies_merchant_wholesalers',
 '418110_-_Recyclable_metal_merchant_wholesalers',
 '418120_-_Recyclable_paper_and_paperboard_merchant_wholesalers',
 '418190_-_Other_recyclable_material_merchant_wholesalers',
 '418210_-_Stationery_and_office_supplies_merchant_wholesalers',
 '418220_-_Other_paper_and_disposable_plastic_product_merchant_wholesalers',
 '418310_-_Agricultural_feed_merchant_wholesalers',
 '418320_-_Seed_merchant_wholesalers',
 '418390_-_Agricultural_chemical_and_other_farm_supplies_merchant_wholesalers',
 '418410_-_Chemical__except_agricultural__and_allied_product_merchant_wholesalers',
 '418910_-_Log_and_wood_chip_merchant_wholesalers',
 '418920_-_Mineral__ore_and_precious_metal_merchant_wholesalers',
 '418930_-_Second-hand_goods__except_machinery_and_automotive__merchant_wholesalers',
 '418990_-_All_other_merchant_wholesalers',
 '419110_-_Business-to-business_electronic_markets',
 '419120_-_Wholesale_trade_agents_and_brokers']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data["naics_44-45"] = can_cbp_data[['441110_-_New_car_dealers',
 '441120_-_Used_car_dealers',
 '441210_-_Recreational_vehicle_dealers',
 '441220_-_Motorcycle__boat_and_other_motor_vehicle_dealers',
 '441310_-_Automotive_parts_and_accessories_stores',
 '441320_-_Tire_dealers',
 '442110_-_Furniture_stores',
 '442210_-_Floor_covering_stores',
 '442291_-_Window_treatment_stores',
 '442292_-_Print_and_picture_frame_stores',
 '442298_-_All_other_home_furnishings_stores',
 '443143_-_Appliance__television_and_other_electronics_stores',
 '443144_-_Computer_and_software_stores',
 '443145_-_Camera_and_photographic_supplies_stores',
 '443146_-_Audio_and_video_recordings_stores',
 '444110_-_Home_centres',
 '444120_-_Paint_and_wallpaper_stores',
 '444130_-_Hardware_stores',
 '444190_-_Other_building_material_dealers',
 '444210_-_Outdoor_power_equipment_stores',
 '444220_-_Nursery_stores_and_garden_centres',
 '445110_-_Supermarkets_and_other_grocery__except_convenience__stores',
 '445120_-_Convenience_stores',
 '445210_-_Meat_markets',
 '445220_-_Fish_and_seafood_markets',
 '445230_-_Fruit_and_vegetable_markets',
 '445291_-_Baked_goods_stores',
 '445292_-_Confectionery_and_nut_stores',
 '445299_-_All_other_specialty_food_stores',
 '445310_-_Beer__wine_and_liquor_stores',
 '446110_-_Pharmacies_and_drug_stores',
 '446120_-_Cosmetics__beauty_supplies_and_perfume_stores',
 '446130_-_Optical_goods_stores',
 '446191_-_Food__health__supplement_stores',
 '446199_-_All_other_health_and_personal_care_stores',
 '447110_-_Gasoline_stations_with_convenience_stores',
 '447190_-_Other_gasoline_stations',
 '448110_-_Men_s_clothing_stores',
 '448120_-_Women_s_clothing_stores',
 '448130_-_Children_s_and_infants__clothing_stores',
 '448140_-_Family_clothing_stores',
 '448150_-_Clothing_accessories_stores',
 '448191_-_Fur_stores',
 '448199_-_All_other_clothing_stores',
 '448210_-_Shoe_stores',
 '448310_-_Jewellery_stores',
 '448320_-_Luggage_and_leather_goods_stores',
 '451111_-_Golf_equipment_and_supplies__specialty_stores',
 '451112_-_Ski_equipment_and_supplies_specialty_stores',
 '451113_-_Cycling_equipment_and_supplies_specialty_stores',
 '451119_-_All_other_sporting_goods_stores',
 '451120_-_Hobby__toy_and_game_stores',
 '451130_-_Sewing__needlework_and_piece_goods_stores',
 '451140_-_Musical_instrument_and_supplies_stores',
 '451310_-_Book_stores_and_news_dealers',
 '452110_-_Department_stores',
 '452910_-_Warehouse_clubs',
 '452991_-_Home_and_auto_supplies_stores',
 '452999_-_All_other_miscellaneous_general_merchandise_stores',
 '453110_-_Florists',
 '453210_-_Office_supplies_and_stationery_stores',
 '453220_-_Gift__novelty_and_souvenir_stores',
 '453310_-_Used_merchandise_stores',
 '453910_-_Pet_and_pet_supplies_stores',
 '453920_-_Art_dealers',
 '453930_-_Mobile_home_dealers',
 '453992_-_Beer_and_wine-making_supplies_stores',
 '453999_-_All_other_miscellaneous_store_retailers__except_beer_and_wine-making_supplies_stores_',
 '454110_-_Electronic_shopping_and_mail-order_houses',
 '454210_-_Vending_machine_operators',
 '454311_-_Heating_oil_dealers',
 '454312_-_Liquefied_petroleum_gas__bottled_gas__dealers',
 '454319_-_Other_fuel_dealers',
 '454390_-_Other_direct_selling_establishments']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data["naics_48-49"] = can_cbp_data[['481110_-_Scheduled_air_transportation',
 '481214_-_Non-scheduled_chartered_air_transportation',
 '481215_-_Non-scheduled_specialty_flying_services',
 '482112_-_Short-haul_freight_rail_transportation',
 '482113_-_Mainline_freight_rail_transportation',
 '482114_-_Passenger_rail_transportation',
 '483115_-_Deep_sea__coastal_and_Great_Lakes_water_transportation__except_by_ferries_',
 '483116_-_Deep_sea__coastal_and_Great_Lakes_water_transportation_by_ferries',
 '483213_-_Inland_water_transportation__except_by_ferries_',
 '483214_-_Inland_water_transportation_by_ferries',
 '484110_-_General_freight_trucking__local',
 '484121_-_General_freight_trucking__long_distance__truck-load',
 '484122_-_General_freight_trucking__long_distance__less_than_truck-load',
 '484210_-_Used_household_and_office_goods_moving',
 '484221_-_Bulk_liquids_trucking__local',
 '484222_-_Dry_bulk_materials_trucking__local',
 '484223_-_Forest_products_trucking__local',
 '484229_-_Other_specialized_freight__except_used_goods__trucking__local',
 '484231_-_Bulk_liquids_trucking__long_distance',
 '484232_-_Dry_bulk_materials_trucking__long_distance',
 '484233_-_Forest_products_trucking__long_distance',
 '484239_-_Other_specialized_freight__except_used_goods__trucking__long_distance',
 '485110_-_Urban_transit_systems',
 '485210_-_Interurban_and_rural_bus_transportation',
 '485310_-_Taxi_service',
 '485320_-_Limousine_service',
 '485410_-_School_and_employee_bus_transportation',
 '485510_-_Charter_bus_industry',
 '485990_-_Other_transit_and_ground_passenger_transportation',
 '486110_-_Pipeline_transportation_of_crude_oil',
 '486210_-_Pipeline_transportation_of_natural_gas',
 '486910_-_Pipeline_transportation_of_refined_petroleum_products',
 '486990_-_All_other_pipeline_transportation',
 '487110_-_Scenic_and_sightseeing_transportation__land',
 '487210_-_Scenic_and_sightseeing_transportation__water',
 '487990_-_Scenic_and_sightseeing_transportation__other',
 '488111_-_Air_traffic_control',
 '488119_-_Other_airport_operations',
 '488190_-_Other_support_activities_for_air_transportation',
 '488210_-_Support_activities_for_rail_transportation',
 '488310_-_Port_and_harbour_operations',
 '488320_-_Marine_cargo_handling',
 '488331_-_Marine_salvage_services',
 '488332_-_Ship_piloting_services',
 '488339_-_Other_navigational_services_to_shipping',
 '488390_-_Other_support_activities_for_water_transportation',
 '488410_-_Motor_vehicle_towing',
 '488490_-_Other_support_activities_for_road_transportation',
 '488511_-_Marine_shipping_agencies',
 '488519_-_Other_freight_transportation_arrangement',
 '488990_-_Other_support_activities_for_transportation',
 '491110_-_Postal_service',
 '492110_-_Couriers',
 '492210_-_Local_messengers_and_local_delivery',
 '493110_-_General_warehousing_and_storage',
 '493120_-_Refrigerated_warehousing_and_storage',
 '493130_-_Farm_product_warehousing_and_storage',
 '493190_-_Other_warehousing_and_storage']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data["naics_51"] = can_cbp_data[['511110_-_Newspaper_publishers',
 '511120_-_Periodical_publishers',
 '511130_-_Book_publishers',
 '511140_-_Directory_and_mailing_list_publishers',
 '511190_-_Other_publishers',
 '511211_-_Software_publishers__except_video_game_publishers_',
 '511212_-_Video_game_publishers',
 '512110_-_Motion_picture_and_video_production',
 '512120_-_Motion_picture_and_video_distribution',
 '512130_-_Motion_picture_and_video_exhibition',
 '512190_-_Post-production_and_other_motion_picture_and_video_industries',
 '512210_-_Record_production',
 '512220_-_Integrated_record_production/distribution',
 '512230_-_Music_publishers',
 '512240_-_Sound_recording_studios',
 '512290_-_Other_sound_recording_industries',
 '515110_-_Radio_broadcasting',
 '515120_-_Television_broadcasting',
 '515210_-_Pay_and_specialty_television',
 '517111_-_Wired_telecommunications_carriers__except_cable_',
 '517112_-_Cable_and_other_program_distribution',
 '517210_-_Wireless_telecommunications_carriers__except_satellite_',
 '517410_-_Satellite_telecommunications',
 '517910_-_Other_telecommunications',
 '518210_-_Data_processing__hosting__and_related_services',
 '519110_-_News_syndicates',
 '519121_-_Libraries',
 '519122_-_Archives',
 '519130_-_Internet_publishing_and_broadcasting_and_web_search_portals',
 '519190_-_All_other_information_services']].astype(int).sum(axis=1)

can_cbp_data["naics_52"] = can_cbp_data[['521110_-_Monetary_authorities_-_central_bank',
 '522111_-_Personal_and_commercial_banking_industry',
 '522112_-_Corporate_and_institutional_banking_industry',
 '522130_-_Local_credit_unions',
 '522190_-_Other_depository_credit_intermediation',
 '522210_-_Credit_card_issuing',
 '522220_-_Sales_financing',
 '522291_-_Consumer_lending',
 '522299_-_All_other_non-depository_credit_intermediation',
 '522310_-_Mortgage_and_non-mortgage_loan_brokers',
 '522321_-_Central_credit_unions',
 '522329_-_Other_financial_transactions_processing_and_clearing_house_activities',
 '522390_-_Other_activities_related_to_credit_intermediation',
 '523110_-_Investment_banking_and_securities_dealing',
 '523120_-_Securities_brokerage',
 '523130_-_Commodity_contracts_dealing',
 '523140_-_Commodity_contracts_brokerage',
 '523210_-_Securities_and_commodity_exchanges',
 '523910_-_Miscellaneous_intermediation',
 '523920_-_Portfolio_management',
 '523930_-_Investment_advice',
 '523990_-_All_other_financial_investment_activities',
 '524111_-_Direct_individual_life__health_and_medical_insurance_carriers',
 '524112_-_Direct_group_life__health_and_medical_insurance_carriers',
 '524121_-_Direct_general_property_and_casualty_insurance_carriers',
 '524122_-_Direct__private__automobile_insurance_carriers',
 '524123_-_Direct__public__automobile_insurance_carriers',
 '524124_-_Direct_property_insurance_carriers',
 '524125_-_Direct_liability_insurance_carriers',
 '524129_-_Other_direct_insurance__except_life__health_and_medical__carriers',
 '524131_-_Life_reinsurance_carriers',
 '524132_-_Accident_and_sickness_reinsurance_carriers',
 '524133_-_Automobile_reinsurance_carriers',
 '524134_-_Property_reinsurance_carriers',
 '524135_-_Liability_reinsurance_carriers',
 '524139_-_General_and_other_reinsurance_carriers',
 '524210_-_Insurance_agencies_and_brokerages',
 '524291_-_Claims_adjusters',
 '524299_-_All_other_insurance_related_activities',
 '526111_-_Trusteed_pension_funds',
 '526112_-_Non-trusteed_pension_funds',
 '526911_-_Equity_funds_-_Canadian',
 '526912_-_Equity_funds_-_foreign',
 '526913_-_Mortgage_funds',
 '526914_-_Money_market_funds',
 '526915_-_Bond_and_income_/_dividend_funds_-_Canadian',
 '526916_-_Bond_and_income_/_dividend_funds_-_foreign',
 '526917_-_Balanced_funds_/_asset_allocation_funds',
 '526919_-_Other_open-ended_funds',
 '526930_-_Segregated__except_pension__funds',
 '526981_-_Securitization_vehicles',
 '526989_-_All_other_miscellaneous_funds_and_financial_vehicles']].astype(int).sum(axis=1)

can_cbp_data["naics_53"] = can_cbp_data[[
 '531111_-_Lessors_of_residential_buildings_and_dwellings__except_social_housing_projects_',
 '531112_-_Lessors_of_social_housing_projects',
 '531120_-_Lessors_of_non-residential_buildings__except_mini-warehouses_',
 '531130_-_Self-storage_mini-warehouses',
 '531190_-_Lessors_of_other_real_estate_property',
 '531211_-_Real_estate_agents',
 '531212_-_Offices_of_real_estate_brokers',
 '531310_-_Real_estate_property_managers',
 '531320_-_Offices_of_real_estate_appraisers',
 '531390_-_Other_activities_related_to_real_estate',
 '532111_-_Passenger_car_rental',
 '532112_-_Passenger_car_leasing',
 '532120_-_Truck__utility_trailer_and_RV__recreational_vehicle__rental_and_leasing',
 '532210_-_Consumer_electronics_and_appliance_rental',
 '532220_-_Formal_wear_and_costume_rental',
 '532230_-_Video_tape_and_disc_rental',
 '532290_-_Other_consumer_goods_rental',
 '532310_-_General_rental_centres',
 '532410_-_Construction__transportation__mining__and_forestry_machinery_and_equipment_rental_and_leasing',
 '532420_-_Office_machinery_and_equipment_rental_and_leasing',
 '532490_-_Other_commercial_and_industrial_machinery_and_equipment_rental_and_leasing',
 '533110_-_Lessors_of_non-financial_intangible_assets__except_copyrighted_works_']].astype(int).sum(axis=1)

can_cbp_data["naics_54"] = can_cbp_data[[
 '541110_-_Offices_of_lawyers',
 '541120_-_Offices_of_notaries',
 '541190_-_Other_legal_services',
 '541212_-_Offices_of__accountants',
 '541213_-_Tax_preparation_services',
 '541215_-_Bookkeeping__payroll_and_related_services',
 '541310_-_Architectural_services',
 '541320_-_Landscape_architectural_services',
 '541330_-_Engineering_services',
 '541340_-_Drafting_services',
 '541350_-_Building_inspection_services',
 '541360_-_Geophysical_surveying_and_mapping_services',
 '541370_-_Surveying_and_mapping__except_geophysical__services',
 '541380_-_Testing_laboratories',
 '541410_-_Interior_design_services',
 '541420_-_Industrial_design_services',
 '541430_-_Graphic_design_services',
 '541490_-_Other_specialized_design_services',
 '541514_-_Computer_systems_design_and_related_services__except_video_game_design_and_development_',
 '541515_-_Video_game_design_and_development_services',
 '541611_-_Administrative_management_and_general_management_consulting_services',
 '541612_-_Human_resources_consulting_services',
 '541619_-_Other_management_consulting_services',
 '541620_-_Environmental_consulting_services',
 '541690_-_Other_scientific_and_technical_consulting_services',
 '541710_-_Research_and_development_in_the__physical__engineering_and_life_sciences',
 '541720_-_Research_and_development_in_the__social_sciences_and_humanities',
 '541810_-_Advertising_agencies',
 '541820_-_Public_relations_services',
 '541830_-_Media_buying_agencies',
 '541840_-_Media_representatives',
 '541850_-_Display_advertising',
 '541860_-_Direct_mail_advertising',
 '541870_-_Advertising_material_distribution_services',
 '541891_-_Specialty_advertising_distributors',
 '541899_-_All_other_services_related_to_advertising',
 '541910_-_Marketing_research_and_public_opinion_polling',
 '541920_-_Photographic_services',
 '541930_-_Translation_and_interpretation_services',
 '541940_-_Veterinary_services',
 '541990_-_All_other_professional__scientific_and_technical_services']].astype(int).sum(axis=1)

can_cbp_data["naics_55"] = can_cbp_data[[
 '551113_-_Holding_companies',
 '551114_-_Head_offices']].astype(int).sum(axis=1)

can_cbp_data["naics_56"] = can_cbp_data[[
 '561110_-_Office_administrative_services',
 '561210_-_Facilities_support_services',
 '561310_-_Employment_placement_agencies_and_executive_search_services',
 '561320_-_Temporary_help_services',
 '561330_-_Professional_employer_organizations',
 '561410_-_Document_preparation_services',
 '561420_-_Telephone_call_centres',
 '561430_-_Business_service_centres',
 '561440_-_Collection_agencies',
 '561450_-_Credit_bureaus',
 '561490_-_Other_business_support_services',
 '561510_-_Travel_agencies',
 '561520_-_Tour_operators',
 '561590_-_Other_travel_arrangement_and_reservation_services',
 '561611_-_Investigation_services',
 '561612_-_Security_guard_and_patrol_services',
 '561613_-_Armoured_car_services',
 '561621_-_Security_systems_services__except_locksmiths_',
 '561622_-_Locksmiths',
 '561710_-_Exterminating_and_pest_control_services',
 '561721_-_Window_cleaning_services',
 '561722_-_Janitorial_services__except_window_cleaning_',
 '561730_-_Landscaping_services',
 '561740_-_Carpet_and_upholstery_cleaning_services',
 '561791_-_Duct_and_chimney_cleaning_services',
 '561799_-_All_other_services_to_buildings_and_dwellings',
 '561910_-_Packaging_and_labelling_services',
 '561920_-_Convention_and_trade_show_organizers',
 '561990_-_All_other_support_services',
 '562110_-_Waste_collection',
 '562210_-_Waste_treatment_and_disposal',
 '562910_-_Remediation_services',
 '562920_-_Material_recovery_facilities',
 '562990_-_All_other_waste_management_services']].astype(int).sum(axis=1)

can_cbp_data["naics_61"] = can_cbp_data[[
'611110_-_Elementary_and_secondary_schools',
 '611210_-_Community_colleges_and_C.E.G.E.P.s',
 '611310_-_Universities',
 '611410_-_Business_and_secretarial_schools',
 '611420_-_Computer_training',
 '611430_-_Professional_and_management_development_training',
 '611510_-_Technical_and_trade_schools',
 '611610_-_Fine_arts_schools',
 '611620_-_Athletic_instruction',
 '611630_-_Language_schools',
 '611690_-_All_other_schools_and_instruction',
 '611710_-_Educational_support_services']].astype(int).sum(axis=1)

can_cbp_data["naics_62"] = can_cbp_data[[
 '621110_-_Offices_of_physicians',
 '621210_-_Offices_of_dentists',
 '621310_-_Offices_of_chiropractors',
 '621320_-_Offices_of_optometrists',
 '621330_-_Offices_of_mental_health_practitioners__except_physicians_',
 '621340_-_Offices_of_physical__occupational__and_speech_therapists_and_audiologists',
 '621390_-_Offices_of_all_other_health_practitioners',
 '621410_-_Family_planning_centres',
 '621420_-_Out-patient_mental_health_and_substance_abuse_centres',
 '621494_-_Community_health_centres',
 '621499_-_All_other_out-patient_care_centres',
 '621510_-_Medical_and_diagnostic_laboratories',
 '621610_-_Home_health_care_services',
 '621911_-_Ambulance__except_air_ambulance__services',
 '621912_-_Air_ambulance_services',
 '621990_-_All_other_ambulatory_health_care_services',
 '622111_-_General__except_paediatric__hospitals',
 '622112_-_Paediatric_hospitals',
 '622210_-_Psychiatric_and_substance_abuse_hospitals',
 '622310_-_Specialty__except_psychiatric_and_substance_abuse__hospitals',
 '623110_-_Nursing_care_facilities',
 '623210_-_Residential_developmental_handicap_facilities',
 '623221_-_Residential_substance_abuse_facilities',
 '623222_-_Homes_for_the_psychiatrically_disabled',
 '623310_-_Community_care_facilities_for_the_elderly',
 '623991_-_Transition_homes_for_women',
 '623992_-_Homes_for_emotionally_disturbed_children',
 '623993_-_Homes_for_the_physically_handicapped_or_disabled',
 '623999_-_All_other_residential_care_facilities',
 '624110_-_Child_and_youth_services',
 '624120_-_Services_for_the_elderly_and_persons_with_disabilities',
 '624190_-_Other_individual_and_family_services',
 '624210_-_Community_food_services',
 '624220_-_Community_housing_services',
 '624230_-_Emergency_and_other_relief_services',
 '624310_-_Vocational_rehabilitation_services',
 '624410_-_Child_day-care_services']].astype(int).sum(axis=1)

can_cbp_data["naics_71"] = can_cbp_data[[
 '711111_-_Theatre__except_musical__companies',
 '711112_-_Musical_theatre_and_opera_companies',
 '711120_-_Dance_companies',
 '711130_-_Musical_groups_and_artists',
 '711190_-_Other_performing_arts_companies',
 '711211_-_Sports_teams_and_clubs',
 '711213_-_Horse_race_tracks',
 '711218_-_Other_spectator_sports',
 '711311_-_Live_theatres_and_other_performing_arts_presenters_with_facilities',
 '711319_-_Sports_stadiums_and_other_presenters_with_facilities',
 '711321_-_Performing_arts_promoters__presenters__without_facilities',
 '711322_-_Festivals_without_facilities',
 '711329_-_Sports_presenters_and_other_presenters_without_facilities',
 '711410_-_Agents_and_managers_for_artists__athletes__entertainers_and_other_public_figures',
 '711511_-_Independent_visual_artists_and_artisans',
 '711512_-_Independent_actors__comedians_and_performers',
 '711513_-_Independent_writers_and_authors',
 '712111_-_Non-commercial_art_museums_and_galleries',
 '712115_-_History_and_science_museums',
 '712119_-_Other_museums',
 '712120_-_Historic_and_heritage_sites',
 '712130_-_Zoos_and_botanical_gardens',
 '712190_-_Nature_parks_and_other_similar_institutions',
 '713110_-_Amusement_and_theme_parks',
 '713120_-_Amusement_arcades',
 '713210_-_Casinos__except_casino_hotels_',
 '713291_-_Lotteries',
 '713299_-_All_other_gambling_industries',
 '713910_-_Golf_courses_and_country_clubs',
 '713920_-_Skiing_facilities',
 '713930_-_Marinas',
 '713940_-_Fitness_and_recreational_sports_centres',
 '713950_-_Bowling_centres',
 '713990_-_All_other_amusement_and_recreation_industries']].astype(int).sum(axis=1)

can_cbp_data["naics_72"] = can_cbp_data[[
 '721111_-_Hotels',
 '721112_-_Motor_hotels',
 '721113_-_Resorts',
 '721114_-_Motels',
 '721120_-_Casino_hotels',
 '721191_-_Bed_and_breakfast',
 '721192_-_Housekeeping_cottages_and_cabins',
 '721198_-_All_other_traveller_accommodation',
 '721211_-_Recreational_vehicle__RV__parks_and_campgrounds',
 '721212_-_Hunting_and_fishing_camps',
 '721213_-_Recreational__except_hunting_and_fishing__and_vacation_camps',
 '721310_-_Rooming_and_boarding_houses',
 '722310_-_Food_service_contractors',
 '722320_-_Caterers',
 '722330_-_Mobile_food_services',
 '722410_-_Drinking_places__alcoholic_beverages_',
 '722511_-_Full-service_restaurants',
 '722512_-_Limited-service_eating_places']].astype(int).sum(axis=1)

can_cbp_data["naics_81"] = can_cbp_data[[
 '811111_-_General_automotive_repair',
 '811112_-_Automotive_exhaust_system_repair',
 '811119_-_Other_automotive_mechanical_and_electrical_repair_and_maintenance',
 '811121_-_Automotive_body__paint_and_interior_repair_and_maintenance',
 '811122_-_Automotive_glass_replacement_shops',
 '811192_-_Car_washes',
 '811199_-_All_other_automotive_repair_and_maintenance',
 '811210_-_Electronic_and_precision_equipment_repair_and_maintenance',
 '811310_-_Commercial_and_industrial_machinery_and_equipment__except_automotive_and_electronic__repair_and_maintenance',
 '811411_-_Home_and_garden_equipment_repair_and_maintenance',
 '811412_-_Appliance_repair_and_maintenance',
 '811420_-_Reupholstery_and_furniture_repair',
 '811430_-_Footwear_and_leather_goods_repair',
 '811490_-_Other_personal_and_household_goods_repair_and_maintenance',
 '812114_-_Barber_shops',
 '812115_-_Beauty_salons',
 '812116_-_Unisex_hair_salons',
 '812190_-_Other_personal_care_services',
 '812210_-_Funeral_homes',
 '812220_-_Cemeteries_and_crematoria',
 '812310_-_Coin-operated_laundries_and_dry_cleaners',
 '812320_-_Dry_cleaning_and_laundry_services__except_coin-operated_',
 '812330_-_Linen_and_uniform_supply',
 '812910_-_Pet_care__except_veterinary__services',
 '812921_-_Photo_finishing_laboratories__except_one-hour_',
 '812922_-_One-hour_photo_finishing',
 '812930_-_Parking_lots_and_garages',
 '812990_-_All_other_personal_services',
 '813110_-_Religious_organizations',
 '813210_-_Grant-making_and_giving_services',
 '813310_-_Social_advocacy_organizations',
 '813410_-_Civic_and_social_organizations',
 '813910_-_Business_associations',
 '813920_-_Professional_organizations',
 '813930_-_Labour_organizations',
 '813940_-_Political_organizations',
 '813990_-_Other_membership_organizations',
 '814110_-_Private_households']].astype(int).sum(axis=1)

can_cbp_data["naics_91"] = can_cbp_data[[
 '911110_-_Defence_services',
 '911210_-_Federal_courts_of_law',
 '911220_-_Federal_correctional_services',
 '911230_-_Federal_police_services',
 '911240_-_Federal_regulatory_services',
 '911290_-_Other_federal_protective_services',
 '911310_-_Federal_labour_and_employment_services',
 '911320_-_Immigration_services',
 '911390_-_Other_federal_labour__employment_and_immigration_services',
 '911410_-_Foreign_affairs',
 '911420_-_International_assistance',
 '911910_-_Other_federal_government_public_administration',
 '912110_-_Provincial_courts_of_law',
 '912120_-_Provincial_correctional_services',
 '912130_-_Provincial_police_services',
 '912140_-_Provincial_fire-fighting_services',
 '912150_-_Provincial_regulatory_services',
 '912190_-_Other_provincial_protective_services',
 '912210_-_Provincial_labour_and_employment_services',
 '912910_-_Other_provincial_and_territorial_public_administration',
 '913110_-_Municipal_courts_of_law',
 '913120_-_Municipal_correctional_services',
 '913130_-_Municipal_police_services',
 '913140_-_Municipal_fire-fighting_services',
 '913150_-_Municipal_regulatory_services',
 '913190_-_Other_municipal_protective_services',
 '913910_-_Other_local__municipal_and_regional_public_administration',
 '914110_-_Aboriginal_public_administration',
 '919110_-_International_and_other_extra-territorial_public_administration']].astype(int).sum(axis=1)

# COMMAND ----------

can_cbp_data

# COMMAND ----------

can_census_data

# COMMAND ----------

can_census_data

# COMMAND ----------

can_census_data = can_census_data.fillna(0)
can_census_data = can_census_data.replace({'NA': 0})
can_census_data = can_census_data.replace({'': 0})
can_census_data.iloc[:,4:] = can_census_data.iloc[:,4:].apply(pd.to_numeric)
can_census_data["GeoUID"] = can_census_data["GeoUID"].astype(int)

# COMMAND ----------

can_census_data.columns

# COMMAND ----------

def div_0(n,d):
    try:
        return n/d
    except:
        return 0

# COMMAND ----------

def get_cancensus_data(row):
    can_census_downtown = can_census_data[can_census_data["GeoUID"].isin([int(i) for i in row['downtown_da']])]
    can_census_city = can_census_data[can_census_data["GeoUID"].isin([int(i) for i in row['ccs_da']])]
    can_dwtn_sum = can_census_downtown.sum(axis=0)
    can_city_sum = can_census_city.sum(axis=0)
    return pd.Series([float(can_dwtn_sum["Population"]), #total_pop_downtown
                    float(can_city_sum["Population"]), #total_pop_city
                    
                    #pct_singlefam_downtown
                    div_0(float(can_dwtn_sum["v_CA16_409:_Single-detached_house"]) + float(can_dwtn_sum['v_CA16_412:_Semi-detached_house']) + float(can_dwtn_sum['v_CA16_413:_Row_house']) + float(can_dwtn_sum['v_CA16_416:_Other_single-attached_house']), float(can_dwtn_sum["v_CA16_408:_Occupied_private_dwellings_by_structural_type_of_dwelling_data"])) *100,
                          
                    #pct_singlefam_city
                    div_0(float(can_city_sum["v_CA16_409:_Single-detached_house"]) + float(can_city_sum['v_CA16_412:_Semi-detached_house']) + float(can_city_sum['v_CA16_413:_Row_house']) + float(can_city_sum['v_CA16_416:_Other_single-attached_house']), float(can_city_sum["v_CA16_408:_Occupied_private_dwellings_by_structural_type_of_dwelling_data"])) *100,
                          
                    #pct_multifam_downtown
                    div_0(float(can_dwtn_sum['v_CA16_410:_Apartment_in_a_building_that_has_five_or_more_storeys']) + float(can_dwtn_sum['v_CA16_414:_Apartment_or_flat_in_a_duplex']) + float(can_dwtn_sum['v_CA16_415:_Apartment_in_a_building_that_has_fewer_than_five_storeys']), float(can_dwtn_sum["v_CA16_408:_Occupied_private_dwellings_by_structural_type_of_dwelling_data"])) *100,
                    
                    #pct_multifam_city
                    div_0(float(can_city_sum['v_CA16_410:_Apartment_in_a_building_that_has_five_or_more_storeys']) + float(can_city_sum['v_CA16_414:_Apartment_or_flat_in_a_duplex']) + float(can_city_sum['v_CA16_415:_Apartment_in_a_building_that_has_fewer_than_five_storeys']), float(can_city_sum["v_CA16_408:_Occupied_private_dwellings_by_structural_type_of_dwelling_data"])) *100,    
                          
                    #pct_mobile_home_and_others_downtown
                    div_0(float(can_dwtn_sum['v_CA16_417:_Movable_dwelling']) + float(can_dwtn_sum['v_CA16_411:_Other_attached_dwelling']), float(can_dwtn_sum["v_CA16_408:_Occupied_private_dwellings_by_structural_type_of_dwelling_data"])) *100,
                          
                    #pct_mobile_home_and_others_city
                    div_0(float(can_city_sum['v_CA16_417:_Movable_dwelling']) + float(can_city_sum['v_CA16_411:_Other_attached_dwelling']), float(can_city_sum["v_CA16_408:_Occupied_private_dwellings_by_structural_type_of_dwelling_data"])) *100,
                          
                    #pct_renter_downtown
                    div_0(float(can_dwtn_sum["v_CA16_4838:_Renter"]), float(can_dwtn_sum["v_CA16_4836:_Total_-_Private_households_by_tenure_-_25%_sample_data"])) *100,
                          
                    #pct_renter_city
                    div_0(float(can_city_sum["v_CA16_4838:_Renter"]), float(can_city_sum["v_CA16_4836:_Total_-_Private_households_by_tenure_-_25%_sample_data"])) *100,                          
                    
                    #median_age_downtown (actually average)
                    np.mean((can_census_downtown["v_CA16_379:_Average_age"]).astype(float)),

                    #median_age_city (actually average)
                    np.mean((can_census_city["v_CA16_379:_Average_age"]).astype(float)),
                    
                    #bachelor_plus_downtown   
                    div_0(float(can_dwtn_sum["v_CA16_5078:_University_certificate__diploma_or_degree_at_bachelor_level_or_above"]), float(can_dwtn_sum["v_CA16_5051:_Total_-_Highest_certificate__diploma_or_degree_for_the_population_aged_15_years_and_over_in_private_households_-_25%_sample_data"])) *100,
                                     
                    #bachelor_plus_city   
                    div_0(float(can_city_sum["v_CA16_5078:_University_certificate__diploma_or_degree_at_bachelor_level_or_above"]), float(can_city_sum["v_CA16_5051:_Total_-_Highest_certificate__diploma_or_degree_for_the_population_aged_15_years_and_over_in_private_households_-_25%_sample_data"])) *100,
                    
                    #median_hhinc_downtown
                    np.mean((can_census_downtown["v_CA16_2397:_Median_total_income_of_households_in_2015__$_"].astype(float)))*(1.0143*1.016*1.0227*0.7338), 
                          
                    #median_hhinc_city
                    np.mean((can_census_city["v_CA16_2397:_Median_total_income_of_households_in_2015__$_"].astype(float)))*(1.0143*1.016*1.0227*0.7338),
                    
                    #median_rent_downtown
np.mean((can_census_downtown["v_CA16_4900:_Median_monthly_shelter_costs_for_rented_dwellings__$_"].astype(float)))*(1.0143*1.016*1.0227*0.7338),
                          
                    #median_rent_city
np.mean((can_census_city["v_CA16_4900:_Median_monthly_shelter_costs_for_rented_dwellings__$_"].astype(float)))*(1.0143*1.016*1.0227*0.7338),
                          
                    #pct_vacant_downtown
                    100 - (div_0(float(can_dwtn_sum["v_CA16_405:_Private_dwellings_occupied_by_usual_residents"]),float(can_dwtn_sum["v_CA16_404:_Total_private_dwellings"]))*100),
                          
                    #pct_vacant_city
                    100 - (div_0(float(can_city_sum["v_CA16_405:_Private_dwellings_occupied_by_usual_residents"]),float(can_city_sum["v_CA16_404:_Total_private_dwellings"]))*100),
                    
                    #pct_nhwhite_downtown      
                    div_0(float(can_dwtn_sum["v_CA16_4044:_European_origins"]), float(can_dwtn_sum["v_CA16_3999:_Total_-_Ethnic_origin_for_the_population_in_private_households_-_25%_sample_data"])) *100,

                    #pct_nhwhite_city    
                    div_0(float(can_city_sum["v_CA16_4044:_European_origins"]), float(can_city_sum["v_CA16_3999:_Total_-_Ethnic_origin_for_the_population_in_private_households_-_25%_sample_data"])) *100,
                      
                    #pct_nhblack_downtown      
                    div_0(float(can_dwtn_sum["v_CA16_4404:_African_origins"]), float(can_dwtn_sum["v_CA16_3999:_Total_-_Ethnic_origin_for_the_population_in_private_households_-_25%_sample_data"])) *100,
                          
                    #pct_nhblack_city      
                    div_0(float(can_city_sum["v_CA16_4404:_African_origins"]), float(can_city_sum["v_CA16_3999:_Total_-_Ethnic_origin_for_the_population_in_private_households_-_25%_sample_data"])) *100,
                  
                    #pct_hisp_downtown
                    div_0(float(can_dwtn_sum["v_CA16_4368:_Hispanic"]),float(can_dwtn_sum["v_CA16_3999:_Total_-_Ethnic_origin_for_the_population_in_private_households_-_25%_sample_data"])) *100,
                          
                    #pct_hisp_city
                    div_0(float(can_city_sum["v_CA16_4368:_Hispanic"]),float(can_city_sum["v_CA16_3999:_Total_-_Ethnic_origin_for_the_population_in_private_households_-_25%_sample_data"])) *100,
                          
                    #pct_nhasian_downtown
                    div_0(float(can_dwtn_sum["v_CA16_4608:_Asian_origins"]), float(can_dwtn_sum["v_CA16_3999:_Total_-_Ethnic_origin_for_the_population_in_private_households_-_25%_sample_data"])) *100,
                          
                    #pct_nhasian_city
                    div_0(float(can_city_sum["v_CA16_4608:_Asian_origins"]), float(can_city_sum["v_CA16_3999:_Total_-_Ethnic_origin_for_the_population_in_private_households_-_25%_sample_data"])) *100,
                    
                    #pct_commute_auto_downtown
                    div_0(float(can_dwtn_sum["v_CA16_5795:_Car__truck__van_-_as_a_driver"]) + float(can_dwtn_sum["v_CA16_5798:_Car__truck__van_-_as_a_passenger"]),float(can_dwtn_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,
                          
                    #pct_commute_auto_city
                    div_0(float(can_city_sum["v_CA16_5795:_Car__truck__van_-_as_a_driver"]) + float(can_city_sum["v_CA16_5798:_Car__truck__van_-_as_a_passenger"]),float(can_city_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,                         
                  
                    #pct_commute_public_transit_downtown
                    div_0(float(can_dwtn_sum["v_CA16_5801:_Public_transit"]),float(can_dwtn_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,
                          
                    #pct_commute_public_transit_city
                    div_0(float(can_city_sum["v_CA16_5801:_Public_transit"]),float(can_city_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,
                          
                    #pct_commute_walk_downtown      
                    div_0(float(can_dwtn_sum["v_CA16_5804:_Walked"]),float(can_dwtn_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,
                          
                    #pct_commute_walk_city      
                    div_0(float(can_city_sum["v_CA16_5804:_Walked"]),float(can_city_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,
                          
                    #pct_commute_bicycle_downtown
                    div_0(float(can_dwtn_sum["v_CA16_5807:_Bicycle"]),float(can_dwtn_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,
                          
                    #pct_commute_bicycle_city
                    div_0(float(can_city_sum["v_CA16_5807:_Bicycle"]),float(can_city_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,
                    
                    #pct_commute_others_downtown      
                    div_0(float(can_dwtn_sum["v_CA16_5810:_Other_method"]),float(can_dwtn_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,
                          
                    #pct_commute_others_city     
                    div_0(float(can_city_sum["v_CA16_5810:_Other_method"]),float(can_city_sum["v_CA16_5792:_Total_-_Main_mode_of_commuting_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data"]))*100,

                    #housing_units_downtown
                    float(can_dwtn_sum["v_CA16_408:_Occupied_private_dwellings_by_structural_type_of_dwelling_data"]),
                      
                    #housing_unitscity
                    float(can_city_sum["v_CA16_408:_Occupied_private_dwellings_by_structural_type_of_dwelling_data"]),

                    #average_commute_time_downtown
                    div_0(float(can_dwtn_sum['v_CA16_5816:_Less_than_15_minutes'])*15
                          + float(can_dwtn_sum['v_CA16_5819:_15_to_29_minutes'])*22.5
                          + float(can_dwtn_sum['v_CA16_5822:_30_to_44_minutes'])*37.5
                          + float(can_dwtn_sum['v_CA16_5825:_45_to_59_minutes'])*52.5
                          + float(can_dwtn_sum['v_CA16_5828:_60_minutes_and_over'])*60,
                          float(can_dwtn_sum['v_CA16_5813:_Total_-_Commuting_duration_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data'])),
                      
                    #average_commute_time_city
                    div_0(float(can_city_sum['v_CA16_5816:_Less_than_15_minutes'])*15
                          + float(can_city_sum['v_CA16_5819:_15_to_29_minutes'])*22.5
                          + float(can_city_sum['v_CA16_5822:_30_to_44_minutes'])*37.5
                          + float(can_city_sum['v_CA16_5825:_45_to_59_minutes'])*52.5
                          + float(can_city_sum['v_CA16_5828:_60_minutes_and_over'])*60,
                          float(can_city_sum['v_CA16_5813:_Total_-_Commuting_duration_for_the_employed_labour_force_aged_15_years_and_over_in_private_households_with_a_usual_place_of_work_or_no_fixed_workplace_address_-_25%_sample_data'])),                      
                      
                    #downtown_area  
                    float(can_dwtn_sum["Area__sq_km_"])*0.386102,
    
                    #city_area  
                    float(can_city_sum["Area__sq_km_"])*0.386102])

# COMMAND ----------

can_city_index[['total_pop_downtown',
               'total_pop_city',
               'pct_singlefam_downtown',
               'pct_singlefam_city',
               'pct_multifam_downtown',
               'pct_multifam_city', 
               'pct_mobile_home_and_others_downtown',
               'pct_mobile_home_and_others_city', 
               'pct_renter_downtown',
               'pct_renter_city', 
               'median_age_downtown',
               'median_age_city', 
               'bachelor_plus_downtown',
               'bachelor_plus_city',
               'median_hhinc_downtown',
               'median_hhinc_city',
               'median_rent_downtown',
               'median_rent_city',
               'pct_vacant_downtown',
               'pct_vacant_city',
               'pct_nhwhite_downtown',
               'pct_nhwhite_city',
               'pct_nhblack_downtown',
               'pct_nhblack_city',
               'pct_hisp_downtown',
               'pct_hisp_city',
               'pct_nhasian_downtown',
               'pct_nhasian_city',
               'pct_commute_auto_downtown',
               'pct_commute_auto_city',
               'pct_commute_public_transit_downtown',
               'pct_commute_public_transit_city',
               'pct_commute_walk_downtown',
               'pct_commute_walk_city',
               'pct_commute_bicycle_downtown',
               'pct_commute_bicycle_city',
               'pct_commute_others_downtown',
               'pct_commute_others_city',
               'housing_units_downtown',
               'housing_units_city',
               'average_commute_time_downtown',
               'average_commute_time_city',
               'land_area_downtown',
               'land_area_city']] = can_city_index.apply(lambda x: get_cancensus_data(x), axis=1)


# COMMAND ----------

can_city_index

# COMMAND ----------

can_cbp_data

# COMMAND ----------

def get_can_cbp_data(da_list):
    cbp_cut = can_cbp_data[can_cbp_data["GEO"].isin(da_list)]
    cbp_dwtn_sum = cbp_cut.sum(axis=0)
    total_jobs = cbp_cut.iloc[:,-20:].to_numpy().sum()
    return pd.Series([total_jobs,
                      div_0(cbp_dwtn_sum["naics_11"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_21"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_22"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_23"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_31-33"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_41-42"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_44-45"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_48-49"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_51"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_52"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_53"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_54"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_55"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_56"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_61"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_62"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_71"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_72"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_81"]*100,total_jobs),
                      div_0(cbp_dwtn_sum["naics_91"]*100,total_jobs)])

# COMMAND ----------

can_city_index[["total_jobs",
              "pct_jobs_agriculture_forestry_fishing_hunting",
              "pct_jobs_mining_quarrying_oil_gas",
              "pct_jobs_utilities",
              "pct_jobs_construction",
              "pct_jobs_manufacturing",
              "pct_jobs_wholesale_trade",
              "pct_jobs_retail_trade",
              "pct_jobs_transport_warehouse",
              "pct_jobs_information",
              "pct_jobs_finance_insurance",
              "pct_jobs_real_estate",
              "pct_jobs_professional_science_techical",
              "pct_jobs_management_of_companies_enterprises",
              "pct_jobs_administrative_support_waste",
              "pct_jobs_educational_services",
              "pct_jobs_healthcare_social_assistance",
              "pct_jobs_arts_entertainment_recreation",
              "pct_jobs_accomodation_food_services",
              "pct_jobs_other",
              "pct_jobs_public_administration"]] = can_city_index["downtown_da"].apply(lambda x: get_can_cbp_data(x))

# COMMAND ----------

can_city_index

# COMMAND ----------

def calculate_entropy(row):
    entropy = 0
    for i in ["pct_jobs_agriculture_forestry_fishing_hunting",
              "pct_jobs_mining_quarrying_oil_gas",
              "pct_jobs_utilities",
              "pct_jobs_construction",
              "pct_jobs_manufacturing",
              "pct_jobs_wholesale_trade",
              "pct_jobs_retail_trade",
              "pct_jobs_transport_warehouse",
              "pct_jobs_information",
              "pct_jobs_finance_insurance",
              "pct_jobs_real_estate",
              "pct_jobs_professional_science_techical",
              "pct_jobs_management_of_companies_enterprises",
              "pct_jobs_administrative_support_waste",
              "pct_jobs_educational_services",
              "pct_jobs_healthcare_social_assistance",
              "pct_jobs_arts_entertainment_recreation",
              "pct_jobs_accomodation_food_services",
              "pct_jobs_other",
              "pct_jobs_public_administration"]:
        if row[i]>0:
            entropy = entropy + ((((row[i]/100)*row["total_jobs"])/row["total_jobs"])*np.log(((row[i]/100)*row["total_jobs"])/row["total_jobs"]))
        else:
            entropy = entropy
    return -entropy

# COMMAND ----------

def calculate_land_use_entropy(row):
    entropy = 0
    housing = row['housing_units']
    biz = row['total_biz']
    for i in ['pct_biz_accom_food',
              'pct_biz_admin_waste',
              'pct_biz_arts_entertainment',
              'pct_biz_construction',
              'pct_biz_educational_svcs',
              'pct_biz_finance_insurance',
              'pct_biz_healthcare_social_assistance',
              'pct_biz_information',
              'pct_biz_mgmt_companies_enterprises',
              'pct_biz_manufacturing',
              'pct_biz_other_except_pub_adm',
              'pct_biz_professional_sci_tech',
              'pct_biz_public_adm',
              'pct_biz_retail_trade',
              'pct_biz_transportation_warehousing',
              'pct_biz_utilities',
              'pct_biz_wholesale_trade',
              'pct_biz_agriculture',
              'pct_biz_real_estate_leasing']:
        try:
            entropy = entropy + (((row[i]/100*biz))/(biz+housing))*np.log(1/(((row[i]/100*biz))/(biz+housing)))
        except:
            entropy = entropy
    return entropy + ((housing/(housing+biz))*np.log(1/(housing/(housing+biz))))

# COMMAND ----------

can_city_index["land_area_downtown"] = can_city_index["land_area_downtown"]*1000000
can_city_index["land_area_city"] = can_city_index["land_area_city"]*1000000
can_city_index["population_density_downtown"] = can_city_index["total_pop_downtown"]/can_city_index["land_area_downtown"]
can_city_index["population_density_city"] = can_city_index["total_pop_city"]/can_city_index["land_area_city"]
can_city_index["employment_density_downtown"] = can_city_index["total_jobs"]/can_city_index["land_area_downtown"]
can_city_index["housing_density_downtown"] = can_city_index["housing_units_downtown"]/can_city_index["land_area_downtown"]
can_city_index["housing_density_city"] = can_city_index["housing_units_city"]/can_city_index["land_area_city"]
can_city_index["employment_entropy"] = can_city_index.apply(lambda x: calculate_entropy(x), axis=1)

# COMMAND ----------

can_city_index['pct_others_downtown'] = 100 - can_city_index['pct_hisp_downtown'] - can_city_index['pct_nhasian_downtown'] - can_city_index['pct_nhblack_downtown'] - can_city_index['pct_nhwhite_downtown']
can_city_index['pct_others_city'] = 100 - can_city_index['pct_hisp_city'] - can_city_index['pct_nhasian_city'] - can_city_index['pct_nhblack_city'] - can_city_index['pct_nhwhite_city']
can_city_index['pct_commute_others_downtown'] = 100 - can_city_index['pct_commute_auto_downtown'] - can_city_index['pct_commute_public_transit_downtown'] - can_city_index['pct_commute_bicycle_downtown'] - can_city_index['pct_commute_walk_downtown']
can_city_index['pct_commute_others_city'] = 100 - can_city_index['pct_commute_auto_city'] - can_city_index['pct_commute_public_transit_city'] - can_city_index['pct_commute_bicycle_city'] - can_city_index['pct_commute_walk_city']

# COMMAND ----------

can_city_index.iloc[1,1] = "Montreal"
can_city_index.iloc[9,1] = "Quebec"

# COMMAND ----------

can_city_index

# COMMAND ----------

create_and_save_as_table(can_city_index.drop(columns=["_c0","1","2","CMA_da","downtown_da","CMA_not_downtown_da","ccs_da"]), "can_city_index_with_features_0626")

# COMMAND ----------

can_city_index.columns

# COMMAND ----------

can_city_index

# COMMAND ----------

can_city_index = can_city_index.rename(columns={"0":"city"})
can_rfr_factors_0626 = can_city_index[['city',
               'total_pop_downtown',
               'total_pop_city',
               'pct_singlefam_downtown',
               'pct_singlefam_city',
               'pct_multifam_downtown',
               'pct_multifam_city',
               'pct_mobile_home_and_others_downtown',
               'pct_mobile_home_and_others_city',
               'pct_renter_downtown',
               'pct_renter_city',
               'median_age_downtown',
               'median_age_city',
               'bachelor_plus_downtown',
               'bachelor_plus_city',
               'median_hhinc_downtown',
               'median_hhinc_city',
               'median_rent_downtown',
               'median_rent_city',
               'pct_vacant_downtown',
               'pct_vacant_city',
               'pct_nhwhite_downtown',
               'pct_nhwhite_city',
               'pct_nhblack_downtown',
               'pct_nhblack_city',
               'pct_nhasian_downtown',
               'pct_nhasian_city',
               'pct_hisp_downtown',
               'pct_hisp_city',
               'pct_commute_auto_downtown',
               'pct_commute_auto_city',
               'pct_commute_public_transit_downtown',
               'pct_commute_public_transit_city',
               'pct_commute_bicycle_downtown',
               'pct_commute_bicycle_city',
               'pct_commute_walk_downtown',
               'pct_commute_walk_city',
               'pct_commute_others_downtown',
               'pct_commute_others_city',
               'housing_units_downtown',
               'housing_units_city',
               'average_commute_time_downtown',
               'average_commute_time_city',
               "pct_jobs_agriculture_forestry_fishing_hunting",
               "pct_jobs_mining_quarrying_oil_gas",
               "pct_jobs_utilities",
               "pct_jobs_construction",
               "pct_jobs_manufacturing",
               "pct_jobs_wholesale_trade",
               "pct_jobs_retail_trade",
               "pct_jobs_transport_warehouse",
               "pct_jobs_information",
               "pct_jobs_finance_insurance",
               "pct_jobs_real_estate",
               "pct_jobs_professional_science_techical",
               "pct_jobs_management_of_companies_enterprises",
               "pct_jobs_administrative_support_waste",
               "pct_jobs_educational_services",
               "pct_jobs_healthcare_social_assistance",
               "pct_jobs_arts_entertainment_recreation",
               "pct_jobs_accomodation_food_services",
               "pct_jobs_other",
               "pct_jobs_public_administration",
               "employment_entropy",
               "population_density_downtown",
               "population_density_city",
               "employment_density_downtown",
               "housing_density_downtown",
               "housing_density_city"]]
create_and_save_as_table(can_rfr_factors_0626, "can_rfr_features_0626")

# COMMAND ----------

can_rfr_factors_0502

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

can_city_index.iloc[1,1] = "Montreal"
can_city_index.iloc[9,1] = "Quebec"
can_city_index

# COMMAND ----------

can_city_index["city"] = can_city_index["0"]
can_city_index["country"] = [0]*len(can_city_index)

# COMMAND ----------

city_df_cut_rfr = can_city_index[['city',
 'pct_singlefam',
 'pct_renter',
 'pct_employment_natresource',
 'pct_employment_construction',
 'pct_employment_manufacturing',
 'pct_employment_wholesale',
 'pct_employment_retail',
 'pct_employment_transpowarehousingutil',
 'pct_employment_info',
 'pct_employment_financeinsre',
 'pct_employment_profscimgmtadminwaste',
 'pct_employment_eduhealthsocialassist',
 'pct_employment_artentrecaccommfood',
 'pct_employment_other',
 'pct_employment_pubadmin',
 'median_age',
 'bachelor_plus',
 'median_hhinc',
 'median_rent',
 'pct_vacant',
 'pct_nhwhite',
 'pct_nhblack',
 'pct_nhasian',
 'pct_hisp',
 'pct_biz_accom_food',
 'pct_biz_admin_waste',
 'pct_biz_arts_entertainment',
 'pct_biz_construction',
 'pct_biz_educational_svcs',
 'pct_biz_finance_insurance',
 'pct_biz_healthcare_social_assistance',
 'pct_biz_information',
 'pct_biz_mgmt_companies_enterprises',
 'pct_biz_manufacturing',
 'pct_biz_other_except_pub_adm',
 'pct_biz_professional_sci_tech',
 'pct_biz_public_adm',
 'pct_biz_retail_trade',
 'pct_biz_transportation_warehousing',
 'pct_biz_utilities',
 'pct_biz_wholesale_trade',
 'pct_biz_agriculture',
 'pct_biz_mining_gas',
 'pct_biz_real_estate_leasing',
 'population_density',
 'business_density',
 'housing_density',
 'land use entropy',
 'country']]
create_and_save_as_table(city_df_cut_rfr, "0320_can_rfr_factors")

# COMMAND ----------

city_df_cut_rfr

# COMMAND ----------



# COMMAND ----------

def get_lq_by_week(da_list, week):
    #dt_DAUIDs = [str.strip(dauid)[1:-1] for dauid in da_list.values[0].split(",")]
    #dt_DAUIDs = dt_DAUIDs[0][1:]
    #dt_DAUIDs = dt_DAUIDs[-1][:-1]
    downtown_devices = can_device_counts[can_device_counts["poi_cbg"].isin(da_list[1])]
    #city_devices = can_device_counts[can_device_counts["poi_cbg"].isin(da_list[0])]
    downtown_devices_week = downtown_devices[downtown_devices["week_uid"] == week]
    #city_devices_week = city_devices[city_devices["week_uid"]==week]
    downtown_devices_sum = np.sum(downtown_devices_week["normalized_visits_by_state_scaling"])
    #city_devices_sum = np.sum(city_devices_week["normalized_visits_by_state_scaling"])
    total_devices = devices_weeks_df[devices_weeks_df["week_uid"]==week]["normalized_visits_by_state_scaling"]
    
    print(downtown_devices_sum)
        
    comparison_week = "2019" + week[4:]
    downtown_devices_comparison_week = downtown_devices[downtown_devices["week_uid"] == comparison_week]
    #city_devices_comparison_week = city_devices[city_devices["week_uid"] == comparison_week]
    downtown_devices_comparison_sum = np.sum(downtown_devices_comparison_week["normalized_visits_by_state_scaling"])
    #city_devices_comparison_sum = np.sum(city_devices_comparison_week["normalized_visits_by_state_scaling"])
    total_comparison_devices = devices_weeks_df[devices_weeks_df["week_uid"]==comparison_week]["normalized_visits_by_state_scaling"]
    
    print(downtown_devices_comparison_sum)
        
    n = float(float(downtown_devices_sum)/float(total_devices))
    d = float(float(downtown_devices_comparison_sum)/float(total_comparison_devices))
       
    print(n,d)
    print(np.divide(n,d))

    return np.divide(n,d)

# COMMAND ----------

week_list = np.arange(2020.03,2020.53,0.01)
week_list = np.append(week_list, np.arange(2021.01,2021.53,0.01))
week_list = np.append(week_list, np.arange(2022.01,2022.09,0.01))
week_list = [str(round(i,2)) for i in week_list]

for i in week_list:
    can_city_index["LQ_"+i] = round(can_city_index[["CMA_da","downtown_da"]].apply(lambda x: get_lq_by_week(x,i), axis=1),2)

# COMMAND ----------

can_city_index.loc[:,"LQ_2020.03"][0]

# COMMAND ----------

can_city_index

# COMMAND ----------

can_city_display = can_city_index[["0", 'LQ_2022.01',
 'LQ_2022.02',
 'LQ_2022.03',
 'LQ_2022.04',
 'LQ_2022.05',
 'LQ_2022.06',
 'LQ_2022.07',
 'LQ_2022.08',
 'LQ_2020.13',
 'LQ_2020.14',
 'LQ_2020.15',
 'LQ_2020.16',
 'LQ_2020.17',
 'LQ_2020.18',
 'LQ_2020.19',
 'LQ_2020.2',
 'LQ_2020.21',
 'LQ_2020.22',
 'LQ_2020.23',
 'LQ_2020.24',
 'LQ_2020.25',
 'LQ_2020.26',
 'LQ_2020.27',
 'LQ_2020.28',
 'LQ_2020.29',
 'LQ_2020.3',
 'LQ_2020.31',
 'LQ_2020.32',
 'LQ_2020.33',
 'LQ_2020.34',
 'LQ_2020.35',
 'LQ_2020.36',
 'LQ_2020.37',
 'LQ_2020.38',
 'LQ_2020.39',
 'LQ_2020.4',
 'LQ_2020.41',
 'LQ_2020.42',
 'LQ_2020.43',
 'LQ_2020.44',
 'LQ_2020.45',
 'LQ_2020.46',
 'LQ_2020.47',
 'LQ_2020.48',
 'LQ_2020.49',
 'LQ_2020.5',
 'LQ_2020.51',
 'LQ_2020.52',
 'LQ_2021.13',
 'LQ_2021.14',
 'LQ_2021.15',
 'LQ_2021.16',
 'LQ_2021.17',
 'LQ_2021.18',
 'LQ_2021.19',
 'LQ_2021.2',
 'LQ_2021.21',
 'LQ_2021.22',
 'LQ_2021.23',
 'LQ_2021.24',
 'LQ_2021.25',
 'LQ_2021.26',
 'LQ_2021.27',
 'LQ_2021.28',
 'LQ_2021.29',
 'LQ_2021.3',
 'LQ_2021.31',
 'LQ_2021.32',
 'LQ_2021.33',
 'LQ_2021.34',
 'LQ_2021.35',
 'LQ_2021.36',
 'LQ_2021.37',
 'LQ_2021.38',
 'LQ_2021.39',
 'LQ_2021.4',
 'LQ_2021.41',
 'LQ_2021.42',
 'LQ_2021.43',
 'LQ_2021.44',
 'LQ_2021.45',
 'LQ_2021.46',
 'LQ_2021.47',
 'LQ_2021.48',
 'LQ_2021.49',
 'LQ_2021.5',
 'LQ_2021.51',
 'LQ_2021.52']]

# COMMAND ----------

can_city_display = can_city_display.transpose()
can_city_display

# COMMAND ----------

can_city_index

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

can_device_counts = get_table_as_pandas_df("all_canada_device_count_agg")
can_device_counts["poi_cbg"] = can_device_counts["poi_cbg"].str[3:]
can_device_counts["week"] = pd.to_datetime(can_device_counts["week_start"])
can_device_counts["week_num"] = can_device_counts["week"].dt.week
can_device_counts["year"] = can_device_counts["week"].dt.year
can_device_counts["week_uid"] = can_device_counts["year"].astype(int)+can_device_counts["week_num"].astype(int)/100
can_device_counts["week_uid"] =  round(can_device_counts["week_uid"],2).astype(str)
pd.set_option('display.max_rows', 20)
can_device_counts

# COMMAND ----------

total_devices_by_week = can_device_counts.groupby("week_start").sum()["normalized_visits_by_state_scaling"]

# COMMAND ----------

total_devices_by_week

# COMMAND ----------

import datetime as dt
devices_weeks_df = pd.DataFrame(total_devices_by_week)
devices_weeks_df["week"] = pd.to_datetime(devices_weeks_df.index.values)
devices_weeks_df["week_num"] = devices_weeks_df["week"].dt.week
devices_weeks_df["year"] = devices_weeks_df["week"].dt.year
devices_weeks_df["week_uid"] = devices_weeks_df["year"].astype(int)+devices_weeks_df["week_num"].astype(int)/100
devices_weeks_df["week_uid"] = round(devices_weeks_df["week_uid"],2).astype(str)
devices_weeks_df

# COMMAND ----------

devices_weeks_df[devices_weeks_df["week_num"] == 53]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

