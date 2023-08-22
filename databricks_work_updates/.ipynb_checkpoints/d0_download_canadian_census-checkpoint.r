library(cancensus)
# you will need an api from cancensus to be able to use this script 
# 
# options(cancensus.api_key = YOUR_API_KEY)

# run this file like like: Rscript data_download CityName RegionLevel Year
# example: Rscript data_download.R Vancouver CT 1996
# just use this to get the vector names
# census_labels <- list_census_vectors("CA11")
# write.csv(census_labels, "CA11_labels.csv")

args = commandArgs(trailingOnly=TRUE)


if (args[1] == "Vancouver") {
  CMA_UID = "59933"
  city_name = args[1]
} else if (args[1] == "Toronto") {
  CMA_UID = "35535"
  city_name = args[1]
} else {
  print("The city is not ready to be downloaded.")
}

if (args[2] == "CT") {
  level_var = "CT"
} else if (args[2] == "CMA") {
  level_var = "CMA"
} else {
  print("The region level is not ready to be downloaded.")
}

if (args[3] == "1996") {
  dataset_var = "CA1996"
  census_vecs <- c("v_CA1996_7", # Total population by sex and age groups; Male, total; 0-4
                      "v_CA1996_8", # Total population by sex and age groups; Male, total; 5-9
                      "v_CA1996_9", # Total population by sex and age groups; Male, total; 10-14
                      
                      "v_CA1996_31", # Total population by sex and age groups; Female, total; 0-4
                      "v_CA1996_32", # Total population by sex and age groups; Female, total; 5-9
                      "v_CA1996_33", # Total population by sex and age groups; Female, total; 10-14
                      
                      #"v_CA1996_107", # Total number of occupied private dwellings by structural type of dwelling
                      
                      "v_CA1996_783", # Total - Total population by visible minority population
                      "v_CA1996_784", # Total visible minority population
                      
                      "v_CA1996_1347", # total population 15 years and over by highest level of schooling
                      "v_CA1996_1356", # total population 15 years and over by highest level of schooling, university
                      
                      "v_CA1996_1385", # total by mobility status 1 year ago
                      "v_CA1996_1386", # non-movers
                      "v_CA1996_1387", # movers
                      "v_CA1996_1388", # movers
                      "v_CA1996_1389", # movers, migrants (total)
                      "v_CA1996_1390", # movers, migrants, internal migrants
                      "v_CA1996_1391", # movers, migrants, internal migrants, intraprovincial migrants
                      "v_CA1996_1392", # movers, migrants, internal migrants, interprovincial migrants
                      "v_CA1996_1393", # movers, migrants, external migrants
                      
                      "v_CA1996_1403", # All persons with employment income by work activity (20% sample data)
                      "v_CA1996_1454", # median individual income
                      
                      "v_CA1996_1614", # hh income of all private households
                      "v_CA1996_1615", # under 10k
                      "v_CA1996_1616", # 10k - 19,999
                      "v_CA1996_1617", # 20k - 29,999
                      "v_CA1996_1618", # 30k - 39,999
                      "v_CA1996_1619", # 40k - 49,999
                      "v_CA1996_1620", # 50k - 59,999
                      "v_CA1996_1621", # 60k - 69,999
                      "v_CA1996_1622", # 70k - 79,999
                      "v_CA1996_1623", # 80k - 89,999
                      "v_CA1996_1624", # 90k - 99,999
                      "v_CA1996_1625", # 100k and up
                      "v_CA1996_1626", # Average household income $
                      "v_CA1996_1627", # Median household income $
                      "v_CA1996_1628", # Standard error of average household income $
                      "v_CA1996_1701", # average gross rent
                      "v_CA1996_1702", # Gross rent spending  30% or more of household income on shelter costs
                      "v_CA1996_1704",  # average owner's major payments
                      "v_CA1996_1705",  # Owner's major payments spending 30% or more of household income on shelter costs
                      
                      "v_CA1996_1678", # number of occupied private dwellings
                      "v_CA1996_1681", # avg value of dwelling
                      "v_CA1996_1682", # number of occupied private dwellings by tenure - owned
                      "v_CA1996_1683", # number of occupied private dwellings by tenure - rented
                      # "v_CA1996_1684", # number of occupied private dwellings by tenure - band housing
                      
                      "v_CA1996_1688", # Period of construction, before 1946
                      "v_CA1996_1689" # Period of construction, Period of construction, 1946-1960
                      
  )
} else if (args[3] == "2006") {
  dataset_var = "CA06"
  census_vecs <- c("v_CA06_101", # Total number of occupied private dwellings by housing tenure
                      "v_CA06_102", # Owned
                      "v_CA06_103", # Rented
                      # "v_CA06_104", # Band housing
                      "v_CA06_109", # Total number of occupied private dwellings by period of construction
                      "v_CA06_110", # Total number of occupied private dwellings by period of construction, before 1946
                      "v_CA06_111", # Total number of occupied private dwellings by period of construction, 1946 - 1960
                      
                      "v_CA06_451", # Total - Mobility status 1 year ago
                      "v_CA06_453", # movers
                      "v_CA06_454", # movers, non-migrants
                      "v_CA06_455", # movers, migrants
                      "v_CA06_456", # movers, migrants, internal migrants
                      "v_CA06_457", # movers, migrants, internal migrants, intraprovincial migrants
                      "v_CA06_458", # movers, migrants, internal migrants, interprovincial migrants
                      "v_CA06_459", # movers migrants, external migrants
                      
                      "v_CA06_1234", # total pop age 15 - 24 by education
                      "v_CA06_1240", # University certificate, diploma or degree (age 15 - 24)
                      
                      "v_CA06_1248", # total pop age 25 - 64 by education
                      "v_CA06_1254", # University certificate, diploma or degree (age 25 - 64)
                      "v_CA06_1262", # total pop 65 years and older by education
                      "v_CA06_1268", # University certificate, diploma or degree (age 65 and up)
                      
                      "v_CA06_1302", # total visible minority by groups
                      "v_CA06_1303", # total visible minority population
                      
                      "v_CA06_1583", # median individual income
                      
                      "v_CA06_1988", # Household income in 2005 of private households
                      "v_CA06_1989", # under 10k
                      "v_CA06_1990", # 10k - 19,999
                      "v_CA06_1991", # 20k - 29,999
                      "v_CA06_1992", # 30k - 39,999
                      "v_CA06_1993", # 40k - 49,999
                      "v_CA06_1994", # 50k - 59,999
                      "v_CA06_1995", # 60k - 69,999
                      "v_CA06_1996", # 70k - 79,999
                      "v_CA06_1997", # 80k - 89,999
                      "v_CA06_1998", # 90k - 99,999
                      "v_CA06_1999", # above 100k
                      "v_CA06_2000", # median household income
                      "v_CA06_2001", # Average household income $
                      "v_CA06_2002", # Standard error of average household income $
                      "v_CA06_2048", # Total number of non-farm, non-reserve private dwellings occupied by usual residents
                      "v_CA06_2049", # renters
                      "v_CA06_2050", # avg gross rent
                      "v_CA06_2051", # Tenant-occupied households spending 30% or more of household income on gross rent
                      "v_CA06_2053", # owners
                      "v_CA06_2054", # Average value of dwelling $
                      "v_CA06_2055", # Average owner major payments $
                      "v_CA06_2056"  # Owner households spending 30% or more of household income on owner's major payments
  )
  
} else if (args[3] == "2011") {
  dataset_var = "CA11"
  census_vecs <- c("v_CA11N_2281", # Number of owner households in non-farm, non-reserve private dwellings
                   "v_CA11N_2282", # % of owner households with a mortgage
                   "v_CA11N_2288", # Number of tenant households in non-farm, non-reserve private dwellings
                   "v_CA11N_2289", # % of tenant households in subsidized housing
                   "v_CA11N_2284", # Median monthly shelter costs for owned dwellings ($)
                   "v_CA11N_2286", # Median value of dwellings ($)
                   "v_CA11N_2291", # Median monthly shelter costs for rented dwellings ($)
                   "v_CA11N_2285", # Average monthly shelter costs for owned dwellings ($)
                   "v_CA11N_2287", # Average value of dwellings ($)
                   "v_CA11N_2292"  # Average monthly shelter costs for rented dwellings ($)

                   )
  
  
} else if (args[3] == "2016") {
  dataset_var = "CA16"
  census_vecs <- c("v_CA16_2207", # median individual income
                      "v_CA16_2397", # median household income
                      "v_CA16_2405", # Total - Household total income groups in 2015 for private households - 100% data
                      "v_CA16_2406", # under 5k
                      "v_CA16_2407", # 5k to 9,999
                      "v_CA16_2408", # 10k to 14,999
                      "v_CA16_2409", # 15k to 19,999
                      "v_CA16_2410", # 20k to 24,999
                      "v_CA16_2411", # 25k to 29,999
                      "v_CA16_2412", # 30k to 34,999
                      "v_CA16_2413", # 35k to 39,999
                      "v_CA16_2414", # 40k to 44,999
                      "v_CA16_2415", # 45k to 49,999
                      "v_CA16_2416", # 50k to 59,999
                      "v_CA16_2417", # 60k to 69,999
                      "v_CA16_2418", # 70k to 79,999
                      "v_CA16_2419", # 80k to 89,999
                      "v_CA16_2420", # 90k to 99,999
                      "v_CA16_2421", # 100k and over
                      "v_CA16_2422", # 100k and over - 100k to 124,999
                      "v_CA16_2423", # 100k and over - 125 to 149,999
                      "v_CA16_2424", # 100k and over - 150k to 199,999
                      "v_CA16_2425", # 100k and over - 200k and over
                      "v_CA16_2510", # Total - Low-income status in 2015 for the population in private households to whom low-income concepts are applicable - 100% data
                      "v_CA16_3954", # Total - Visible minority for the population in private households - 25% sample data
                      "v_CA16_3957", # Total visible minority population
                      
                      "v_CA16_4836", # Total - Private households by tenure - 25% sample data
                      "v_CA16_4837", # Owner
                      "v_CA16_4838", # Renter
                      
                      "v_CA16_4862", # Total - Occupied private dwellings by period of construction - 25% sample data
                      "v_CA16_4863", # Total - Occupied private dwellings by period of construction - 1960 or before
                      
                      "v_CA16_4890", # Total - Owner households in non-farm, non-reserve private dwellings - 25% sample data
                      "v_CA16_4891", # % of owner households with a mortgage
                      "v_CA16_4892", # % of owner households spending 30% or more of its income on shelter costs
                      "v_CA16_4893", # median monthly shelter costs - owners
                      "v_CA16_4894", # Average monthly shelter costs for owned dwellings ($)
                      "v_CA16_4895", # median value of dwellings
                      "v_CA16_4896", # average value of dwellings
                      "v_CA16_4897", # Total - Tenant households in non-farm, non-reserve private dwellings - 25% sample data
                      "v_CA16_4898", # % of tenant households in subsidized housing
                      "v_CA16_4899", # % of tenant households spending 30% or more of its income on shelter costs
                      "v_CA16_4900", # median monthly shelter costs - renters
                      "v_CA16_4901", # Average monthly shelter costs for rented dwellings ($)
                      "v_CA16_4985", # Average total income of households in 2015 ($)
                      "v_CA16_5051", # uni educated households denom
                      
                      "v_CA16_5105", # uni educated households
                      "v_CA16_6692", # Total - Mobility status 1 year ago - 25% sample data
                      "v_CA16_6695", # mobility, non-movers
                      "v_CA16_6698", # movers
                      "v_CA16_6701", # movers, non-migrants
                      "v_CA16_6704", # movers, migrants
                      "v_CA16_6707", # movers, migrants, internal migrants
                      "v_CA16_6710", # movers, migrants, internal migrants, intraprovincial
                      "v_CA16_6713", # movers, migrants, internal migrants, interprovincial
                      "v_CA16_6716" # movers, migrants, external migrants
  )
} else {
  print("The year is not ready to be downloaded.")
}

census_data <- get_census(dataset = dataset_var, regions = list(CMA = CMA_UID),
                             vectors = census_vecs, level = level_var)

# from stackoverflow, just here to make naming the files easier
substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

write.csv(census_data, paste("~/git/displacement-typologies/data/outputs/downloads/", as.character(city_name), substrRight(as.character(dataset_var), 2), as.character(level_var), "data.csv", sep = "_"))


