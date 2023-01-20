library(RSQLite) #Use SQLite database to store and read data
library(dplyr) # Data manipulation
library(tidyverse) #Data science package
library(ggplot2) #Load ggplot2 library for graph generations
library(plotly) #Load plot library for interactive graph generations
library(shinydashboard) # Load shinydashboard library
library(shiny) # Load shiny library
library(DT) #Load Datatable library
library(chron) # Load shron library for date manipulation
library(shinycssloaders) #Load shiny css library
library(janitor) # Adding dataframe total

repository <- file.path(dirname(rstudioapi::getSourceEditorContext()$path))
setwd(repository)

tags$head(tags$script(src = "message-handler.js"))

mydb <- dbConnect(RSQLite::SQLite(), "vnac2022.sqlite")

#### Look-up tables Preparation ####
mainActivityEmploy <- read.csv(file = "mainActivityEmploy.csv") # read Main Activity done by Employer in Question Q11.2.3
dbWriteTable(mydb, "mainActivityEmploy", mainActivityEmploy, overwrite = TRUE) # writing Main Activity done by Employer in Question Q11.2.3 to Database

sex <- read.csv(file = "sex.csv") # read sex of household members
dbWriteTable(mydb, "sex", sex, overwrite = TRUE) # write sex df to database

benefitsEmploy <- read.csv(file = "benefitsEmploy.csv") # read in Benefits that hired employees receive
dbWriteTable(mydb, "benefitsEmploy", benefitsEmploy, overwrite=TRUE) # write benefitsEmploy df to mydb database

#### Geographical Location Dataset function ####

hhpop <- dbGetQuery(mydb, "SELECT DISTINCT(id) AS id, COUNT(id) AS population FROM hhcomposition GROUP BY id")

dbWriteTable(mydb, "hhpop", hhpop, overwrite = TRUE)

section1_household <- function(household){
  geography_section1 <- dbGetQuery(mydb, "SELECT main.id,
                                   main.interview__id,
                                   main.province,
                                   province.provname,
                                   main.island,
                                   island.islandname,
                                   main.area_council,
                                   ac.acname,
                                   main.ea_number,
                                   main.village,
                                   village.villagename,
                                   main.new_village,
                                   main.can_enumerate,
                                   hhpop.population,
                                   main.Q0111__1 AS workingOnCrops,
                                   main.Q0111__2 AS workingOnLivestock,
                                   main.Q0111__3 AS workingOnForestry,
                                   main.Q0111__4 AS Fishing,
                                   main.Q0111__5 AS Aquaculture,
                                   main.Q0111__6 AS NoAgricultureActivities,
                                   main.total_member_hh,
                                   main.interview_date,
                                   main.Q0107 AS mainIncomeSource,
                                   incomesource.cashincome AS mainIncomeSourceDescription,
                                   main.other_cash_income AS Other_Income_Specify,
                                   main.Q0301 AS number_operated_parcels,
                                   main.prim_phone_num,
                                   main.another_phn_avail,
                                   main.sec_phonenum,
                                   main.sticker,
                                   main.g3a_sticker_prefix,
                                   main.g3b_sticker_number,
                                   main.g4_dwelling_gps__Latitude,
                                   main.g4_dwelling_gps__Longitude,
                                   main.interview_end_time
                            FROM main
                            INNER JOIN province ON main.province = province.provid
                            INNER JOIN island ON main.island = island.islandid
                            INNER JOIN ac ON main.area_council = ac.acid
                            INNER JOIN village ON main.village = village.villageid
                            INNER JOIN incomesource ON main.Q0107 = incomesource.recid
                            INNER JOIN hhpop ON main.id = hhpop.id
                            
                            WHERE main.province > 1
                     ")
  
  geography_section1$workingOnCrops = as.factor(geography_section1$workingOnCrops)
  geography_section1$workingOnLivestock = as.factor(geography_section1$workingOnLivestock)
  geography_section1$workingOnForestry = as.factor(geography_section1$workingOnForestry)
  geography_section1$Fishing = as.factor(geography_section1$Fishing)
  geography_section1$Aquaculture = as.factor(geography_section1$Aquaculture)
  geography_section1$NoAgricultureActivities = as.factor(geography_section1$NoAgricultureActivities)
  

high_value_crops <- dbGetQuery(mydb, "SELECT id,
                                         SUM(CASE WHEN noPlantsKava < 0 THEN 0 ELSE noPlantsKava END) as 'kavaPlants_number',
                                         SUM(CASE WHEN noPlantsCoconut < 0 THEN 0 ELSE noPlantsCoconut END) as 'coconutPlants_number',
                                         SUM(CASE WHEN noPlantsCocoa < 0 THEN 0 ELSE noPlantsCocoa END) as 'cocoaPlants_number',
                                         SUM(CASE WHEN noPlantsCoffee <0 THEN 0 ELSE noPlantsCoffee END) as 'coffeePlants_number',
                                         SUM(CASE WHEN noPlantsVanilla <0 THEN 0 ELSE noPlantsVanilla END) as 'vanillaPlants_number',
                                         SUM(CASE WHEN noPlantsTahitian <0 THEN 0 ELSE noPlantsTahitian END) as 'tahitianLimePlants_number',
                                         SUM(CASE WHEN noPlantsPepper <0 THEN 0 ELSE noPlantsPepper END) as 'pepperPlants_number',
                                         SUM(CASE WHEN noPlantsNoni <0 THEN 0 ELSE noPlantsNoni END) as 'noniPlants_number'
                                  FROM landoperated
                                  GROUP BY id
                           ")

household_hvCrops <- geography_section1 %>% left_join(high_value_crops)
#testhh <- merge(geography_section1, high_value_crops, by = "id")


cattle_number <- dbGetQuery(mydb, "SELECT id,
                                          SUM(Q050304A) AS totalCattles
                                   FROM cattleComposition
                                   WHERE Q050304A > -1
                                   GROUP BY id
                                          
                            ")

household_cattle <- household_hvCrops %>% left_join(cattle_number)


sheep_number <- dbGetQuery(mydb, "SELECT id,
                                          SUM(Q050403A) AS totalSheeps
                                   FROM sheepComposition
                                   WHERE Q050403A > -1
                                   GROUP BY id
                                          
                            ")

household_sheep <- household_cattle %>% left_join(sheep_number)



goat_Number <- dbGetQuery(mydb, "SELECT id,
                                          SUM(Q050503A) AS totalGoats
                                   FROM goatComposition
                                   WHERE Q050503A > -1
                                   GROUP BY id
                                          
                            ")

household_goat <- household_sheep %>% left_join(goat_Number)


pig_Number <- dbGetQuery(mydb, "SELECT id,
                                          SUM(Q050603A) AS totalPigs
                                   FROM pigComposition
                                   WHERE Q050603A >-1
                                   GROUP BY id
                                          
                            ")

household_pig <- household_goat %>% left_join(pig_Number)


chicken_Number <- dbGetQuery(mydb, "SELECT id,
                                           SUM(Q050702A) AS totalChicken
                                    FROM poultryComposition
                                    WHERE Q050702A >-1
                                    GROUP BY id
                             ")


householdProduction <- household_pig %>% left_join(chicken_Number)


dbWriteTable(mydb, "householdProduction", householdProduction, overwrite = TRUE)







  
}

section1_data <- section1(geography_section1)

#### Household Composition Dataset function ####

section2 <- function(hhComposition){
  hhcomp <- main %>%
   select(1, 38, 89:147)
  hhRoster <- dbGetQuery(mydb, "SELECT * FROM hhComposition")
  hhComposition <- merge(hhcomp, hhRoster, by = "id")
  
}

section2_data <- section2(hhComposition)



#### Livestock Section function ####

# section5 Cattle

section5_cattle <- function(householdCattle){
  lvstock <- dbGetQuery(mydb, "SELECT main.id,
                                      main.province,
                                      province.provname,
                                      main.island,
                                      island.islandname,
                                      main.area_council,
                                      ac.acname,
                                      main.ea_number,
                                      main.village,
                                      village.villagename,
                                      main.new_village,
                                      main.can_enumerate,
                                   main.Q0111__2 AS workingOnLivestock,
                                   main.Q0501__1 AS DairyCattle,
                                   main.Q0501__2 AS BeefCattle,
                                   main.Q0501__3 AS Sheep,
                                   main.Q0501__4 AS Goat,
                                   main.Q0501__5 AS Pigs,
                                   main.Q0501__6 AS PoultryDucks,
                                   main.Q0501__7 AS Apiculture,
                                   main.Q0501__8 AS Horse,
                                   main.Q0501__9 AS OtherLIvestock,
                                   main.raisingDairy,
                                   main.no_dairy AS NumberOfDairyCattle,
                                   main.Q050301 AS HowCattlesAreKept,
                                   livestockkeptMethod.livestockkeptmethod,
                                   main.Q050303__1 AS Hereford,
                                   main.Q050303__2 AS SantaGertrudis,
                                   main.Q050303__3 AS Limousin,
                                   main.Q050303__4 AS Brahman_Zebu,
                                   main.Q050303__5 AS Charolais,
                                   main.Q050303__6 AS DroughtMaster,
                                   main.Q050303__7 AS Native,
                                   main.Q050303__8 AS OtherCattleBreed,
                                   main.Q050303a AS cattleIdentification,
                                   cattleIdent.cattleIdent,
                                   main.Q050305 AS mainPurposeforSlaughter,
                                   cattleSlaughterPurpose.slaughterPurpose,
                                   main.Q050307 AS mainPastureType,
                                   CASE WHEN main.Q050307 = 1 THEN 'Native Grass' 
                                        WHEN main.Q050307 = 2 THEN 'Improved Pasture (e.g Juncao)'
                                   ELSE 'NA' END AS pastureTypeDesc,
                                   main.Q050307A AS cattleSuplimentaryFeed,
                                   CASE WHEN main.Q050307A = 1 THEN 'Yes'
                                        WHEN main.Q050307A = 2 THEN 'NO'
                                   ELSE 'NA' END AS whethersuplimentaryisUsed
                              FROM main
                                   INNER JOIN province ON main.province = province.provid
                                   INNER JOIN island ON main.island = island.islandid
                                   INNER JOIN ac ON main.area_council = ac.acid
                                   INNER JOIN village ON main.village = village.villageid
                                   LEFT JOIN cattleIdent ON main.Q050303a = cattleIdent.recid
                                   LEFT JOIN cattleSlaughterPurpose ON main.Q050305 = cattleSlaughterPurpose.recid
                                   LEFT JOIN livestockkeptMethod ON main.Q050301 = livestockkeptMethod.recid
                                   
                                   WHERE main.can_enumerate = 1
                        
                        ")
  
  lvstock$workingOnLivestock = as.factor(lvstock$workingOnLivestock)
  
  
  
  cattlecompo <- dbGetQuery(mydb, "SELECT cattleComposition.id,
                                          cattleComposition.R050304__id AS cattleClassid,
                                          cattleClass.cattleClasss,
                                          cattleComposition.Q050304A AS totalNumber,
                                          cattleComposition.Q050304B AS cattlesoldtoAbattoir_Slaughterhouse,
                                          cattleComposition.Q050304C AS cattlesoldAlive,
                                          cattleComposition.Q050304D AS cattleLost,
                                          cattleComposition.Q050304E AS cattleGivenAway,
                                          cattleComposition.Q050304F AS cattleSlaughtered
                                   FROM cattleComposition
                                   LEFT JOIN cattleClass ON cattleComposition.R050304__id = cattleClass.recid

                            ")
  
  householdCattle <- lvstock %>% left_join(cattlecompo)
}


#### Section 5 Sheep ####

section_sheep <- function(householdSheep){
  sheepStock <- dbGetQuery(mydb, "SELECT main.id,
                                      main.province,
                                      province.provname,
                                      main.island,
                                      island.islandname,
                                      main.area_council,
                                      ac.acname,
                                      main.ea_number,
                                      main.village,
                                      village.villagename,
                                      main.new_village,
                                      main.can_enumerate,
                                      main.Q0501__3 AS Sheep,
                                      main.Q050401 AS sheepKeptMethod,
                                      main.Q050401A AS pastureSharedorNot,
                                      CASE WHEN main.Q050401A = 1 THEN 'Exclusively for own sheep only'
                                           WHEN main.Q050401A = 2 THEN 'Shared with other farmers in the village'
                                      ELSE 'NA' END pastureSharedorNotDesc,
                                      main.Q050403aa AS sheepIdentification,
                                      cattleIdent.cattleIdent AS sheepIdentificationDesc,
                                      main.Q050404 AS sheepslaughterpurpose,
                                      cattleSlaughterPurpose.slaughterPurpose sheepSlaughterPurposeDesc,
                                      main.Q050406 AS pastureType,
                                      CASE WHEN main.Q050406 = 1 THEN 'Native Pasture'
                                           WHEN main.Q050406 = 2 THEN 'Improved Pasture'
                                      ELSE 'NA' END AS pastureTypeDesc,
                                      main.Q050306A AS SuplimentaryFeedUsed
                                      
                                      
                                FROM main
                                INNER JOIN province ON main.province = province.provid
                                INNER JOIN island ON main.island = island.islandid
                                INNER JOIN ac ON main.area_council = ac.acid
                                INNER JOIN village ON main.village = village.villageid
                                LEFT JOIN cattleSlaughterPurpose ON main.Q050404 = cattleSlaughterPurpose.recid
                                LEFT JOIN cattleIdent ON main.Q050403aa = cattleIdent.recid
                                
                                
                                ")
  
  sheepStock$Sheep = as.factor(sheepStock$Sheep)
  
  
  sheepcomp <- dbGetQuery(mydb, "SELECT sheepComposition.id,
                                          sheepComposition.R050403__id AS sheepNumber,
                                          sheepClass.sheepclassdesc,
                                          sheepComposition.Q050403A AS totalNumber,
                                          sheepComposition.Q050403B AS sheepsoldtoAbattoir_Slaughterhouse,
                                          sheepComposition.Q050403C AS sheepsoldAlive,
                                          sheepComposition.Q050403D AS sheepLost,
                                          sheepComposition.Q050403E AS sheepGivenAway,
                                          sheepComposition.Q050403F AS sheepslaughtered
                                   FROM sheepComposition
                                   LEFT JOIN sheepClass ON sheepComposition.R050403__id = sheepClass.recid
                                   
                                   ")
  
  householdSheep <- sheepStock %>% left_join(sheepcomp)
  
  
}
# **************************************** End of Section 5 Data **********************************************************




#### Section 6 Forestry ####

# **************************************** Beginning of Section 6 Data ***************************************************

forestry_section6 <- dbGetQuery(mydb, "SELECT main.id,
                                                 main.interview__id,
                                                 main.province,
                                                 province.provname,
                                                 main.island,
                                                 island.islandname,
                                                 main.area_council,
                                                 ac.acname,
                                                 main.ea_number,
                                                 main.village,
                                                 village.villagename,
                                                 main.new_village,
                                                 main.can_enumerate,
                                                 CASE WHEN main.typeForestry__1 = 1 THEN 1 ELSE 0 END AS NaturalForest,
                                                 CASE WHEN main.typeForestry__2 = 1 THEN 1 ELSE 0 END AS PlantedForest,
                                                 main.Q0601 AS mainUseNaturalForest,
                                                 mainUseNaturalForest.forestusedescription

                                    FROM main
                                    INNER JOIN province ON main.province = province.provid
                                    INNER JOIN island ON main.island = island.islandid
                                    INNER JOIN ac ON main.area_council = ac.acid
                                    INNER JOIN village ON main.village = village.villageid
                                    LEFT JOIN mainUseNaturalForest ON main.Q0601 = mainUseNaturalForest.forestusedid 
                                
                                ")
                                   

















householdProduction <- dbGetQuery(mydb, "SELECT provname,
                                         SUM(CASE WHEN kavaPlants_number <0 THEN 0 ELSE kavaPlants_number END) AS totalkava,
                                         SUM(CASE WHEN coconutPlants_number <0 THEN 0 ELSE coconutPlants_number END) AS totalcoconut,
                                         SUM(CASE WHEN cocoaPlants_number <0 THEN 0 ELSE cocoaPlants_number END) AS totalcocoa,
                                         SUM(CASE WHEN coffeePlants_number <0 THEN 0 ELSE coffeePlants_number END ) AS totalcoffee,
                                         SUM(CASE WHEN vanillaPlants_number <0 THEN 0 ELSE vanillaPlants_number END) AS totalvanilla,
                                         SUM(CASE WHEN tahitianLimePlants_number <0 THEN  0 ELSE tahitianLimePlants_number END ) AS totaltahitianLime,
                                         SUM(CASE WHEN pepperPlants_number <0 THEN 0 ELSE pepperPlants_number END) AS totalpepper,
                                         SUM(CASE WHEN noniPlants_number <0 THEN 0 ELSE noniPlants_number END) AS totalnoni,
                                         SUM(CASE WHEN totalCattles <0 THEN 0 ELSE totalCattles END) AS totalCattles,
                                         SUM(CASE WHEN totalSheeps <0 THEN 0 ELSE totalSheeps END) AS totalsheeps,
                                         SUM(CASE WHEN totalPigs <0 THEN 0 ELSE totalPigs END) AS totalpigs,
                                         SUM(CASE WHEN totalGoats <0 THEN 0 ELSE totalGoats END) AS totalgoats,
                                         SUM(CASE WHEN totalChicken <0 THEN 0 ELSE totalChicken END) AS totalchicken
                                FROM householdProduction
                                GROUP BY provname
                           
                           ")

householdProduction <- householdProduction %>%
  adorn_totals("row")











land_operated_section3 <- dbGetQuery(mydb, "SELECT * FROM landoperated")

temporalCrop <- dbGetQuery(mydb, "SELECT * FROM TemporalCrop")
temporalCropQuantity <- dbGetQuery(mydb, "SELECT * FROM temporalCropQuantity")



collectionDate <- dbGetQuery(mydb, "SELECT main.id,
                                           main.province,
                                           main.area_council,
                                           main.fv_time, 
                                           interview_status.responsible,
                                           enumerator.interviewername,
                                           enumerator.supervisorname
                                    FROM main 
                                    INNER JOIN interview_status ON main.id = interview_status.id
                                    INNER JOIN  enumerator ON main.id = interview_status.id AND interview_status.responsible = enumerator.interviewerid
                             
                             ")

collectionDate$mydate <- substr(collectionDate$fv_time, 1, 10)
collectionDate$mydate = as.Date(collectionDate$mydate)


testing <- collectionDate[, c("interviewername", "mydate", "id")]

testing_DT <- testing %>%
  group_by(interviewername, mydate) %>%
  arrange(mydate) %>%
  summarise(interviews = n())

collectionDate_DT_PV <- testing_DT %>%
  pivot_wider(names_from = mydate, values_from = interviews, values_fill = 0)




collectionDate_DT <- collectionDate %>%
  group_by(province, area_council, mydate, responsible, interviewername, supervisorname) %>%
  summarise(total = n())




collectionDate_DT_PV <- collectionDate_DT %>%
  pivot_wider(names_from = interviewername, values_from = total, values_fill = 0)



enumerator_ea <- dbGetQuery(mydb, "SELECT interview_status.responsible,
                                          enumerator.interviewername,
                                          main.ea_number,
                                          interview_status.id,
                                          main.prim_phone_num
                                   FROM interview_status
                                   INNER JOIN enumerator ON interview_status.responsible = enumerator.interviewerid
                                   INNER JOIN main ON interview_status.id = main.id
                                   WHERE main.ea_number = 410007 OR  main.ea_number = 410003 OR main.ea_number = 410008 OR main.ea_number = 410004 OR main.ea_number = 410006 OR main.ea_number = 410009
                                   
                            
                            
                            ")


ea <-dbGetQuery(mydb, "SELECT frame.provid,
                              provname,
                              frame.acid,
                              acname,
                              eaid,
                              households,
                              population, avghhsize
                       FROM frame
                       INNER JOIN ac on frame.acid = ac.acid
                ")

ea_vnac_col <- dbGetQuery(mydb, "SELECT main.ea_number AS eaid,
                                        COUNT(main.id) AS interviews
                                  FROM main
                                  WHERE main.ea_number > 0
                                  GROUP BY main.ea_number
                          
                          ")

ea_visits <- ea %>% left_join(ea_vnac_col)

enum_interviews <- dbGetQuery(mydb, "SELECT responsible,
                                            enumerator.interviewername,
                                            
                                            COUNT(id) AS interviews
                                    FROM  interview_status
                                    INNER JOIN enumerator ON interview_status.responsible = enumerator.interviewerid 
                                    GROUP BY responsible
                              ")

enum_days <- dbGetQuery(mydb, "SELECT interview_status.responsible,
                                      main.fv_time
                               FROM interview_status
                               INNER JOIN main ON interview_status.id = main.id
                        ")

enum_days$mydate <- substr(enum_days$fv_time, 1, 10)

dbWriteTable(mydb, "enum_days", enum_days, overwrite = TRUE )

enum_totalDays <- dbGetQuery(mydb, "SELECT responsible, COUNT(DISTINCT(mydate)) AS totalDays FROM enum_days GROUP BY responsible")

interviewerCollections <- merge(enum_interviews, enum_totalDays, by = "responsible")

interviewerCollections$dailyAverage <- round(interviewerCollections$interviews / interviewerCollections$totalDays, digits = 2)




# **************************************** End of Section 6 Data **********************************************************
#### Section 11 Labour Force ####

# **************************************** Beginning of Section 11  Data ***************************************************

labour_section11 <- dbGetQuery(mydb, "SELECT main.id,
                                                 main.interview__id,
                                                 main.province,
                                                 province.provname,
                                                 main.island,
                                                 island.islandname,
                                                 main.area_council,
                                                 ac.acname,
                                                 main.ea_number,
                                                 main.village,
                                                 village.villagename,
                                                 main.new_village,
                                                 main.can_enumerate,
                                                 main.employ,
                                                 labourHiredComposition.sexEmploy,
                                                 sex.sexdesc,
                                                 labourHiredComposition.ageEmploy,
                                                 labourHiredComposition.mainActivityEmploy,
                                                 mainActivityEmploy.mainActivityEmploydesc,
                                                 labourHiredComposition.hoursEmploy,
                                                 labourHiredComposition.paidCashEmploy,
                                                 labourHiredComposition.amountPaidEmploy,
                                                 labourHiredComposition.benefitsEmploy__1 AS 'No other benefits',
                                                 labourHiredComposition.benefitsEmploy__2 AS 'Free or subsidized housing',
                                                 labourHiredComposition.benefitsEmploy__3 AS 'Free meals',
                                                 labourHiredComposition.benefitsEmploy__4 AS 'Other benefits',
                                                 main.Q01103 AS 'Hired Organizations/Groups',
                                                 main.Q0110300 AS 'Number of Organizations/Groups Hired',
                                                 organisationComposition.Q0110302 AS 'Number of Male',
                                                 organisationComposition.Q0110303 AS 'Number ofFemale'
                                                 
                                                 FROM main
                                                 
                                                 INNER JOIN province ON main.province = province.provid
                                                 INNER JOIN island ON main.island = island.islandid
                                                 INNER JOIN ac ON main.area_council = ac.acid
                                                 INNER JOIN village ON main.village = village.villageid
                                                 LEFT JOIN labourHiredComposition ON main.id = labourHiredComposition.id
                                                 LEFT JOIN mainActivityEmploy ON labourHiredComposition.mainActivityEmploy = mainActivityEmploy.recid
                                                 LEFT JOIN sex ON labourHiredComposition.sexEmploy = sex.sexid
                                                 LEFT JOIN organisationComposition ON main.id = organisationComposition.id
                                                
                                                 WHERE can_enumerate = 1
                                                 
                                                 
                                                 
                                                 ")



# **************************************** End of Section 11 Data **********************************************************


#### geographical Location ####

gegraphy <- main[, c("id",
                     "gps_accuracy", 
                     "province",
                     "island",
                     "area_council",
                     "ea_number",
                     "village",
                     "new_village",
                     "g4_dwelling_gps__Latitude",
                     "g4_dwelling_gps__Longitude"
                     
                     )]


dwelling_gps <- dbGetQuery(mydb, "SELECT  id,
                                          province,
                                          province.provname
                                          island,
                                          area_council,
                                          ac.acname,
                                          ea_number,
                                          village,
                                          new_village,
                                          g4_dwelling_gps__Latitude,
                                          g4_dwelling_gps__Longitude
                                  FROM main
                                  INNER JOIN province ON main.province = province.provid
                                  INNER JOIN ac ON main.area_council = ac.acid
                           
                           ")


#### Household Composition table ####

  hh_comp_main <- main[, c("id",
                           "province",
                           "island",
                           "area_council",
                           "ea_number",
                           "total_member_hh",
                           "Q0111__1",
                           "Q0111__2",
                           "Q0111__3",
                           "Q0111__4",
                           "Q0111__5",
                           "Q0111__6",
                           "Q0107",
                           "other_cash_income"
                           
                           )]
  
  hh_member_roster <- dbGetQuery(mydb, "SELECT * FROM hhcomposition")
  
  # Final House Composition table
  
  hh_composition <<- hh_comp_main %>% left_join(hh_member_roster)
  
  dbWriteTable(conn, "hh_composition", hh_composition, overwrite = TRUE)
  
  
#### Enumeration Areas threshold checking ####
  
  vnac_collections <- dbGetQuery(conn, "SELECT ea_number AS EA2022,
                                               CASE WHEN COUNT(DISTINCT(id)) > 0 THEN COUNT(DISTINCT(id)) ELSE 0 END AS hh_2022,
                                               CASE WHEN COUNT(id) > 0 THEN COUNT(id) ELSE 0 END AS pop_2022
                                        FROM hh_composition
                                        WHERE province >1 AND ea_number >0
                                        GROUP BY ea_number
                                               
                                          
                                 ")
  
  
  census2020Col <- dbGetQuery(conn, "SELECT ea_area.PID,
                                            ea_area.Pname,
                                            ea_area.AC2022,
                                            ea_area.ACNAME22,
                                            ea_area.EA2016,
                                            ea_area.EA2022,
                                            census2020.households AS household2020,
                                            census2020.population AS population2020
                                    FROM ea_area
                                    INNER JOIN  census2020 ON ea_area.EA2022 = census2020.eaid
                                    GROUP BY ea_area.PID, ea_area.Pname, ea_area.AC2022, ea_area.ACNAME22, ea_area.EA2016, ea_area.EA2022  

                              ")

  final_col_difference <- merge(vnac_collections, census2020Col, by = "EA2022")
  
  final_col_difference <- census2020Col %>% left_join(vnac_collections)
  
  final_col_difference$hhDiff <- final_col_difference$hh_2022 - final_col_difference$household2020
  final_col_difference$popDiff <- final_col_difference$pop_2022 - final_col_difference$population2020
  
 # write.csv(final_col_difference, "c:/DataViz/VNAC2022/Dataprocessing/final_col_difference.csv")
  
  
#### Land Operated table ####
  
  operatedLand_main <- main[, c("id",
                            "province",
                            "island",
                            "area_council",
                            "ea_number",
                            "Q0301"
                            
                            )]
  
  operatedLand_roster <- dbGetQuery(mydb, "SELECT * FROM landoperated")
  
  # Final land operated table
  
  landOperated <- operatedLand_main %>% left_join(operatedLand_roster)

  
#### Temporal Crops table ####
  
  temporalCrop_main <- main[, c("id",
                                "province",
                                "island",
                                "area_council",
                                "ea_number"
                               )]
  
  temporalCrop_roster <- dbGetQuery(mydb, "SELECT * FROM temporalCrop")
  
  #Final temporal crop table
  
  temporalCrops <- temporalCrop_main %>% left_join(temporalCrop_roster)
  
  temporal
  
  
  #### Muti-Year-Crops table ####
  
  multiYearCropMain <- main[, c("id",
                                   "province",
                                   "island",
                                   "area_council",
                                   "ea_number"
                                    )]
  
  test30 <- dbGetQuery(mydb, "SELECT province.provname, 
                                   enumerator.supervisorname, 
                                   responsible, 
                                   enumerator.interviewername,
                                   CAST((COUNT(main.id)/30) AS DECIMAL(5, 2)) AS interviews
                            FROM interview_status 
                            INNER JOIN main ON interview_status.id = main.id
                            INNER JOIN enumerator ON interview_status.responsible = enumerator.interviewerid
                            INNER JOIN  province ON interview_status.id = main.id AND main.province = province.provid
                            GROUP BY responsible ") 
  
  test_pass30 <- test30 %>% filter(interviews >= 3)
  test_fail30 <- test30 %>% filter(interviews < 3)
  
  #write.csv(test_fail30, "C:/DataViz/VNAC2022/Dataprocessing/export/fall_below_3_interviews_per_day_30_days.csv", row.names = FALSE)
  #write.csv(test_pass30, "C:/DataViz/VNAC2022/Dataprocessing/export/3_or_more_interviews_per_day_30_days.csv", row.names = FALSE)
  
  test24 <- dbGetQuery(mydb, "SELECT province.provname, 
                                   enumerator.supervisorname, 
                                   responsible, 
                                   enumerator.interviewername,
                                   main.fv_time,
                                   COUNT(main.id) AS totalInterviews,
                                   24 AS numberofDays,
                                   LEFT(fv_time, 10) AS mydate,
                                   COUNT(main.id)/24 AS interviews
                            FROM interview_status 
                            INNER JOIN main ON interview_status.id = main.id
                            INNER JOIN enumerator ON interview_status.responsible = enumerator.interviewerid
                            INNER JOIN  province ON interview_status.id = main.id AND main.province = province.provid
                            ") 
  
  test_pass24 <- test24 %>% filter(interviews >= 3)
  test_fail24 <- test24 %>% filter(interviews < 3)
  
  #write.csv(test_fail24, "C:/DataViz/VNAC2022/Dataprocessing/export/fall_below_3_interviews_per_day_24_days.csv", row.names = FALSE)
  #write.csv(test_pass24, "C:/DataViz/VNAC2022/Dataprocessing/export/3_or_more_interviews_per_day_24_days.csv", row.names = FALSE)
  
  
  
  
  
  
  
  
