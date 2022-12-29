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

repository <- file.path(dirname(rstudioapi::getSourceEditorContext()$path))
setwd(repository)

tags$head(tags$script(src = "message-handler.js"))

mydb <- dbConnect(RSQLite::SQLite(), "vnac2022.sqlite")


#### Geographical Location Dataset function ####
section1 <- function(geography_section1){
  main <<- dbGetQuery(mydb, "SELECT * FROM main")
  geography_section1 <- main %>%
    select(1:37)
  
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
  
  
  
  
  
  
  
  
