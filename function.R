library(RSQLite) #Use SQLite database to store and read data
library(susoapi)
library(zoo) #Date Converstions
library(dplyr)
library(tidyr)

repository <- file.path(dirname(rstudioapi::getSourceEditorContext()$path))
setwd(repository)
mydb <- dbConnect(RSQLite::SQLite(), "vnac2022.sqlite")

generate_data <- function(){
  
  #### API Configuration ####
  # Assign Credentials to the variables (System Variables)
  server_name = "https://vboscapi.com"
  server_user = "vnac2022"
  server_password = "Van@rg2022!"
  
  # Assign credentials to the SUSOAPI function "set_credentials
  set_credentials(
    server = server_name,
    user = server_user,
    password = server_password
  )
  
  #Retrieve all questions being assigned to the BEC_USER

    all_questionnaires <- get_questionnaires(workspace = "vnac")
  
  #create JOB Id for purpose of exporting all data from the server
  start_export(
    qnr_id = 'f86c7c6e55ea45258f09b6b19112ef62$1',
    export_type = "Tabular",
    interview_status = "All",
    include_meta = TRUE,
    workspace = "vnac"
  ) -> started_job_id

}
process_data <- function(){
  # Get export job details ID in preparation in export data

    get_export_job_details(job_id = started_job_id, workspace = "vnac")
  
}

download_data <- function(){
  
  # Export data using the export ID to a zip file
  get_export_file(
    job_id = started_job_id,
    path = "data/",
    workspace = "vnac",
  )
  
  
}


extract_tables <- function(){

  #Extract tables from zipped folder
  
  main <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "VNAC2022.tab"))
  main$province <- as.numeric(as.factor(main$province))
  colnames(main)[1] = "id"
  hhComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R01.tab"))
  colnames(hhComposition)[1] = "id"
  landoperated <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R0301.tab"))
  colnames(landoperated)[1]="id"
  # cropRoster <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "CropRoster.tab"))
  # colnames(cropRoster)[1]="id"
  temporalCrop <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "TemporalCrop.tab"))
  colnames(temporalCrop)[1]="id"
  temporalCropQuantity <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R4171.tab"))
  colnames(temporalCropQuantity)[1]="id"
  nokavaHarvest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "noKavaHarvest.tab"))
  colnames(nokavaHarvest)[1]="id"
  noCoconutHarvest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "noCocoHarvest.tab"))
  colnames(noCoconutHarvest)[1]="id"
  noCocoaHarvest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "noCocoaHarvest.tab"))
  colnames(noCocoaHarvest)[1]="id"
  noCoffeeHarvest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "noCoffeeHarvest.tab"))
  colnames(noCoffeeHarvest)[1]="id"
  noVanillaHarvest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "noCoffeeHarvest.tab"))
  colnames(noVanillaHarvest)[1]="id"
  noPepperHarvest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "noPepperHarvest.tab"))
  colnames(noPepperHarvest)[1]="id"
  noTahitianHarvest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "noTahitianHarvest.tab"))
  colnames(noTahitianHarvest)[1]="id"
  noNoniHarvest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "noNoniHarvest.tab"))
  colnames(noNoniHarvest)[1]="id"
  other_mult_crop_roster <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "other_mult_crop_roster.tab"))
  colnames(other_mult_crop_roster)[1]="id"
  cattleComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R050304.tab"))
  colnames(cattleComposition)[1]="id"
  sheepComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R050403.tab"))
  colnames(sheepComposition)[1]="id"
  goatComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R050503.tab"))
  colnames(goatComposition)[1]="id"
  pigComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R050603.tab"))
  colnames(pigComposition)[1]="id"
  poultryComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R050702.tab"))
  colnames(poultryComposition)[1]="id"
  agricultureDetails <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R050802.tab"))
  colnames(agricultureDetails)[1]="id"
  noPlantedForest <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R0603.tab"))
  colnames(noPlantedForest)[1]="id"
  fishCatch <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R0704.tab"))
  colnames(fishCatch)[1]="id"
  aquaCalturecatch <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R0803.tab"))
  colnames(aquaCalturecatch)[1]="id"
  smallMachinesComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R1003.tab"))
  colnames(smallMachinesComposition)[1]="id"
  heavymachinesComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R1004.tab"))
  colnames(heavymachinesComposition)[1]="id"
  assistanceComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "Assistance.tab"))
  colnames(assistanceComposition)[1]="id"
  labourHiredComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "rosterEmploy.tab"))
  colnames(labourHiredComposition)[1]="id"
  organisationComposition <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "R01103.tab"))
  colnames(organisationComposition)[1]="id"
  interviewer_actions <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "interview__actions.tab"))
  colnames(interviewer_actions)[1]="id"
  interviewer_actions <- interviewer_actions %>%
    filter(action == 6, role == 3)
  
  interview_status <- read.delim2(unzip("data/VNAC2022_1_Tabular_All.zip", "interview__diagnostics.tab"))
  colnames(interview_status)[1]="id"
  #frame <- read.csv("data/censusframe.csv")
  frame <- read.csv("census2020_frame.csv")
  interview_level <- read.csv("interview_status.csv")
  province <- read.delim2("province.txt")
  ac <- read.csv("ac.csv")
  ea <- read.delim2("ea.txt")
  enumerator <- read.csv("employees.csv")
  ea_squarekilometers <- read.csv("ea_squarekilometers.csv")
  ea_area <- read.csv("ea_area_squarekilometers.csv")
  
  #interviewer$date <- as.Date(interviewer$date)
  
  
  # Write tables to the SQLite database
  
  dbWriteTable(mydb, "main", main, overwrite = TRUE)
  dbWriteTable(mydb, "hhComposition", hhComposition, overwrite = TRUE)
  dbWriteTable(mydb, "landoperated", landoperated, overwrite = TRUE)
  #dbWriteTable(mydb, "cropRoster", cropRoster, overwrite = TRUE)
  dbWriteTable(mydb, "temporalCrop", temporalCrop, overwrite = TRUE)
  dbWriteTable(mydb, "temporalCropQuantity", temporalCropQuantity, overwrite = TRUE)
  dbWriteTable(mydb, "nokavaHarvest", nokavaHarvest, overwrite = TRUE)
  dbWriteTable(mydb, "noCoconutHarvest", noCoconutHarvest, overwrite = TRUE)
  dbWriteTable(mydb, "noCocoaHarvest", noCocoaHarvest, overwrite = TRUE)
  dbWriteTable(mydb, "noCoffeeHarvest", noCoffeeHarvest, overwrite = TRUE)
  dbWriteTable(mydb, "noVanillaHarvest", noVanillaHarvest, overwrite = TRUE)
  dbWriteTable(mydb, "noPepperHarvest", noPepperHarvest, overwrite = TRUE)
  dbWriteTable(mydb, "noTahitianHarvest", noTahitianHarvest, overwrite = TRUE)
  dbWriteTable(mydb, "noVanillaHarvest", noVanillaHarvest, overwrite = TRUE)
  dbWriteTable(mydb, "other_mult_crop_roster", other_mult_crop_roster, overwrite = TRUE)
  dbWriteTable(mydb, "cattleComposition", cattleComposition, overwrite = TRUE)
  dbWriteTable(mydb, "sheepComposition", sheepComposition, overwrite = TRUE)
  dbWriteTable(mydb, "goatComposition", goatComposition, overwrite = TRUE)
  dbWriteTable(mydb, "pigComposition", goatComposition, overwrite = TRUE)
  dbWriteTable(mydb, "poultryComposition", poultryComposition, overwrite = TRUE)
  dbWriteTable(mydb, "agricultureDetails", agricultureDetails, overwrite = TRUE)
  dbWriteTable(mydb, "noPlantedForest", noPlantedForest, overwrite = TRUE)
  dbWriteTable(mydb, "fishCatch", fishCatch, overwrite = TRUE)
  dbWriteTable(mydb, "aquaCalturecatch", aquaCalturecatch, overwrite = TRUE)
  dbWriteTable(mydb, "smallMachinesComposition", smallMachinesComposition, overwrite = TRUE)
  dbWriteTable(mydb, "heavymachinesComposition", heavymachinesComposition, overwrite = TRUE)
  dbWriteTable(mydb, "assistanceComposition", assistanceComposition, overwrite = TRUE)
  dbWriteTable(mydb, "labourHiredComposition", labourHiredComposition, overwrite = TRUE)
  dbWriteTable(mydb, "organisationComposition", organisationComposition, overwrite = TRUE)
  dbWriteTable(mydb, "interviewer_actions", interviewer_actions, overwrite = TRUE)
  dbWriteTable(mydb, "interview_status", interview_status, overwrite = TRUE)
  dbWriteTable(mydb, "frame", frame, overwrite = TRUE)
  dbWriteTable(mydb, "interview_level", interview_level, overwrite = TRUE)
  dbWriteTable(mydb, "province", province, overwrite = TRUE)
  dbWriteTable(mydb, "ac", ac, overwrite = TRUE)
  dbWriteTable(mydb, "ea", ea, overwrite = TRUE)
  dbWriteTable(mydb, "enumerator", enumerator, overwrite = TRUE)
  dbWriteTable(mydb, "ea_squarekilometers", ea_squarekilometers, overwrite=TRUE)
  dbWriteTable(mydb, "ea_area", ea_area, overwrite = TRUE)
  
  
}
  
  #Delete extracted tables from the working folder
  
  delete_files <- function(){
    file.remove(list.files(pattern = "*.tab"))
    
    #Define the file name that will be deleted
    fn <- "data/VNAC2022_1_Tabular_All.zip"
    #Check its existence
    if (file.exists(fn)) {
      #Delete file if it exists
      file.remove(fn)
    }
  }
  
  
user_message <- function(){
  "Download has been completed..."
}

#Deploy Application to the shiny.io server

dployApp <- function(){
  library(rsconnect)
  rsconnect::setAccountInfo(name='vnsodashboard', token='187092F1FEF9167B04CD2E1ADC3095CC', 
                            secret='jJtn4/cY1b7jWN2aLe+yMcy4uivsx85kbvfR7wj/')
  
  deployApp()
  
}

#Disconnect from database
disconnect <- function(){
  dbDisconnect(mydb)
}


intStatus <- dbGetQuery(mydb, "SELECT * FROM interview_status")

test_case <- intStatus %>%
  mutate(status = case_when(interview__status == 0 ~'Restored', 
            interview__status == 20 ~'Created',
            interview__status == 40 ~'SupervisorAssigned',
            interview__status == 60 ~'InterviewerAssigned',
            interview__status == 65 ~'RejectedBySupervisor',
            interview__status == 80 ~'ReadyForInterview',
            interview__status == 85 ~'SendToCapi',
            interview__status == 95 ~'Restarted',
            interview__status == 100 ~'Completed',
            interview__status == 120 ~'ApprovedBySupervisor',
            interview__status == 125 ~'RejectedByHeadquarters',
            interview__status == 130 ~'ApprovedByHeadquarters',
            
            ))

test_case_new <- test_case %>%
  group_by(responsible, status) %>%
  summarise(total = n())

test_case_new_pivot <- test_case_new %>%
  pivot_wider(names_from = status, values_from = total, values_fill = 0)

