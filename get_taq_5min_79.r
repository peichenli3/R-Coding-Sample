#!/usr/bin/env Rscript
# Purpose:
#         daily TAQ data manipulation about ETFs
# Inputs: 
#    
#         NYSE daily TAQ data 
#     
# Outputs: 
#    
#    
# Author: Peichen Li
# Date: 07/22/2021
# Steps:
#     1. Import the daily TAQ data of SAS format
#     2. Process last 5 min (before_co + closing_options = 2)
#     3. Process 9:30am - 3:55pm (77)
#     4. taq_all = 79 rows, including variables:
#     5. Date | SYM_ROOT | TIME_M | Index | PRICE | SIZE | Volume_5min
#     6. fwrite to csv


library(haven)
library(data.table)
library(tidyverse)


args <- commandArgs(trailingOnly = TRUE)

file_loc = read.csv('/ocean/projects/ses190002p/peichen3/taq_location.csv')

file_sas = toString(file_loc$file_location[as.numeric(args)])

file_name <- regmatches(file_sas, regexpr("ctm_[0-9]+", file_sas))

file_csv = paste("/ocean/projects/ses190002p/peichen3/taq_79/", file_name, ".csv", sep="")

# Define function
get_taq_5min_79 <- function(file) {

    
    
  ###########################
  # 1. Prepare the datasets #
  ###########################
  
  # Import data and variables
  DATE = read_sas(file, n_max = 1)$DATE
  taq <- as.data.table(read_sas(file, col_select = c("TIME_M", "SYM_ROOT", "EX", "SYM_SUFFIX", "SIZE", "PRICE", "TR_SCOND")))[SYM_SUFFIX == "", ]
  
  # Calculate "TIME_5min_nth", which stands for n-th 5min time interval
  taq$TIME_5min_nth <- as.numeric(unlist(taq$TIME_M)) %/% 300
  
  # Calculate "TIME_5min_remainder", which stands for the position in each 5min time interval
  taq$TIME_5min_position <- as.numeric(unlist(taq$TIME_M)) %% 300
  
  
  # Take out closing options (TR_COND contains "6" & the larger size one)
  closing_options <- taq[grepl("6", TR_SCOND) == TRUE] %>%
    group_by(SYM_ROOT) %>%
    arrange(-SIZE, .by_group = TRUE) %>%
    slice(1)
  
  # Quit if half intraday
  if (all(closing_options$TIME_5min_nth %in% 156:157)) {stop("half trading day, stop!")
  } else { #continue the script
  print("Complete trading day, continue!")   
  }
  
  
  # Release
  rm(closing_options)
  gc()
  
  
  
  ###########################
  # 2. Process last 5min    #
  ###########################
  
  
  
  # taq afte 3:55pm
  # label with closing_option
  # label with first_closing_option
  # label with id_within_tick
  taq_post355_label <- as.data.table(taq)[TIME_5min_nth >= 191, ] %>%
    mutate(closing_option = ifelse(grepl("6", TR_SCOND), 1, 0)) %>%
    group_by(SYM_ROOT) %>%
    arrange(as.numeric(unlist(TIME_M)), .by_group = TRUE) %>%
    mutate(first_closing_option = closing_option== 1 & !duplicated(closing_option == 1)) %>%
    mutate(id_within_tick = row_number())
  
  
  
  before_closing_options <- as.data.table(taq_post355_label)[,if(1 %in% first_closing_option) .SD[1:min(which(1==first_closing_option)) - 1],by=SYM_ROOT] %>%
    group_by(SYM_ROOT) %>%
    arrange(as.numeric(unlist(TIME_M)), .by_group = TRUE) %>%
    mutate(Volume_5min = sum(SIZE)) %>%
    slice(n()) %>%
    mutate(TIME_5min_nth = 191) %>%
    select(-closing_option, -first_closing_option, -id_within_tick)
  
  
  closing_options <- as.data.table(taq_post355_label)[,if(1 %in% first_closing_option) .SD[1:min(which(TRUE==first_closing_option))],by=SYM_ROOT] %>%
    group_by(SYM_ROOT) %>%
    arrange(as.numeric(unlist(TIME_M)), .by_group = TRUE) %>%
    mutate(Volume_5min = sum(SIZE)) %>%
    slice(n()) %>%
    mutate(TIME_5min_nth = 192) %>%
    select(-closing_option, -first_closing_option, -id_within_tick)
  
  
  last_5min <- rbind(before_closing_options, closing_options)
  
  # Release
  rm(taq_post355_label)
  rm(before_closing_options)
  rm(closing_options)
  gc()
  
  
  
  #################################
  ##  2. Process 9:30am - 3:55pm ##
  #################################
  
  # create 79 time slots for each ticker
  taq_5min_79 <- as.data.table(taq) %>%
    select(SYM_ROOT) %>%
    group_by(SYM_ROOT) %>%
    slice(1) %>%
    mutate(freq = 79) %>%
    slice(rep(seq_len(n()), freq)) %>% 
    select(-freq) %>%
    group_by(SYM_ROOT) %>%
    mutate(Index = row_number()) %>%
    mutate(TIME_5min_nth = Index + 113)
    
    
  # Filter for intraday trading (after 9:30am and before 3:55pm)
  # Group by Ticker and each 5min time interval
  # and then pick the last trade record
  # Add trading volume
  taq <- as.data.table(taq)[TIME_5min_nth %in% 114:190, ] %>%
    group_by(SYM_ROOT, TIME_5min_nth) %>%
    mutate(Volume_5min = sum(SIZE)) %>%
    arrange(-TIME_5min_position, .by_group = TRUE) %>%
    slice(1) %>%
    ungroup()
  
  
  
  
  #######################################
  ######    taq all day 79 x 5min   #####
  #######################################
  
  
  
  # Append the tables
  taq_all <- rbind(taq, last_5min) %>%
    group_by(SYM_ROOT) %>%
    arrange(as.numeric(unlist(TIME_M)), .by_group = TRUE) %>%
    select(SYM_ROOT, TIME_M, TIME_5min_nth, PRICE, SIZE, Volume_5min) %>%
    as.data.table()
  
  # left join for 79 x 5min for each ticker
  taq_79 <- left_join(taq_5min_79, taq_all, by = c("SYM_ROOT", "TIME_5min_nth"), suffix = c(".x", ".y")) %>%
    mutate(Date = DATE) %>%
    select(Date, SYM_ROOT, TIME_M, TIME_5min_nth, Index, PRICE, SIZE, Volume_5min)
  
  # Release
  rm(taq)
  rm(last_5min)
  gc()
  
  # write to disk
  fwrite(taq_79, file = file_csv, row.names = FALSE)
  print(paste(file_csv, "written", sep=" "))
  rm(taq_all, taq_5min_79)
  gc()
  return(1)
}


# Apply function
get_taq_5min_79(file_sas)







