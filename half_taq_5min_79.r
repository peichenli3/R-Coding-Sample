#!/usr/bin/env Rscript
# 
# Created by: Peichen Li
# Last Updated: 07/31/2021
# 
# Purpose:
# 
# This Rscript file is for getting 79 trades for each Stock in daily TAQ
# Start from 9:30AM, Pick the last trade for each 5-min time interval, until the last trade for the 3:50PM - 3:55PM time interval
# For the last few minutes (3:55PM - 4:05PM), we pick two trades
# One is the Closing Option (the first trade whose TR_SCOND contains character "6") and the other is the trade before the Closing Option
# Thus 79 trades in total
# 
# Inputs: 
#    
# 1. NYSE daily TAQ data "ctm_YYYYMMDD.sas7bdat" (SAS), 2012 to 2020
# 2. "taq_location_HalfTrading.csv"
#     
# Outputs: 
#    
# taq_79 "ctm_YYYYMMDD.csv"
# 
# Steps:
#     1. Import the daily TAQ data of SAS format
#     2. Process last 5 min (before_co + closing_options = 2 trades)
#     3. Process 9:30am - 3:55pm (77)
#     4. taq_all = 79 rows, including variables:
#     5. Date | SYM_ROOT | TIME_M | Index | PRICE | SIZE | Volume_5min
#     6. fwrite to csv



# Process half day trading (ends around 13:00)
# Generally 0703, Black Friday and 1224 for each year
# Method: align to the right and fill empty with NA
# This is for taq_79
# Append_taq_20 should be the same (but only rebalance days == half trading days)




library(haven)
library(data.table)
library(tidyverse)


args <- commandArgs(trailingOnly = TRUE)

# taq_location_HalfTrading
file_loc = read.csv('/ocean/projects/ses190002p/peichen3/taq_location_HalfTrading.csv')

file_sas = toString(file_loc$file_location[as.numeric(args)])

file_name <- regmatches(file_sas, regexpr("ctm_[0-9]+", file_sas))

file_csv = paste("/ocean/projects/ses190002p/peichen3/taq_79/", file_name, ".csv", sep="")

# Define function
half_taq_5min_79 <- function(file) {
  
  
  
  ############################
  # 1. Process the daily TAQ #
  ############################
  
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
  
  # Quit if not half trading day
  if (all(closing_options$TIME_5min_nth < 190)) {print("Half trading day, continue!")
  } else { #continue the script
    stop("Complete trading day, stop!")   
  }
  
  
  # Release
  rm(closing_options)
  gc()
  
  
  
  ###########################
  # 2. Process last 5min    #
  ###########################
  
  
  
  # taq after 12:55pm
  # label with closing_option
  # label with first_closing_option
  # label with id_within_tick
  # Align to right (1pm to 4pm)
  taq_post1255_label <- as.data.table(taq)[TIME_5min_nth >= 155, ] %>%
    mutate(closing_option = ifelse(grepl("6", TR_SCOND), 1, 0)) %>%
    group_by(SYM_ROOT) %>%
    arrange(as.numeric(unlist(TIME_M)), .by_group = TRUE) %>%
    mutate(first_closing_option = closing_option== 1 & !duplicated(closing_option == 1)) %>%
    mutate(id_within_tick = row_number())
  
  
  # 12:55pm to 3:55pm
  before_closing_options <- as.data.table(taq_post1255_label)[,if(1 %in% first_closing_option) .SD[1:min(which(1==first_closing_option)) - 1],by=SYM_ROOT] %>%
    group_by(SYM_ROOT) %>%
    arrange(as.numeric(unlist(TIME_M)), .by_group = TRUE) %>%
    mutate(Volume_5min = sum(SIZE)) %>%
    slice(n()) %>%
    mutate(TIME_5min_nth = 191) %>%
    select(-closing_option, -first_closing_option, -id_within_tick)
  
  # 1:00pm to 4:00pm
  closing_options <- as.data.table(taq_post1255_label)[,if(1 %in% first_closing_option) .SD[1:min(which(TRUE==first_closing_option))],by=SYM_ROOT] %>%
    group_by(SYM_ROOT) %>%
    arrange(as.numeric(unlist(TIME_M)), .by_group = TRUE) %>%
    mutate(Volume_5min = sum(SIZE)) %>%
    slice(n()) %>%
    mutate(TIME_5min_nth = 192) %>%
    mutate(Volume_5min = SIZE) %>%
    select(-closing_option, -first_closing_option, -id_within_tick)
  
  
  last_5min <- rbind(before_closing_options, closing_options)
  
  # Release
  rm(taq_post1255_label)
  rm(before_closing_options)
  rm(closing_options)
  gc()
  
  
  
  #################################
  ##  2.Process 9:30am - 12:55pm ##
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
  
  
  # Filter for intraday trading (after 9:30am and before 12:55pm)
  # Group by Ticker and each 5min time interval
  # and then pick the last trade record
  # Add trading volume
  taq <- as.data.table(taq)[TIME_5min_nth %in% 114:154, ] %>%
    group_by(SYM_ROOT, TIME_5min_nth) %>%
    mutate(Volume_5min = sum(SIZE)) %>%
    arrange(-TIME_5min_position, .by_group = TRUE) %>%
    slice(1) %>%
    ungroup()
  
  # Align to right (e.g. 12:55pm to 3:55pm, so TIME_5min_nth +36 )
  taq$TIME_5min_nth <- taq$TIME_5min_nth + 36
  
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
half_taq_5min_79(file_sas)



