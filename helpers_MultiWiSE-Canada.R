##################################################################################
# helpers_MultiWiSE-Canada.R
# Purpose:       Helper functions required to run the offline version of the MultiWiSE Canada Dashboard
# Note:          This file is sourced by the primary R script (MultiWiSE-Canada_offline.R).
#                Do NOT modify this file unless you are updating the helper functions.
# Last modified: April 9, 2026
##################################################################################

options(stringsAsFactors = FALSE)
options(error = NULL)
`%||%` <- function(x, y) if (is.null(x)) y else x  # Null-coalescing helper: return y when x is NULL

# read_and_check() --------------------------------------------------------
# Wrapper function to run all the processes required for the MultiWiSE approach
run_multiwise <- function(res_hist,
                          exp_wind,
                          pm25_dir,
                          out_dir) {
  
  
  print("Reading in and cleaning up data...")
  
  # Read in and process residential history and exposure window file
  res_df <- read_and_check(
    path          = res_hist_file,
    file_label    = "Residential History"
  )
  exp_df <- read_and_check(
    path          = exp_wind_file,
    file_label    = "Exposure Window"
  )
  
  res_df <- res_df %>% mutate(StartDate = as.Date(StartDate), EndDate = as.Date(EndDate))
  exp_df <- exp_df %>% mutate(ExposureStartDate = as.Date(ExposureStartDate),
                              ExposureEndDate   = as.Date(ExposureEndDate))
  
  res_ids <- unique(res_df$IndividualID[!is.na(res_df$PostalCode)])
  exp_ids <- unique(exp_df$IndividualID[!is.na(exp_df$ExposureStartDate)])
  
  
  missing_in_exp <- setdiff(res_ids, exp_ids)
  if (length(missing_in_exp)) {
    stop(sprintf(
      "Unable to process data. There is a mismatch between the IndividualIDs in the residential history and exposure window files. The following IDs are included in the residential history file but not in the exposure window file: %s. Please ensure that the files have the same number of IndividualIDs, and that each IndividualID exists in both files.",
      paste(missing_in_exp, collapse = ", ")
    ), call. = FALSE)
  }
  
  missing_in_res <- setdiff(exp_ids, res_ids)
  if (length(missing_in_res)) {
    stop(sprintf(
      "Unable to process data. There is a mismatch between the IndividualIDs in the exposure window file and the residential history file. The following IDs are included in the exposure window file but not in the residential history file: %s. Please ensure that the files have the same number of IndividualIDs, and that each IndividualID exists in both files.",
      paste(missing_in_res, collapse = ", ")
    ), call. = FALSE)
  }
  
  histories <- merge(res_df, exp_df, by = "IndividualID", all = TRUE, sort = FALSE)
  
  ids_all <- unique(histories$IndividualID[!is.na(histories$IndividualID)])
  ids_all <- ids_all[order(ids_all)]
  n <- length(ids_all); if (n == 0) stop("No IndividualID values detected after merging files.", call. = FALSE)
  
  warnings_collected <- list()
  errors_collected   <- list()
  out_list <- list()
  
  i <- 0L
  dt <- data.table::as.data.table(histories)
  data.table::setorder(dt, IndividualID, StartDate)
  
  print("Processing data...")
  pb <- txtProgressBar(min = 0, max = length(ids_all), style = 3)
  
  for (id in ids_all) {
    i <- i + 1L
    
    setTxtProgressBar(pb, i)
    
    d <- dt[dt$IndividualID == id,]
    warns <- character(); errs <- character()
    res <- tryCatch(
      withCallingHandlers(
        fn_single(d, 
                  file_path = weekly_pm25_directory),
        warning = function(w) {
          m <- conditionMessage(w)
          if (!grepl('warnings? in .*summarise\\(', m, ignore.case = TRUE)) warns <<- c(warns, m)
          invokeRestart("muffleWarning")
        }
      ),
      error = function(e) { errs <<- c(errs, conditionMessage(e)); NULL }
    )
    msg <- if (length(errs)) paste(unique(errs), collapse = "; ")
    else if (length(warns)) paste(unique(warns), collapse = "; ")
    else "No issues"
    
    if (!exists("log_dt_local")) log_dt_local <- data.table::data.table(IndividualID=character(), Message=character())
    log_dt_local <- rbind(log_dt_local, data.table::data.table(IndividualID=id, Message=msg))
    if (!is.null(res)) out_list[[as.character(id)]] <- res
    
  }
  close(pb)
  
  
  if (length(out_list) > 0) {
    metrics_wide <- data.table::rbindlist(
      lapply(names(out_list), function(id) {
        df <- out_list[[id]]$metrics
        df$IndividualID <- id
        data.table::as.data.table(df)[, c("IndividualID",
                                          setdiff(names(df), "IndividualID")), with = FALSE]
      }), use.names = TRUE, fill = TRUE
    )
    
    data_list  <- setNames(lapply(out_list, `[[`, "data"), names(out_list))
    
    batch <- list(metrics_wide = metrics_wide,
                  data         = data_list)
    
    batch_clean <- save_batch_outputs(batch, out_dir_root = output_directory)
    
    if (exists("log_dt_local")) {
      data.table::fwrite(log_dt_local, file = file.path(output_directory, paste0("Warning_Error_Log_",Sys.Date(),".csv")))
      batch_clean$error_log <- log_dt_local
      rm(log_dt_local)
    }
    
    print(paste0("Finished processing data. Outputs saved to: ", output_directory))
  }
  
  return(batch_clean)
}

# read_and_check() --------------------------------------------------------
# Read helper with strict file/type checks and robust date validation
read_and_check <- function(path,
                           file_label = c("Residential History", "Exposure Window")[1],
                           date_cols  = NULL) {
  
  if (file_label == 'Residential History') {
    required_cols <- c("IndividualID", "PostalCode", "StartDate", "EndDate")
  } else {
    required_cols <- c("IndividualID", "ExposureStartDate", "ExposureEndDate")
  }
  
  # Basic file checks
  allowed_ext <- c("csv", "xls", "xlsx")
  ext <- tolower(tools::file_ext(path))
  if (!ext %in% allowed_ext) {
    stop(sprintf(
      "The provided %s file is not a .csv, .xls, or .xlsx file. Please ensure that both the Residential History and Exposure Window files are provided in .csv, .xls, or .xlsx.",
      basename(path)
    ), call. = FALSE)
  }
  
  if (!file.exists(path))
    stop(paste0("The ",file_label, " file does not exist at the provided file path (",path,")."), call. = FALSE)
  
  # Read with dates as TEXT to avoid silent mis-parsing
  df <- switch(
    ext,
    csv  = data.table::fread(path, colClasses = "character"),             # all text
    xls  = readxl::read_excel(path, col_types = "text"),                  # all text
    xlsx = readxl::read_excel(path, col_types = "text")                   # all text
  )
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  
  # Header validation
  if (!all(required_cols %in% names(df))) {
    stop(sprintf(
      "The %s file does not have the correct column names. Please use the following column names for the %s file: %s.",
      file_label, file_label, paste(required_cols, collapse = ", ")
    ), call. = FALSE)
  }
  
  #  Date-string validation
  if (is.null(date_cols)) {
    date_cols <- grep("Date$", names(df), value = TRUE)
  }
  
  if (length(date_cols)) {
    bad_by_col <- vapply(
      date_cols,
      function(nm) check_date_format(df[[nm]]),
      integer(1)
    )
    bad_cols  <- names(bad_by_col)[bad_by_col > 0]
    bad_total <- sum(bad_by_col)
    
    if (bad_total > 0) {
      col_list <- switch(
        length(bad_cols),
        `1` = bad_cols,
        `2` = paste(bad_cols, collapse = " and "),
        paste0(paste(bad_cols[-length(bad_cols)], collapse = ", "),
               ", and ", bad_cols[length(bad_cols)])
      )
      
      stop(sprintf(
        paste(
          "Unable to process data.",
          "The %s column%s in the %s file has %d date(s) that are incorrectly formatted or missing.",
          "Please ensure all dates are formatted as follows: YYYY-MM-DD."
        ),
        col_list,
        ifelse(length(bad_cols) > 1, "s", ""),
        file_label,
        bad_total
      ), call. = FALSE)
    }
  }
  
  #  Return
  df
}

# check_date_format() --------------------------------------------------------
# Count invalid entries in a date vector; enforce YYYY-MM-DD and real calendar dates
check_date_format <- function(date_vec) {
  x <- trimws(as.character(date_vec))
  invalid_blank <- is.na(x) | x == ""
  matches_ymd <- grepl("^\\d{4}-\\d{2}-\\d{2}$", x)
  to_check <- which(!is.na(x) & matches_ymd)
  parsed   <- as.Date(x[to_check], format = "%Y-%m-%d")
  roundtrip_ok <- !is.na(parsed) & (format(parsed, "%Y-%m-%d") == x[to_check])
  invalid <- invalid_blank | !matches_ymd
  invalid[to_check] <- !roundtrip_ok
  sum(invalid, na.rm = TRUE)
}

# postal_code_error() --------------------------------------------------------
# Standardized postal-code error message
postal_code_error <- function(postal_code) {
  sprintf(
    "Error: Unable to process individual. The postal code %s provided in the residential history is incorrectly formatted and/or does not link to available data. Please ensure all postal codes included in the residential history file are correctly formatted (6 digits, no spaces) and located in one of the 13 provinces and territories in Canada.",
    postal_code
  )
}

# fn_build_individual_exposure_history() --------------------------------------------------------
# Build individual exposure history
fn_build_individual_exposure_history <- function(residential_history_table,
                                                 exposure_window_start,
                                                 exposure_window_end,
                                                 file_path) {
  
  # Basic validations
  if (!is.data.frame(residential_history_table))
    stop("Unable to process data. The provided residential history is not a data.frame. Please ensure you passed in a residential_history_table with columns PostalCode, StartDate, EndDate.")
  
  bad_pc_fmt <- !grepl("^[A-Za-z]\\d[A-Za-z]\\d[A-Za-z]\\d$",
                       residential_history_table$PostalCode %||% "")
  if (any(bad_pc_fmt)) {
    bad_first <- residential_history_table$PostalCode[bad_pc_fmt][1]
    stop(postal_code_error(bad_first), call. = FALSE)
  }
  
  bad_res_start <- check_date_format(residential_history_table$StartDate)
  bad_res_end   <- check_date_format(residential_history_table$EndDate)
  bad_exp_start <- check_date_format(exposure_window_start)
  bad_exp_end   <- check_date_format(exposure_window_end)
  
  if (bad_res_start + bad_res_end > 0)
    stop(sprintf(
      "Unable to process data. The StartDate and/or EndDate column(s) in the residential history file have %d date(s) that are incorrectly formatted or missing. Please ensure all dates are formatted as follows: YYYY-MM-DD.",
      bad_res_start + bad_res_end
    ), call. = FALSE)
  
  if (bad_exp_start + bad_exp_end > 0)
    stop(sprintf(
      "Unable to process data. The ExposureStartDate and/or ExposureEndDate column(s) in the exposure window file have %d date(s) that are incorrectly formatted or missing. Please ensure all dates are formatted as follows: YYYY-MM-DD.",
      bad_exp_start + bad_exp_end
    ), call. = FALSE)
  
  residential_history_table$StartDate <- as.Date(residential_history_table$StartDate)
  residential_history_table$EndDate   <- as.Date(residential_history_table$EndDate)
  exposure_window_start <- as.Date(exposure_window_start)
  exposure_window_end   <- as.Date(exposure_window_end)
  
  if (exposure_window_end < exposure_window_start) {
    stop(
      "Error: Unable to process individual. The provided end date for the exposure window occurs prior to the start date. Please ensure the end date occurs after the start date, and that the exposure window is at least 1 year in length.",
      call. = FALSE
    )
  }
  
  if (as.numeric(exposure_window_end - exposure_window_start + 1L) < 365) {
    stop(
      "Error: Unable to process individual. The provided exposure window is less than 1 year in length. Please ensure the exposure window is at least 1 year in length.",
      call. = FALSE
    )
  }
  
  # Prep residential history (merge duplicates, handle small gaps/overlaps)
  res_dt <- data.table::as.data.table(residential_history_table)[order(StartDate)]
  
  if (nrow(res_dt) > 1) {
    j <- 1L
    while (j < nrow(res_dt)) {
      if (res_dt$PostalCode[j] == res_dt$PostalCode[j + 1]) {
        res_dt$StartDate[j] <- min(res_dt$StartDate[j], res_dt$StartDate[j + 1])
        res_dt$EndDate[j]   <- max(res_dt$EndDate[j],   res_dt$EndDate[j + 1])
        res_dt <- res_dt[-(j + 1), ]
      } else {
        j <- j + 1L
      }
    }
  }
  
  if (any(res_dt$StartDate > res_dt$EndDate, na.rm = TRUE)) {
    pc <- res_dt$PostalCode[which(res_dt$StartDate > res_dt$EndDate)][1]
    stop(sprintf(
      "Error: Unable to process individual. In the residential history, the provided end date for postal code %s occurs prior to the start date. For all postal codes, please ensure the start date occurs before the end date, and that the residential history is contiguous and non-overlapping.",
      pc
    ), call. = FALSE)
  }
  
  if (nrow(res_dt) > 1) {
    for (i in seq_len(nrow(res_dt) - 1)) {
      prev_pc <- res_dt$PostalCode[i]
      next_pc <- res_dt$PostalCode[i + 1]
      s_next  <- res_dt$StartDate[i + 1]
      e_curr  <- res_dt$EndDate[i]
      if (s_next <= e_curr) {
        odays <- as.numeric(e_curr - s_next + 1)
        if (odays <= 90) {
          half <- ceiling(odays / 2)
          res_dt$EndDate[i]     <- s_next + half - 1
          res_dt$StartDate[i+1] <- s_next + half
          warning(sprintf(
            "Warning: Individual had a %d-day overlap in their residential history between %s and %s. To remove this overlap and ensure the residential history is contiguous, the first half of the overlapping period was assigned to %s and the second half assigned to %s.",
            odays, prev_pc, next_pc, prev_pc, next_pc
          ), call. = FALSE)
        } else {
          stop(sprintf(
            "Error: Individual had a %d-day overlap in their residential history between %s and %s. Because this overlap is greater than 90 days in length, the individual cannot be processed. Please ensure that the residential history is contiguous with no overlaps, or if there are overlaps, ensure that the overlaps are less than 90 days in length.",
            odays, prev_pc, next_pc
          ), call. = FALSE)
        }
      }
    }
  }
  
  if (nrow(res_dt) > 1) {
    for (i in seq_len(nrow(res_dt) - 1)) {
      prev_pc <- res_dt$PostalCode[i]
      next_pc <- res_dt$PostalCode[i + 1]
      gdays   <- as.numeric(res_dt$StartDate[i + 1] - res_dt$EndDate[i] - 1)
      if (gdays > 0 && gdays <= 90) {
        half <- floor(gdays / 2)
        res_dt$EndDate[i]     <- res_dt$EndDate[i] + half
        res_dt$StartDate[i+1] <- res_dt$StartDate[i+1] - (gdays - half)
        warning(sprintf(
          "Warning: Individual had a %d-day gap in their residential history between %s and %s. To fill this gap in residential history, the first half of gap was assigned to %s and the second half of the gap was assigned to %s.",
          gdays, prev_pc, next_pc, prev_pc, next_pc
        ), call. = FALSE)
      } else if (gdays > 90) {
        stop(sprintf(
          "Error: Individual had a %d-day gap in their residential history between %s and %s. Because this gap is greater than 90 days in length, the individual cannot be processed. Please ensure there are no gaps in the residential history, or if there are gaps, ensure that the gaps are less than 90 days in length.",
          gdays, prev_pc, next_pc
        ), call. = FALSE)
      }
    }
  }
  
  # Enforce data availability window (2010-01-01 to 2023-12-31)
  data_start <- as.Date("2010-01-01")
  data_end   <- as.Date("2023-12-31")
  orig_start <- exposure_window_start
  orig_end   <- exposure_window_end
  
  if (exposure_window_start < data_start) {
    exposure_window_start <- data_start
    warning(sprintf(
      "Warning: The provided start date (%s) of the exposure window is outside the data availability window %s. The exposure window start date was automatically set to January 1, 2010.",
      format(orig_start), sprintf("%s to %s", format(data_start), format(data_end))
    ), call. = FALSE)
  }
  if (exposure_window_end > data_end) {
    exposure_window_end <- data_end
    warning(sprintf(
      "Warning: The provided end date (%s) of the exposure window is outside the data availability window %s. The exposure window end date was automatically set to December 31, 2023.",
      format(orig_end), sprintf("%s to %s", format(data_start), format(data_end))
    ), call. = FALSE)
  }
  
  # Align exposure window to residential history
  res_min <- min(res_dt$StartDate, na.rm = TRUE)
  res_max <- max(res_dt$EndDate,   na.rm = TRUE)
  orig_start2 <- exposure_window_start
  orig_end2   <- exposure_window_end
  
  if (exposure_window_start < res_min) {
    exposure_window_start <- res_min
    warning(sprintf(
      "Warning: The start date (%s) of the exposure window occurs outside the range of dates provided in the residential history file. The start date of the exposure window was automatically set to %s to ensure alignment between the exposure window and residential history.",
      format(orig_start2), format(exposure_window_start)
    ), call. = FALSE)
  }
  
  if (exposure_window_end > res_max) {
    exposure_window_end <- res_max
    warning(sprintf(
      "Warning: The end date (%s) of the exposure window occurs outside the range of dates provided in the residential history file. The end date of the exposure window was automatically set to %s to ensure alignment between the exposure window and residential history.",
      format(orig_end2), format(exposure_window_end)
    ), call. = FALSE)
  }
  
  if (as.numeric(exposure_window_end - exposure_window_start + 1L) < 365) {
    stop(
      "Error: Unable to process individual. The provided exposure window is less than 1 year in length. Please ensure the exposure window is at least 1 year in length.",
      call. = FALSE
    )
  }
  
  # Build partial-week table and aggregate to weekly_exposure
  pw_list <- list(); ctr <- 1L; counterfactuals <- vector(mode = "numeric", length = nrow(res_dt))
  
  for (i in seq_len(nrow(res_dt))) {
    pc       <- res_dt$PostalCode[i]
    pc_start <- res_dt$StartDate[i]
    pc_end   <- res_dt$EndDate[i]
    
    overlap_start <- max(pc_start, exposure_window_start)
    overlap_end   <- min(pc_end,   exposure_window_end)
    if (overlap_end < overlap_start) next
    
    dt <- fn_get_postal_code_data(pc, file_path = file_path)
    
    dt[, c("epiweek_start_date","epiweek_end_date") :=
         lapply(.SD, as.Date), .SDcols = c("epiweek_start_date","epiweek_end_date")]
    
    counterfactuals[i] <- (dt %>%
                             filter(!is.na(modified_z),
                                    dplyr::between(modified_z, -2, 2),
                                    n_days == 7) %>%
                             pull(pm25_weekly_sum) %>% median(na.rm = TRUE) %>% replace_na(0))/7
    
    dt <- dt[epiweek_end_date >= overlap_start & epiweek_start_date <= overlap_end]
    if (nrow(dt) == 0) next
    
    dt[, `:=`(
      partial_interval_start = pmax(epiweek_start_date, overlap_start),
      partial_interval_end   = pmin(epiweek_end_date,   overlap_end)
    )]
    dt[, days_overlap     := as.numeric(partial_interval_end - partial_interval_start) + 1L]
    dt[, days_overlap     := ifelse(days_overlap < 7, days_overlap, n_days)]
    dt[, fraction_of_week := days_overlap / n_days]
    dt[, partial_pm25_weekly_sum := pm25_weekly_sum * fraction_of_week]
    dt[, partial_non_wfs_weekly_sum_pm25 := non_wfs_weekly_sum_pm25 * fraction_of_week]
    dt[, partial_n_days          := n_days * fraction_of_week]
    
    pw_list[[ctr]] <- dt[, .(
      epiweek_index, epiweek_start_date, epiweek_end_date,
      partial_pm25_weekly_sum,
      partial_non_wfs_weekly_sum_pm25,
      partial_n_days, PostalCode
    )]
    ctr <- ctr + 1L
  }
  
  
  if (ctr == 1L) stop("No overlapping data found for this individual's history.", call. = FALSE)
  
  combine_partial <- data.table::rbindlist(pw_list, fill = TRUE)
  
  weekly_exposure <- combine_partial[, .(
    weekly_pm25_sum_i = sum(partial_pm25_weekly_sum, na.rm = TRUE),
    weekly_non_wfs_pm25_sum_i = sum(partial_non_wfs_weekly_sum_pm25, na.rm = TRUE),
    n_days            = sum(partial_n_days,          na.rm = TRUE),
    PostalCode        = ifelse(uniqueN(PostalCode) > 1 & sum(partial_n_days,na.rm = TRUE) > 0, PostalCode[which.max(partial_n_days)], PostalCode),
    num_PCs        = uniqueN(PostalCode)
  ), by = .(epiweek_index, epiweek_start_date, epiweek_end_date)][order(epiweek_start_date)]
  
  weekly_exposure[, weekly_pm25_avg_i := weekly_pm25_sum_i / n_days]
  
  weekly_exposure[, weekly_non_wfs_pm25_avg_i := ifelse(weekly_non_wfs_pm25_sum_i == weekly_pm25_sum_i,
                                                        weekly_non_wfs_pm25_sum_i / n_days,
                                                        weekly_non_wfs_pm25_sum_i / 7)]
  
  weekly_exposure[, weekly_non_wfs_pm25_sum_i := ifelse(weekly_non_wfs_pm25_sum_i == weekly_pm25_sum_i,
                                                        weekly_non_wfs_pm25_sum_i,
                                                        weekly_non_wfs_pm25_sum_i * (n_days/7))]
  
  weekly_exposure[, weekly_wfs_pm25_sum_i := weekly_pm25_sum_i - weekly_non_wfs_pm25_sum_i]
  weekly_exposure[, weekly_wfs_pm25_avg_i := weekly_wfs_pm25_sum_i / n_days]
  weekly_exposure[, is_smoke_impacted := ifelse(!is.na(weekly_wfs_pm25_avg_i) & weekly_wfs_pm25_avg_i > 0, 1, 0)]
  
  weekly_exposure$overlap_days <- as.numeric((weekly_exposure$epiweek_end_date - weekly_exposure$epiweek_start_date)+1)
  
  if (nrow(weekly_exposure) > 0 && weekly_exposure$epiweek_start_date[1] < overlap_start)
    weekly_exposure <- weekly_exposure[-1]
  if (nrow(weekly_exposure) > 0 && weekly_exposure$epiweek_end_date[nrow(weekly_exposure)] > overlap_end)
    weekly_exposure <- weekly_exposure[-nrow(weekly_exposure)]
  
  if (nrow(weekly_exposure) > 0) {
    weekly_exposure[, week_sequence := .I]
  } else {
    warning("Warning: No overlapping data found for this individual's history.", call. = FALSE)
    weekly_exposure[, week_sequence := integer()]
  }
  
  weekly_exposure <- weekly_exposure[order(week_sequence)]
  weekly_exposure[, cumulative_wfs_pm25_sum_individual := cumsum(weekly_wfs_pm25_sum_i)]
  weekly_exposure[, cumulative_non_wfs_pm25_sum_individual := cumsum(weekly_non_wfs_pm25_sum_i)]
  weekly_exposure[, cumulative_total_pm25_sum_individual := cumsum(weekly_pm25_sum_i)]
  
  return(list(weekly_exposure,counterfactuals))
}

# province_lookup & province_abbr --------------------------------------------------------
# Match postal codes to provinces, and provinces to their abbreviations
province_lookup <- data.frame(
  start_pc = c('A','B','C','E','G','H','J','K','L','M','N','P','R','S','T','V','X','Y'),
  province = c(
    'Newfoundland','NovaScotia','PrinceEdwardIsland','NewBrunswick',
    'Quebec','Quebec','Quebec','Ontario','Ontario','Ontario','Ontario',
    'Ontario','Manitoba','Saskatchewan','Alberta','BritishColumbia',
    'NorthwestTerritories-Nunavut','Yukon'
  ),
  provinceID = c('NL', 'NS', 'PE', 'NB', 
                 'QC', 'QC', 'QC', 'ON', 'ON', 'ON', 'ON',
                 'ON', 'MB', 'SK', 'AB', 'BC',
                 'NT-NU', 'YT'),
  stringsAsFactors = FALSE
)
province_abbr <- c(
  Newfoundland = "NL", NovaScotia = "NS", PrinceEdwardIsland = "PE",
  NewBrunswick = "NB", Quebec = "QC", Ontario = "ON", Manitoba = "MB",
  Saskatchewan = "SK", Alberta = "AB", BritishColumbia = "BC",
  "NorthwestTerritories-Nunavut" = "NT", Yukon = "YT"
)

# fn_get_postal_code_data() --------------------------------------------------------
# Fetch & load postal-code file from local filepath
# TO-DO: UPDATE THIS
fn_get_postal_code_data <- function(postal_code,
                                    file_path) {
  
  if (!dir.exists(file_path))
    stop(paste0("We ran into issues when trying to access the provided file path for the PM2.5 data: ",file_path,
                ". Please ensure you have set `weekly_pm25_directory` to the correct file path."),
         call. = FALSE)
  
  prefix <- toupper(substr(postal_code, 1, 1))
  info   <- subset(province_lookup, start_pc == prefix)
  if (!nrow(info)) stop(postal_code_error(postal_code), call. = FALSE)
  province_folder <- info$provinceID
  abbr <- province_abbr[[info$province]]
  if (is.null(abbr)) stop(postal_code_error(postal_code), call. = FALSE)
  
  filename     <- paste0(abbr, "_PC_CanOSSEM_weekly_wfs_pm25_", postal_code, "_all_yrs.RDS")
  filename_path <- paste0(file_path,'/',province_folder,'/',filename)
  
  if (file.exists(filename_path)) {
    dt <- data.table::as.data.table(readRDS(filename_path))
  } else {
    stop(paste0('We ran into issues trying to access the data for postal code: ', postal_code, 
                ' using the following file path: ', filename_path,'. Please ensure that the `weekly_pm25_directory` is correct.'
    ), call. = FALSE)
  }
  
  dt[, PostalCode := postal_code]
  
  if (!"epiweek_index" %in% names(dt)) {
    if ("epiweek" %in% names(dt)) data.table::setnames(dt, "epiweek", "epiweek_index")
    else stop("File lacks epiweek index", call. = FALSE)
  }
  
  if(nrow(dt) != uniqueN(dt$epiweek_start_date) || nrow(dt) != uniqueN(dt$epiweek_index)) {
    stop(paste0("We ran into issues when proccessing the weekly PM2.5 data for postal code: ", postal_code, ". Please ensure you have downloaded the correct weekly PM2.5 dataset."),
         call. = FALSE)
  }
  
  dt
}

# fn_calc_modified_z() --------------------------------------------------------
# Function to calculate the modified Z score
fn_calc_modified_z <- function(x) {
  med  <- median(x, na.rm = TRUE)
  madv <- mad(x, constant = 1.4826, na.rm = TRUE)
  if (madv == 0) return(rep(NA_real_, length(x)))
  (x - med) / madv
}

# fn_identify_smoke_episodes() --------------------------------------------------------
# Function to identify smoke episodes and severe smoke episodes
fn_identify_smoke_episodes <- function(is_smoke_impacted, cont_week,
                                       weekly_sum_wfs_pm25_values,
                                       episode_threshold = 0,
                                       num_weeks_in_episode = 2,
                                       max_no_smoke_gap = 3,
                                       weeks_needed_above_threshold = 1) {
  
  n <- length(is_smoke_impacted); epi_id <- integer(n)
  cur_id <- 0; in_epi <- FALSE; smoke_wks <- wks_above <- gap <- 0
  bridge <- integer(0); fail <- function(id) epi_id[epi_id == id] <<- 0
  
  for (i in seq_len(n)) {
    if (i > 1 && cont_week[i] - cont_week[i-1] > 1) {
      if (in_epi && (smoke_wks < num_weeks_in_episode ||
                     wks_above < weeks_needed_above_threshold)) fail(cur_id)
      in_epi <- FALSE; smoke_wks <- wks_above <- gap <- 0; bridge <- integer(0)
    }
    
    if (is_smoke_impacted[i] == 1) {
      if (!in_epi) { cur_id <- cur_id + 1; in_epi <- TRUE; smoke_wks <- wks_above <- 0 }
      if (length(bridge) > 0) { epi_id[bridge] <- cur_id; bridge <- integer(0) }
      smoke_wks <- smoke_wks + 1
      if (weekly_sum_wfs_pm25_values[i] > episode_threshold) wks_above <- wks_above + 1
      epi_id[i] <- cur_id; gap <- 0
      
    } else if (in_epi) {
      gap <- gap + 1; bridge <- c(bridge, i)
      if (gap > max_no_smoke_gap) {
        if (smoke_wks < num_weeks_in_episode ||
            wks_above < weeks_needed_above_threshold) fail(cur_id)
        in_epi <- FALSE; bridge <- integer(0)
      }
    }
  }
  
  if (in_epi && (smoke_wks < num_weeks_in_episode ||
                 wks_above < weeks_needed_above_threshold)) fail(cur_id)
  
  for (id in setdiff(unique(epi_id), 0)) {
    idx       <- which(epi_id == id)
    smoke_idx <- idx[is_smoke_impacted[idx] == 1]
    epi_id[idx[idx > max(smoke_idx)]] <- 0
  }
  uniq <- setdiff(unique(epi_id), 0)
  for (k in seq_along(uniq)) epi_id[epi_id == uniq[k]] <- k
  
  epi_id
}

# fn_compute_metrics() --------------------------------------------------------
# Function to compute the 12 MultiWiSE metrics and additional variables
fn_compute_metrics <- function(df,
                               counterfactuals,
                               exposure_start,
                               exposure_end,
                               smoke_week_threshold = 0) {
  
  MICRO_TO_MILLI <- 1 / 1000
  df <- df %>% dplyr::mutate(is_smoke_impacted = as.integer(weekly_wfs_pm25_avg_i > smoke_week_threshold))
  
  total_smoke_weeks <- sum(df$is_smoke_impacted, na.rm = TRUE)
  mean_smoke_week   <- if (total_smoke_weeks > 0)
    mean(df$weekly_wfs_pm25_avg_i[df$is_smoke_impacted == 1], na.rm = TRUE) else 0
  
  weeks_wfs_over_5         <- sum(df$weekly_wfs_pm25_avg_i > 5,  na.rm = TRUE)
  weeks_total_pm25_over_25 <- sum(df$weekly_wfs_pm25_avg_i > 0 & df$weekly_pm25_avg_i > 25, na.rm = TRUE)
  
  num_normal_episodes <- max(df$episode_id,        na.rm = TRUE)
  num_severe_episodes <- max(df$severe_episode_id, na.rm = TRUE)
  
  epi_summary_span <- df %>%
    dplyr::filter(episode_id > 0) %>%
    dplyr::group_by(episode_id) %>%
    dplyr::summarise(span_weeks = max(week_sequence) - min(week_sequence) + 1,
                     .groups = "drop")
  
  longest_episode_len <- if (nrow(epi_summary_span) > 0)
    max(epi_summary_span$span_weeks, na.rm = TRUE) else 0
  
  epi_df <- df %>% dplyr::filter(episode_id > 0, is_smoke_impacted == 1)
  episode_summary <- epi_df %>%
    dplyr::group_by(episode_id) %>%
    dplyr::summarise(avg_pm25 = mean(weekly_wfs_pm25_avg_i, na.rm = TRUE),
                     .groups = "drop")
  
  worst_episode_exposure <- if (nrow(episode_summary) > 0)
    max(episode_summary$avg_pm25, na.rm = TRUE) else 0
  
  severe_pm25_cum <- df %>% dplyr::filter(severe_episode_id > 0, is_smoke_impacted == 1) %>%
    dplyr::summarise(sum(weekly_wfs_pm25_sum_i, na.rm = TRUE)) %>% dplyr::pull()
  
  obs_cumul_mg <- dplyr::last(df$cumulative_total_pm25_sum_individual) * MICRO_TO_MILLI
  wfs_cumul_mg <- dplyr::last(df$cumulative_wfs_pm25_sum_individual)   * MICRO_TO_MILLI
  non_wfs_cumul_mg  <- sum(df$weekly_non_wfs_pm25_sum_i, na.rm = TRUE) * MICRO_TO_MILLI
  
  pct_wfs_from_severe <- if (wfs_cumul_mg > 0)
    100 * severe_pm25_cum * MICRO_TO_MILLI / wfs_cumul_mg else NA_real_
  
  if (nrow(epi_df) == 0) {
    avg_time_between <- round(as.numeric(exposure_end - exposure_start) / 7, 2)
  } else {
    episode_info <- epi_df %>%
      dplyr::group_by(episode_id) %>%
      dplyr::summarise(start_epi_date = min(epiweek_start_date, na.rm = TRUE),
                       end_epi_date   = max(epiweek_end_date,   na.rm = TRUE),
                       .groups        = "drop") %>%
      dplyr::arrange(start_epi_date)
    gap_starts <- c(exposure_start, episode_info$end_epi_date)
    gap_ends   <- c(episode_info$start_epi_date, exposure_end)
    gaps_days  <- as.numeric(gap_ends - gap_starts)
    avg_time_between <- round(mean(gaps_days) / 7, 2)
  }
  
  numbered <- tibble::tibble(
    `1_Cumulative_WFS_PM25`        = round(wfs_cumul_mg, 2),
    `2_WFS_Fraction`               = round(100 * wfs_cumul_mg / obs_cumul_mg, 2),
    `3_Average_WFS_PM25`           = round(mean_smoke_week, 2),
    `4_Any_WFS`                    = total_smoke_weeks,
    `5_WFS_PM25_exceeds_5`         = weeks_wfs_over_5,
    `6_Total_PM25_exceeds_25`      = weeks_total_pm25_over_25,
    `7_WFS_Episodes`               = num_normal_episodes,
    `8_Severe_Episodes`            = num_severe_episodes,
    `9_Longest_Episode`            = longest_episode_len,
    `10_Worst_Episode`             = round(worst_episode_exposure, 2),
    `11_WFS_from_Severe_Episodes`  = round(pct_wfs_from_severe, 1),
    `12_Average_Recovery`          = avg_time_between
  )
  
  extras <- tibble::tibble(
    `Cumulative_Total_PM25`     = round(obs_cumul_mg, 2),
    `Average_Total_PM25`        = round(mean(df$weekly_pm25_avg_i, na.rm = TRUE), 2),
    `Cumulative_NonWFS_PM25`    = round(non_wfs_cumul_mg, 2),
    `Average_NonWFS_PM25`       = round(mean(df$weekly_non_wfs_pm25_avg_i, na.rm = TRUE), 2),
    `Average_Counterfactual_Value` = round(mean(counterfactuals), 2)
  )
  
  dplyr::bind_cols(numbered, extras)
}

# Plotting functions --------------------------------------------------------
median_range_color        <- "#B0C6DF"
wildfire_attributable_col <- "#FF8572"
PLOT_BASE_SIZE   <- 12   # Fixed base font size across figures
PLOT_LINE_THIN   <- 0.25 # Thin axis/line strokes for clean look
PROFILE_PLOT_HEIGHT_PX <- 560  # Taller canvas for longer y-axis

# fn_plot_cumulative_sum_by_year() --------------------------------------------------------
# Create cumulative exposure plot
fn_plot_cumulative_sum_by_year <- function(df, y_limits = NULL, line_thin = PLOT_LINE_THIN) {
  df_plot <- df %>% dplyr::mutate(
    total   = cumsum(weekly_pm25_sum) / 1000,
    non_wfs = cumsum(weekly_non_wfs_pm25_sum) / 1000,
    wfs     = cumsum(weekly_wfs_pm25_sum)     / 1000
  )
  y_max <- if (is.null(y_limits))
    max(c(df_plot$total, df_plot$non_wfs, df_plot$wfs), na.rm = TRUE) * 1.20
  else y_limits[2]
  
  ggplot2::ggplot(df_plot, ggplot2::aes(x = epiweek_start_date)) +
    ggplot2::geom_line(ggplot2::aes(y = total,   color = "Total"),   linewidth = 1) +
    ggplot2::geom_line(ggplot2::aes(y = non_wfs, color = "Non-WFS"), linewidth = 1) +
    ggplot2::geom_line(ggplot2::aes(y = wfs,     color = "WFS"),     linewidth = 1) +
    ggplot2::scale_color_manual(values = c(
      "Total"   = "black",
      "Non-WFS" = median_range_color,
      "WFS"     = wildfire_attributable_col
    ), breaks = c("Total","Non-WFS","WFS"), name = NULL) +
    ggplot2::scale_x_date(date_breaks = "1 year", date_labels = "%Y", expand = c(0, 0)) +
    ggplot2::scale_y_continuous(expand = c(0, 0), limits = c(0, y_max)) +
    ggplot2::labs(x = "Year",
                  y = expression("Cumulative PM"[2.5]*" (mg/m"^3*")")) +
    ggplot2::theme_classic(base_size = PLOT_BASE_SIZE) +
    ggplot2::theme(
      legend.position = "top",
      axis.line  = ggplot2::element_line(linewidth = PLOT_LINE_THIN),
      axis.ticks = ggplot2::element_line(linewidth = PLOT_LINE_THIN),
      plot.title = ggplot2::element_text(hjust = 0)
    )
}

# fn_plot_time_series_weekly_mean() --------------------------------------------------------
# Create weekly exposure time series plot
fn_plot_time_series_weekly_mean <- function(df, y_limits = NULL,
                                            line_thin = PLOT_LINE_THIN,
                                            alpha_norm = 0.20,
                                            alpha_severe = 0.20,
                                            fill_norm = "khaki2",
                                            fill_severe = "orange2") {
  
  df_plot <- df %>% dplyr::mutate(
    wfs     = dplyr::coalesce(weekly_wfs_pm25_avg,     0),
    non_wfs = dplyr::coalesce(weekly_non_wfs_pm25_avg, 0),
    total   = dplyr::coalesce(weekly_pm25_avg,         0)
  )
  
  y_max <- if (is.null(y_limits))
    max(c(df_plot$total, df_plot$non_wfs, df_plot$wfs), na.rm = TRUE) * 1.20
  else y_limits[2]
  
  plot <- ggplot2::ggplot() 
  
  if(sum(df_plot$episode_id > 0)) {
    epi_spans <- df_plot %>% dplyr::filter(episode_id > 0) %>% dplyr::group_by(episode_id) %>%
      dplyr::summarise(start = min(epiweek_start_date, na.rm = TRUE),
                       end   = max(epiweek_end_date, na.rm = TRUE), .groups = "drop")
    
    plot <- plot + ggplot2::geom_rect(data = epi_spans, inherit.aes = FALSE,
                                      ggplot2::aes(xmin = start-3, xmax = end+3, ymin = -Inf, ymax = Inf,
                                                   fill = "Episode"), alpha = alpha_norm) 
  }
  if(sum(df_plot$severe_episode_id > 0)) {
    sev_spans <- df_plot %>% dplyr::filter(severe_episode_id > 0) %>% dplyr::group_by(severe_episode_id) %>%
      dplyr::summarise(start = min(epiweek_start_date, na.rm = TRUE),
                       end   = max(epiweek_end_date, na.rm = TRUE), .groups = "drop")
    
    plot <- plot + ggplot2::geom_rect(data = sev_spans, inherit.aes = FALSE,
                                      ggplot2::aes(xmin = start-3, xmax = end+3, ymin = -Inf, ymax = Inf,
                                                   fill = "Severe episode"), alpha = alpha_severe)
  }
  plot <- plot +
    ggplot2::geom_line(data = df_plot,
                       ggplot2::aes(x = epiweek_start_date, y = total,   color = "Total"),   linewidth = 0.9) +
    ggplot2::geom_line(data = df_plot,
                       ggplot2::aes(x = epiweek_start_date, y = non_wfs, color = "Non-WFS"), linewidth = 0.9) +
    ggplot2::geom_line(data = df_plot,
                       ggplot2::aes(x = epiweek_start_date, y = wfs,     color = "WFS"),     linewidth = 0.9) 
  
  if (sum(df_plot$severe_episode_id > 0) || sum(df_plot$episode_id > 0)) {
    plot <- plot + ggplot2::scale_fill_manual(values = c("Episode" = fill_norm, "Severe episode" = fill_severe),
                                              name = NULL,
                                              guide = ggplot2::guide_legend(override.aes = list(alpha = 0.4))) 
  }
  
  plot + 
    ggplot2::scale_color_manual(values = c(
      "Total"   = "black",
      "Non-WFS" = median_range_color,
      "WFS"     = wildfire_attributable_col
    ), breaks = c("Total","Non-WFS","WFS"),
    name   = NULL,
    guide  = ggplot2::guide_legend(override.aes = list(fill = NA, linewidth = 1.5))) +
    ggplot2::scale_x_date(date_breaks = "1 year", date_labels = "%Y", expand = c(0, 0)) +
    ggplot2::scale_y_continuous(expand = c(0, 0), limits = c(0, y_max)) +
    ggplot2::labs(x = "Year",
                  y = expression("Mean PM"[2.5]*" ("*mu*"g/m"^3*")")) +
    ggplot2::theme_classic(base_size = PLOT_BASE_SIZE) +
    ggplot2::theme(
      legend.position = "top",
      axis.line  = ggplot2::element_line(linewidth = line_thin),
      axis.ticks = ggplot2::element_line(linewidth = line_thin),
      plot.title = ggplot2::element_text(hjust = 0)
    )
}

# fn_run_individual_analysis() --------------------------------------------------------
# Function to run the analyses for a given individual
fn_run_individual_analysis <- function(res_hist,
                                       start_exposure,
                                       end_exposure,
                                       file_path,
                                       IndividualID) {
  
  output <- fn_build_individual_exposure_history(
    residential_history_table = res_hist,
    exposure_window_start     = start_exposure,
    exposure_window_end       = end_exposure,
    file_path       = file_path
  )
  
  trajectory <- output[[1]]
  counterfactuals <- output[[2]]
  
  trajectory[, episode_id := fn_identify_smoke_episodes(
    is_smoke_impacted          = is_smoke_impacted,
    cont_week                  = week_sequence,
    weekly_sum_wfs_pm25_values = weekly_wfs_pm25_sum_i,
    episode_threshold          = 0,
    num_weeks_in_episode       = 2,
    max_no_smoke_gap           = 3,
    weeks_needed_above_threshold = 1
  )]
  
  trajectory[, severe_episode_id := fn_identify_smoke_episodes(
    is_smoke_impacted          = is_smoke_impacted,
    cont_week                  = week_sequence,
    weekly_sum_wfs_pm25_values = weekly_wfs_pm25_sum_i,
    episode_threshold          = 250,
    num_weeks_in_episode       = 1,
    max_no_smoke_gap           = 3,
    weeks_needed_above_threshold = 1
  )]
  
  # Combine normal + severe into contiguous episode blocks for visuals
  episodes_combined <- rep(0L, nrow(trajectory))
  epi_counter <- 1L
  for (i in seq_len(nrow(trajectory))) {
    curr_has_epi <- trajectory$episode_id[i] > 0L ||
      trajectory$severe_episode_id[i] > 0L
    next_has_epi <- if (i < nrow(trajectory))
      (trajectory$episode_id[i+1] > 0L ||
         trajectory$severe_episode_id[i+1] > 0L) else FALSE
    if (curr_has_epi) {
      episodes_combined[i] <- epi_counter
      if (!next_has_epi) epi_counter <- 1 + epi_counter
    }
  }
  trajectory[, episode_id := episodes_combined]
  
  metrics <- fn_compute_metrics(
    trajectory,
    counterfactuals,
    exposure_start = start_exposure,
    exposure_end   = end_exposure
  )
  
  # Defer figure generation; return data & metrics only
  list(id = IndividualID,
       metrics = metrics,
       data = trajectory)
}

# fn_single() --------------------------------------------------------
# Function to run analyses for an individual (handles repeated/duplicate windows)
fn_single <- function(d, file_path) {
  id <- unique(d$IndividualID)
  
  exp_rows_raw <- d[, .(ExposureStartDate, ExposureEndDate)]
  exp_rows_raw <- exp_rows_raw[!is.na(ExposureStartDate) & !is.na(ExposureEndDate)]
  if (nrow(exp_rows_raw) == 0) {
    stop(sprintf("Missing exposure window for ID %s. Ensure ExposureStartDate/ExposureEndDate are provided.", id), call. = FALSE)
  }
  
  res_rows_n <- nrow(unique(d[, .(PostalCode, StartDate, EndDate)]))
  exp_unique <- unique(exp_rows_raw)
  multiple_distinct <- nrow(exp_unique) > 1L
  identical_dups <- (nrow(exp_unique) == 1L) && res_rows_n > 0L &&
    (nrow(exp_rows_raw) > res_rows_n)
  
  if (multiple_distinct || identical_dups) {
    warning("Warning: Multiple exposure windows were provided for this individual. Data were processed using the first available exposure window.", call. = FALSE)
  }
  
  win <- exp_unique[order(ExposureStartDate, ExposureEndDate)][1, ]
  
  fn_run_individual_analysis(
    res_hist       = d[, .(PostalCode, StartDate, EndDate)],
    start_exposure = win$ExposureStartDate,
    end_exposure   = win$ExposureEndDate,
    file_path     = file_path,
    IndividualID   = id
  )
}

# sanitize_names() --------------------------------------------------------
# Clean varaible names
sanitize_names <- function(x) {
  x %>%
    stringi::stri_trans_general("Latin-ASCII") %>%
    gsub("₂", "2", ., fixed = TRUE) %>%
    gsub("₅", "5", ., fixed = TRUE) %>%
    gsub("PM2\\.5", "PM2_5", ., perl = TRUE) %>%
    gsub("[/()]", "_", ., fixed = TRUE) %>%
    gsub("[^A-Za-z0-9_]+", "_", .) %>%
    gsub("_+", "_", .) %>%
    gsub("^_|_$", "", .)
}

# save_batch_outputs() --------------------------------------------------------
# Save outputs to user-defined filepath
save_batch_outputs <- function(batch, out_dir_root) {
  if (!dir.exists(out_dir_root)) {
    stop(paste0("The file path provided for saving the outputs (",out_dir_root,") does not exist."), call. = FALSE)
  }
  
  # Metrics CSV
  metrics_csv <- data.table::copy(batch$metrics_wide)
  data.table::setnames(metrics_csv, sanitize_names(names(metrics_csv)))
  data.table::fwrite(metrics_csv, file = file.path(out_dir_root, paste0("MultiWiSE_Metrics_",Sys.Date(),".csv")))
  
  # Weekly PM2.5 long table
  long_weekly <- data.table::rbindlist(batch$data, idcol = "IndividualID", use.names = TRUE)
  long_weekly[, epiweek_index_rel := seq_len(.N), by = IndividualID]
  
  rename_map <- c(
    "weekly_pm25_avg_i"         = "weekly_pm25_avg",
    "weekly_wfs_pm25_avg_i"     = "weekly_wfs_pm25_avg",
    "weekly_non_wfs_pm25_avg_i" = "weekly_non_wfs_pm25_avg",
    "weekly_pm25_sum_i"         = "weekly_pm25_sum",
    "weekly_wfs_pm25_sum_i"     = "weekly_wfs_pm25_sum",
    "weekly_non_wfs_pm25_sum_i" = "weekly_non_wfs_pm25_sum",
    "epiweek_index"             = "epiweek_index_old",
    "epiweek_index_rel"         = "epiweek_index"
  )
  for (old in names(rename_map)) {
    if (old %in% names(long_weekly)) {
      data.table::setnames(long_weekly, old, rename_map[[old]])
    }
  }
  cols_keep <- c(
    "IndividualID", "PostalCode", "epiweek_index", "n_days",
    "epiweek_start_date", "epiweek_end_date",
    "weekly_pm25_avg", "weekly_wfs_pm25_avg", "weekly_non_wfs_pm25_avg",
    "weekly_pm25_sum", "weekly_wfs_pm25_sum", "weekly_non_wfs_pm25_sum",
    "episode_id", "severe_episode_id"
  )
  long_weekly <- long_weekly[, ..cols_keep]
  
  batch_clean <- list(multiwise_metrics = metrics_csv,
                      weekly_data       = long_weekly)
  
  # Rounding for CSV
  avg_cols <- c("weekly_pm25_avg", "weekly_wfs_pm25_avg", "weekly_non_wfs_pm25_avg")
  sum_cols <- c("weekly_pm25_sum", "weekly_wfs_pm25_sum", "weekly_non_wfs_pm25_sum")
  for (cl in intersect(avg_cols, names(long_weekly))) long_weekly[[cl]] <- signif(as.numeric(long_weekly[[cl]]), 3)
  for (cl in intersect(sum_cols, names(long_weekly))) long_weekly[[cl]] <- signif(as.numeric(long_weekly[[cl]]), 4)
  
  data.table::fwrite(long_weekly, file = file.path(out_dir_root, paste0("Weekly_PM25_Estimates_",Sys.Date(),".csv")))
  
  return(batch_clean)
  
}
