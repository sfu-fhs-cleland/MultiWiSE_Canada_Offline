##################################################################################
# MultiWiSE-Canada_offline.R
# Purpose:       An R script that provides the same functionality as the MultiWiSE Canada Dashboard but can
#                be run in secure research environments without internet access or R Shiny capabilities
# Note:          Documentation on the MultiWiSE Canada dashboard can be found at: https://sfu-fhs-cleland.shinyapps.io/MultiWiSE-Canada/
# Last Modified: April 9, 2026
##################################################################################

### USER PROVIDED INPUT ------------------------------------------------------------
## You must provide the following inputs to the R script:

# 1. The path to the directory you are working from. The directory must contain both this script and 'helpers_MultiWiSE-Canada.R'
working_directory <- ''

# 2. The path to the directory you want to save outputs to (CSV files with results and PNG files with figures)
output_directory <- ''

# 3. The file path for the exposure window file (.csv, .xls, or .xlsx file)
#    For guidance on how to format this file, please see the README
exp_wind_file <- ''

# 4. The file path for the residential history file (.csv, .xls, or .xlsx file)
#    For guidance on how to format this file, please see the README
res_hist_file <- ''

# 5. The path to the directory with the pre-processed postal code-level weekly PM2.5 data. 
#    This data can be downloaded at: https://www.dropbox.com/scl/fo/ynbextn32g93lj3xirh6s/AJauEcPHlBlOX6Cl3lcEcec?rlkey=1se5jryxdzlsqtlsqp1nclil9&st=liq7nzrv&dl=0
#    Please ensure that your local directory organizes the files using the same structure and naming convention as the Dropbox folder
weekly_pm25_directory <- ''

### SET-UP R SCRIPT ------------------------------------------------------------
# Set working directory
setwd(working_directory)

# Load helper functions
source('helpers_MultiWiSE-Canada.R')

# Load libraries
required_packages <- c("data.table", "dplyr", "tidyr", "ggplot2", "patchwork", "tibble", "stringi", "readxl")
new_packages <- required_packages[!vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)]
if (length(new_packages)) install.packages(new_packages)
lapply(required_packages, library, character.only = TRUE)

### GENERATE METRICS ------------------------------------------------------------
# Process data using MultiWiSE approach. 
# The metrics, weekly PM2.5 values, and error log .csv files are saved to 'output_directory'
results <- run_multiwise(
  res_hist = res_hist_file,
  exp_wind = exp_wind_file,
  pm25_dir = weekly_pm25_directory,
  out_dir = output_directory
)

# Extract relevant results to use as needed
multiwise_metrics <- results$multiwise_metrics
weekly_pm25_data <- results$weekly_data
error_warning_log <- results$error_log

### GENERATE PLOTS (OPTIONAL) ------------------------------------------------------------
# Specify IDs you want to generate plots for. A plot for each ID will be saved to 'output_directory'
ids <- c('')

for (id in ids) {
  weekly_pm25_id <- weekly_pm25_data[weekly_pm25_data$IndividualID == id,]
  
  # Cumulative plot
  p1 <- fn_plot_cumulative_sum_by_year(weekly_pm25_id) + theme(axis.title = ggplot2::element_text(size = 20), 
                                                   axis.text = ggplot2::element_text(size = 16), 
                                                   legend.text = ggplot2::element_text(size = 18))
  
  # Time series plot
  p2 <- fn_plot_time_series_weekly_mean(weekly_pm25_id) + theme(axis.title = ggplot2::element_text(size = 20), 
                                                    axis.text = ggplot2::element_text(size = 16), 
                                                    legend.text = ggplot2::element_text(size = 18))
  # Combine plots
  p <- (p1 / p2) +
    patchwork::plot_annotation(
      title = id,
      theme = ggplot2::theme(plot.title = ggplot2::element_text(size = 26, face = "bold", hjust = 0.5)
      )
    )
  
  # Display in RStudio
  print(p)
  
  # Save to 'output_directory'
  file <- paste0(output_directory,"/MultiWiSE_Figure_",id,"_",Sys.Date(), ".png")
  ggplot2::ggsave(filename = file, plot = p, width = 14, height = 19, dpi = 300)
}
