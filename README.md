# MultiWiSE_Canada_Offline
An R script that provides the same functionality as the MultiWiSE Canada Dashboard but can be run in secure research environments without internet access or R Shiny capabilities. The MultiWiSE Canada dashboard and associated documentation can be found at: https://sfu-fhs-cleland.shinyapps.io/MultiWiSE-Canada/.

To run the offline version of the dashboard, you need to download both the primary R script (MultiWiSE-Canada_offline.R) and the R script with the helper functions (helpers_MultiWiSE-Canada.R). You also need to download the pre-processed postal code-level weekly PM2.5 estimates, which are available at: https://www.dropbox.com/scl/fo/ynbextn32g93lj3xirh6s/AJauEcPHlBlOX6Cl3lcEcec?rlkey=1se5jryxdzlsqtlsqp1nclil9&st=liq7nzrv&dl=0. Please ensure that the local directory you download the PM2.5 estimates to organizes the files using the same structure and naming convention as the Dropbox folder.

To run the offline version of the dashboard, you need to enter the following information into the primary R script (MultiWiSE-Canada_offline.R): 
1. The path to the working directory. The directory must contain both this primary and helper R scripts. 
2. The path to the directory you want to save outputs to (CSV files with results and PNG files with figures)
3. The file path for the exposure window file (.csv, .xls, or .xlsx file). For guidance on how to format this file, please see the README PDF.
4. The file path for the residential history file (.csv, .xls, or .xlsx file). For guidance on how to format this file, please see the README PDF.
5. The path to the directory with the pre-processed postal code-level weekly PM2.5 data. 
