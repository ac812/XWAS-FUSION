#' Once all weights have been generated, run FUSION.profile_wgt.R
#' args[1] is the path to the config file
#' args[2] is the path to the source code

args <- commandArgs(trailingOnly=TRUE)
source(paste0(args[2], "/Config.R"))
message("All weights computed")
#make a WGTLIST file which lists paths to each of the *.RDat files
system(paste0("find ", config$workingDir, "/weights/ -name \"*.RDat\" > ", config$workingDir, "/weights/WGTLIST"))
system(paste0("Rscript ", args[2], "/FUSION.profile_wgt.R ", config$workingDir, "/weights/WGTLIST > ", config$workingDir, "/weights/weights.profile 2>", config$workingDir, "/weights/weights.profile.err"))

