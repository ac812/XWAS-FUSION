#' Run each command in the Commands file
#' args[1] is the path to the config file
#' args[2] is the path to the source code directory
suppressPackageStartupMessages(library(data.table))

args <- commandArgs(trailingOnly=TRUE)
#load config (using same arguments are ConfigChrScheduler.R)
source(paste0(args[2], "/Config.R"))

#contains list of JobIDs submitted to queue
jobList <- NULL

for(i in 1:22){
	#create submission file
	message(paste0("Creating job file for chromosome ", i))
	chrCommandFile <- paste0(config$workingDir,"/temp/tmp_weights/CommandsScheduler_", i, ".sh")
	chrLogOut <- paste0(config$workingDir,"/temp/tmp_weights/CommandsScheduler_", i, ".o")
	chrLogError <- paste0(config$workingDir,"/temp/tmp_weights/CommandsScheduler_", i, ".e")
	commandFile <- paste0(config$workingDir, "/temp/tmp_weights/Commands_", i, ".txt")
	#get number of markers to process
	n <- as.integer(system2("wc",args=c("-l", commandFile, "| awk '{print $1}'"), stdout=TRUE))
	
	cat(paste0("#!/bin/bash", "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("#$ -S /bin/bash", "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("#$ -o ", chrLogOut, "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("#$ -e ", chrLogError, "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("#$ -N FUSIONWeights", i, "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("#$ -q ", config$queue, "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("#$ -t 1-", n, "\n\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("tmp=$(awk -v var=\"$SGE_TASK_ID\" 'NR==var' ", commandFile, ")", "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("export R_LIBS=", config$pathRLIBS, "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("cd ", config$workingDir, "/weights/", "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("Rscript $tmp", "\n"), file=chrCommandFile, append=TRUE)
	cat(paste0("exit 0", "\n"), file=chrCommandFile, append=TRUE)
	message("Job submission start.")
	system(paste0(config$path_qsub, " ", chrCommandFile))
	jobList <- c(jobList, paste0("FUSIONWeights",i))
}

#wait for all chromosomes to be processed
#wait until job array finished
holdWeightsFile <- paste0(config$workingDir, "/temp/tmp_weights/hold.sh")
holdWeightsOut <- paste0(config$workingDir, "/temp/tmp_weights/hold.o")
holdWeightsError <- paste0(config$workingDir, "/temp/tmp_weights/hold.e")
cat(paste0("#!/bin/bash", "\n"), file=holdWeightsFile, append=TRUE)
cat(paste0("#$ -S /bin/bash", "\n"), file=holdWeightsFile, append=TRUE)
cat(paste0("#$ -q ", config$queue, "\n"), file=holdWeightsFile, append=TRUE)
cat(paste0("#$ -o ", holdWeightsOut, "\n"), file=holdWeightsFile, append=TRUE)
cat(paste0("#$ -e ", holdWeightsError, "\n"), file=holdWeightsFile, append=TRUE)
cat(paste0("#$ -N FUSIONHold", "\n"), file=holdWeightsFile, append=TRUE)
cat(paste0("#$ -M ", config$email, "\n"), file=holdWeightsFile, append=TRUE)
cat(paste0("#$ -m ea ", "\n\n"), file=holdWeightsFile, append=TRUE) #send mail at the end of the job or if the job has been aborted
cat(paste0("export R_LIBS=", config$pathRLIBS, "\n"), file=holdWeightsFile, append=TRUE)
cat(paste0("Rscript ", args[2], "/PostProcessWeights.R ", args[1], " ", args[2], "\n"), file=holdWeightsFile, append=TRUE)
system(paste0(config$path_qsub, " -hold_jid ", paste(jobList, collapse=","), " ", holdWeightsFile))
