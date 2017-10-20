#' ----------------------------------------------------------------
#' Configures the commands to be executed to get the weights for 
#' each marker by parallelizing over chromosomes
#' args[1] is the path to the config file
#' args[2] is the path to the source code
#'-----------------------------------------------------------------


#' Creates the submission file that submits Config_compute_weights.R to SGE scheduler.
#' @param chrSchedulerFile Full path of job submission file.
#' @param chrLogOut Full path of job .out file.
#' @param chrLogError Full path of job .error file.
#' @param queue Queue name of where job is to be submitted.
#' @param pathRLIBS RLIBS environmental variable.
#' @param codePath Full path of the source code directory.
#' @param configPath Full path of configuration file.
createSGEChrSchedulerSubmissionFile <- function(chrSchedulerFile, chrLogOut, chrLogError, queue, pathRLIBS, codePath, configPath){
	cat(paste0("#!/bin/bash", "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#$ -S /bin/bash", "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#$ -o ", chrLogOut, "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#$ -e ", chrLogError, "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#$ -N FUSIONChr", "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#$ -q ", queue, "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#$ -t 1-22", "\n\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("export R_LIBS=", pathRLIBS, "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("Rscript ", codePath, "/Config_compute_weights.R ",  configPath, " ", codePath, " ", "$SGE_TASK_ID", "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("exit 0", "\n"), file=chrSchedulerFile, append=TRUE)
}

#' Creates the submission file that submits Config_compute_weights.R to SLURM scheduler.
#' @param chrSchedulerFile Full path of job submission file.
#' @param chrLogOut Full path of job .out file.
#' @param chrLogError Full path of job .error file.
#' @param queue Queue name of where job is to be submitted.
#' @param pathRLIBS RLIBS environmental variable.
#' @param codePath Full path of the source code directory.
#' @param configPath Full path of configuration file.
createSLURMChrSchedulerSubmissionFile <- function(chrSchedulerFile, chrLogOut, chrLogError, queue, pathRLIBS, codePath, configPath){
	cat(paste0("#!/bin/bash", "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#SBATCH --output=", chrLogOut, "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#SBATCH --error=", chrLogError, "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#SBATCH --job_name=FUSIONChr", "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#SBATCH --ntasks=1", "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("#SBATCH --array=1-22", "\n\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("export R_LIBS=", pathRLIBS, "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("Rscript ", codePath, "/Config_compute_weights.R ",  configPath, " ", codePath, " ", "$SLURM_ARRAY_TASK_ID", "\n"), file=chrSchedulerFile, append=TRUE)
	cat(paste0("exit 0", "\n"), file=chrSchedulerFile, append=TRUE)
}


#' Creates the submission file that submits Schedule_compute_weights.R to SGE scheduler.
#' @param holdChrFile Full path of job submission file.
#' @param holdChrOut Full path of job .out file.
#' @param holdChrError Full path of job .error file.
#' @param queue Queue name of where job is to be submitted.
#' @param pathRLIBS RLIBS environmental variable.
#' @param codePath Full path of the source code directory.
#' @param configPath Full path of configuration file.
createSGEHoldSubmissionFile <- function(holdChrFile, holdChrOut, holdChrError, queue, pathRLIBS, codePath, configPath){
	cat(paste0("#!/bin/bash", "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#$ -S /bin/bash", "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#$ -q ", queue, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#$ -o ", holdChrOut, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#$ -e ", holdChrError, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#$ -N FUSIONChrHold", "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("export R_LIBS=", pathRLIBS, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("Rscript ", codePath, "/Schedule_compute_weights_SGE.R ", configPath, " ", codePath, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("exit 0", "\n"), file=holdChrFile, append=TRUE)
}

#' Creates the submission file that submits Schedule_compute_weights.R to SLURM scheduler.
#' @param holdChrFile Full path of job submission file.
#' @param holdChrOut Full path of job .out file.
#' @param holdChrError Full path of job .error file.
#' @param queue Queue name of where job is to be submitted.
#' @param pathRLIBS RLIBS environmental variable.
#' @param codePath Full path of the source code directory.
#' @param configPath Full path of configuration file.
createSLURMHoldSubmissionFile <- function(holdChrFile, holdChrOut, holdChrError, queue, pathRLIBS, codePath, configPath){
	cat(paste0("#!/bin/bash", "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#SBATCH --output=", holdChrOut, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#SBATCH --error=", holdChrError, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#SBATCH --job_name=FUSIONChr", "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("#SBATCH --ntasks=1", "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("export R_LIBS=", pathRLIBS, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("Rscript ", codePath, "/Schedule_compute_weights_SLURM.R ", configPath, " ", codePath, "\n"), file=holdChrFile, append=TRUE)
	cat(paste0("exit 0", "\n"), file=holdChrFile, append=TRUE)
}



#=========================================================
#		Main code
#=========================================================
args <- commandArgs(trailingOnly=TRUE)

#load config (using same arguments are ConfigChrScheduler.R)
source(paste0(args[2], "/Config.R"))
#create temp directory to save temp files
unlink(paste0(config$workingDir, "/temp"), recursive=TRUE)
unlink(paste0(config$workingDir, "/temp/tmp_weights"), recursive=TRUE)
unlink(paste0(config$workingDir, "/PLINK"), recursive=TRUE)
unlink(paste0(config$workingDir, "/weights"), recursive=TRUE)
unlink(paste0(config$workingDir, "/temp/ChrPLINK"), recursive=TRUE)
dir.create(paste0(config$workingDir, "/temp"))
dir.create(paste0(config$workingDir, "/temp/tmp_weights"),recursive=TRUE)
dir.create(paste0(config$workingDir, "/temp/ChrPLINK"), recursive=TRUE)
dir.create(paste0(config$workingDir, "/weights"))
dir.create(paste0(config$workingDir, "/PLINK"))

#create submission file
message("Parallelising over chromosomes")
chrSchedulerFile <- paste0(config$workingDir,"/temp/tmp_weights/ChrScheduler.sh")
chrLogOut <- paste0(config$workingDir,"/temp/tmp_weights/ChrScheduler.o")
chrLogError <- paste0(config$workingDir,"/temp/tmp_weights/ChrScheduler.e")
#submission script to be executed after all chromosomes have been processed
holdChrFile <- paste0(config$workingDir, "/temp/tmp_weights/holdChrScheduler.sh")
holdChrOut <- paste0(config$workingDir, "/temp/tmp_weights/holdChrScheduler.o")
holdChrError <- paste0(config$workingDir, "/temp/tmp_weights/holdChrScheduler.e")

#create submission script depending on scheduler
if(config$scheduler == "SGE"){
	createSGEChrSchedulerSubmissionFile(chrSchedulerFile, chrLogOut, chrLogError, config$queue, config$pathRLIBS, args[2], args[1])
	system(paste0(config$path_qsub, " ", chrSchedulerFile))
	#wait for all the chromosomes to be processed and then run Schedule_compute_weights.R
	createSGEHoldSubmissionFile(holdChrFile, holdChrOut, holdChrError, config$queue, config$pathRLIBS, args[2], args[1])
	system(paste0(config$path_qsub, " -hold_jid FUSIONChr ", holdChrFile))
}

if(config$scheduler == "SLURM")
{
	createSLURMChrSchedulerSubmissionFile(chrSchedulerFile, chrLogOut, chrLogError, config$queue, config$pathRLIBS, args[2], args[1])
	system(paste0(config$path_sbatch, " ", chrSchedulerFile))
	#wait for all the chromosomes to be processed and then run Schedule_compute_weights.R - 
	#NOTE Array jobs and job to run after array jobs are complete have to be the same name for singleton to work
	createSLURMHoldSubmissionFile(holdChrFile, holdChrOut, holdChrError, config$queue, config$pathRLIBS, args[2], args[1])
	system(paste0(config$path_sbatch, " --dependency=singleton ", holdChrFile))
}



