#' ----------------------------------------------------------------
#' Creates PLINK file for each marker.
#' args[1] is the path to the config file
#' args[2] is the path to the source code
#' args[3] is the chromosome number [1-22]
#'-----------------------------------------------------------------
# Process input to compute weights
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(dplyr))

#'-------------------------------------------------------
#'  FUNCTIONS
#'-------------------------------------------------------

#' Validate expression files.
#' @param expressionDir Path of directory where expression data is saved
validate.expression.files <- function(expressionDir){
	message("Checking Expression Files")
	#check that markers in expressionPath are unique
	file.path <- paste0(expressionDir, "/expression22.txt")
	expression <- fread(input=file.path)
	markerIDs.expression <- expression[,markerID]
	if(length(markerIDs.expression) != unique(length(markerIDs.expression))){
		stop(paste0("ERROR: MarkerIDs are not unique! in ", file.path))
	}
	
	#<TODO>
	#get list of inds in .fam file
	#get list of inds in expression file
	#if length of inds in .fam != in length of inds .expression then error
	#if length of inds in intersection != length of inds then error
}

#' Extract common SNPs between LDREF and geno
#' @param ldrefDir Path of directory where LDREF is saved
#' @param genoDir Path of directory where the genotype data is saved
#' @param workingDir Path of the working directory
#' @param chr Chromosome number
#' @param pathPLINK path to PLINK executable
#' @param pathPLINKOut path to directory where to save the filtered chr PLINK files
extract.common.SNPs <- function(ldrefDir, genoDir, workingDir, chr, pathPLINK, pathPLINKOut){
	message(paste0("Extracting common SNPs between chr PLINK files and LDREF in ", chr))
	ldref.file <- paste0(ldrefDir, "/1000G.EUR.", chr, ".bim")
	ldref.data <- fread(ldref.file)
	ldref.snps <- ldref.data[,V2]
		
	geno.file <- paste0(genoDir, "/geno", chr, ".bim")
	geno.data <- fread(geno.file)
	geno.snps <- geno.data[,V2]
		
	snps <- intersect(ldref.snps, geno.snps)
	snps.file <- paste0(workingDir, "/temp/ChrPLINK/snps", chr, ".txt")
	write(snps, file=snps.file)
	system(paste0(config$pathPLINK, " --bfile ", genoDir, "/geno", chr, " --extract ", snps.file, " --make-bed --out ", pathPLINKOut, "/genoFusion", chr), ignore.stdout=T, ignore.stderr=T, wait=T)
}

#'Get cis-region of marker and save in separate PLINK file.  This function is for markers that have a single position.
#'@param sourceDir Path to source code
#'@param workingDir Path to working directory
#'@param mapDir Path to map files directory
#'@param pathPLINKin Path to input PLINK directory
#'@param expressionDir Path to expression files directory
#'@param chr Chromosome number
#'@param commandsFile File that has the list of commands to run to compute weights for chromosome <chr>
#'@param pathPLINK Path to PLINK executable
#'@param pathGCTA Path to GCTA executable
#'@param pathGemma Path to Gemma executable
#'@param outPLINKDir Path to directory where to save the marker PLINK files
createMarkerSinglePLINK <- function(sourceDir, workingDir, mapDir, pathPLINKin, expressionDir, chr, commandsFile, pathPLINK, pathGCTA, pathGemma, outPLINKDir){
	#load chromosome files
	map.file <- paste0(mapDir, "/map", chr, ".txt")
	map.data <- fread(map.file)
	message("Creating PLINK files for chromosome ", chr, " ...")
	genoFusion.file <- paste0(pathPLINKin, "/genoFusion", chr)
	
	expression.file <- paste0(expressionDir, "/expression", chr, ".txt")
	expression.data <- fread(expression.file)
	
	for(j in 1:nrow(expression.data)){
		#create marker files
		pos <- map.data[j,position]
		markerName <- map.data[j, markerID]
		#message(paste0("Processing ", markerName, "\n"))
		start.region <- ifelse(pos < 500000, 0, pos-500000)
		end.region <- map.data[j,position]+500000
		#Catch errors - PLINK was giving error when there are no SNPs withing the region specified for the marker so no PLINK files were generated for the respective marker
		tryCatch(system(paste0(pathPLINK, " --bfile ", genoFusion.file, " --chr ", chr, " --from-bp ", start.region, " --to-bp ", end.region, " --make-bed --out ", paste0(outPLINKDir, "/", markerName)), ignore.stdout=T, ignore.stderr=T, wait=T), error=function(c){
			c$message <- paste0("ERROR: ", c$message, ": When generating PLINK file for ", markerName, " loop number ", j) 
		})
		
		#replace fam - Phenotype has to be the expression of the marker
		#Check if the PLINK files were generated
		fam.file.path <- paste0(outPLINKDir, "/", markerName, ".fam")
		if(file.exists(fam.file.path)){
			markerExpression <- expression.data[markerID == markerName]
			if(nrow(markerExpression) != 1){
				stop(paste0("ERROR: More than one marker returned for ", markerName))
			}
			markerExpression.frame <- data.frame(t(markerExpression))
			markerExpression.frame$Inds <- rownames(markerExpression.frame)
			rownames(markerExpression.frame) <- NULL
			colnames(markerExpression.frame) <- c("expression", "indID")
			
			fam.file <- fread(fam.file.path)
			fam.frame <- data.frame(fam.file)
			temp <- left_join(fam.frame, markerExpression.frame, by=c("V2" = "indID"))
			
			if(nrow(temp) != nrow(fam.frame)){
				stop(paste0("ERROR:  Different number of individuals in ", paste0(outPLINKDir, "/", markerName, ".fam")))
			}
			write.table(temp[, c(1,2,3,4,5,7)], file=paste0(outPLINKDir, "/", markerName, ".fam"), quote=FALSE, col.names=FALSE, row.names=FALSE, sep="\t")
		
			#write compute_weights command 
			command <- paste0(sourceDir, "/FUSION.compute_weights.R --bfile ", outPLINKDir, "/", markerName, " --tmp TMP", markerName, " --out ", markerName, " --models top1,blup,lasso,enet --PATH_plink ", pathPLINK, " --PATH_gcta ", pathGCTA, " --PATH_gemma ", pathGemma, " --crossval 5 --verbose 2 --save_hsq")
			cat(command, file=commandsFile, sep="\n", append=TRUE) 
			#message(paste0("Processed marker", markerName, "\n"))
		}
	}
	message(paste0("Finished processing markers from chromosome ", chr, "\n"))
}

#'Get cis-region of marker and save in separate PLINK file.  This function is for markers that have a start and end position.
#'@param sourceDir Path to source code
#'@param workingDir Path to working directory
#'@param mapDir Path to map files directory
#'@param pathPLINKin Path to input PLINK directory
#'@param expressionDir Path to expression files directory
#'@param chr Chromosome number
#'@param commandsFile File that has the list of commands to run to compute weights for chromosome <chr>
#'@param pathPLINK Path to PLINK executable
#'@param pathGCTA Path to GCTA executable
#'@param pathGemma Path to Gemma executable
#'@param outPLINKDir Path to directory where to save the marker PLINK files
createMarkerRangePLINK <- function(sourceDir, workingDir, mapDir, pathPLINKin, expressionDir, chr, commandsFile, pathPLINK, pathGCTA, pathGemma, outPLINKDir){
	#load chromosome files
	map.file <- paste0(mapDir, "/map", chr, ".txt")
	map.data <- fread(map.file)
	message("Creating PLINK files for chromosome ", chr, " ...")
	genoFusion.file <- paste0(pathPLINKin, "/genoFusion", chr)
	
	expression.file <- paste0(expressionDir, "/expression", chr, ".txt")
	expression.data <- fread(expression.file)
	
	for(j in 1:nrow(expression.data)){
		#create marker files
		posStart <- map.data[j,posStart]
		posEnd <- map.data[j,posEnd]
		markerName <- map.data[j, markerID]
		#message(paste0("Processing ", markerName, "\n"))
		start.region <- ifelse(posStart < 500000, 0, posStart-500000)
		end.region <- posEnd+500000
		#Catch errors - PLINK was giving error when there are no SNPs withing the region specified for the marker so no PLINK files were generated for the respective marker
		tryCatch(system(paste0(pathPLINK, " --bfile ", genoFusion.file, " --chr ", chr, " --from-bp ", start.region, " --to-bp ", end.region, " --make-bed --out ", paste0(outPLINKDir, "/", markerName)), ignore.stdout=T, ignore.stderr=T, wait=T), error=function(c){
			c$message <- paste0("ERROR: ", c$message, ": When generating PLINK file for ", markerName, " loop number ", j) 
		})
		
		#replace fam - Phenotype has to be the expression of the marker
		#Check if the PLINK files were generated
		fam.file.path <- paste0(outPLINKDir, "/", markerName, ".fam")
		if(file.exists(fam.file.path)){
			markerExpression <- expression.data[markerID == markerName]
			if(nrow(markerExpression) != 1){
				stop(paste0("ERROR: More than one marker returned for ", markerName))
			}
			markerExpression.frame <- data.frame(t(markerExpression))
			markerExpression.frame$Inds <- rownames(markerExpression.frame)
			rownames(markerExpression.frame) <- NULL
			colnames(markerExpression.frame) <- c("expression", "indID")
			
			fam.file <- fread(fam.file.path)
			fam.frame <- data.frame(fam.file)
			temp <- left_join(fam.frame, markerExpression.frame, by=c("V2" = "indID"))
			
			if(nrow(temp) != nrow(fam.frame)){
				stop(paste0("ERROR:  Different number of individuals in ", paste0(outPLINKDir, "/", markerName, ".fam")))
			}
			write.table(temp[, c(1,2,3,4,5,7)], file=paste0(outPLINKDir, "/", markerName, ".fam"), quote=FALSE, col.names=FALSE, row.names=FALSE, sep="\t")
		
			#write compute_weights command 
			command <- paste0(sourceDir, "/FUSION.compute_weights.R --bfile ", outPLINKDir, "/", markerName, " --tmp TMP", markerName, " --out ", markerName, " --models top1,blup,lasso,enet --PATH_plink ", pathPLINK, " --PATH_gcta ", pathGCTA, " --PATH_gemma ", pathGemma, " --crossval 5 --verbose 2 --save_hsq")
			cat(command, file=commandsFile, sep="\n", append=TRUE) 
			#message(paste0("Processed marker", markerName, "\n"))
		}
	}
	message(paste0("Finished processing markers from chromosome ", chr, "\n"))
}

#------------------------------------------------------------------------------------------------------
# MAIN
#------------------------------------------------------------------------------------------------------
#initialise settings
args <- commandArgs(trailingOnly=TRUE)

#load config (using same arguments are ConfigChrScheduler.R)
path.config <- paste0(args[2], "/Config.R")
source(path.config)
plinkFusionPath <- paste0(config$workingDir, "/temp/ChrPLINK") #temp PLINK files for each chromsome
inWeightPLINKpath <- paste0(config$workingDir, "/PLINK") #PLINK files for each marker used as input to FUSION.compute_weights.R
commandsFile <- paste0(config$workingDir, "/temp/tmp_weights/Commands_", as.numeric(args[3]), ".txt") 

#validate.expression.files(config$expressionDir)
extract.common.SNPs(config$ldrefDir, config$genoDir, config$workingDir, as.numeric(args[3]), config$pathPLINK, plinkFusionPath)
if(config$markerPostionSingle == TRUE){
	createMarkerSinglePLINK(args[2], config$workingDir, config$mapDir, plinkFusionPath, config$expressionDir, as.numeric(args[3]), commandsFile, config$pathPLINK, config$pathGCTA, config$pathGemma, inWeightPLINKpath)
}else{
	createMarkerRangePLINK(args[2], config$workingDir, config$mapDir, plinkFusionPath, config$expressionDir, as.numeric(args[3]), commandsFile, config$pathPLINK, config$pathGCTA, config$pathGemma, inWeightPLINKpath)
}
