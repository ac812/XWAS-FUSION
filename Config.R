# arg[1] is the path to the config file
# arg[2] is the path to the source code
suppressPackageStartupMessages(library(yaml))

#' Read a .yml file format with error handling.
#' 
#' @param file .yml config file.
#' @param source Source code file where script is running.
read.yml <- function(file, source, ...) {
	tryCatch(suppressWarnings(yaml.load_file(file)), error=function(c){
		c$message <- paste0(c$message, " input file ", file, ", message from ", source)
		stop(c)
	})
}

config <- read.yml(args[1], paste0(args[2], "/Config.R"))


