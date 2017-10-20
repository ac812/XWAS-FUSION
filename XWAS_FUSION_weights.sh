#!/bin/bash

path_to_script=$(dirname $0)
echo $path_to_script

Rscript $path_to_script"/ConfigChrScheduler.R" $1 $path_to_script
