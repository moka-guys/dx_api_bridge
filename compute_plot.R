#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)
if (length(args)!=2) {
  stop("Usage: compute.plot.R <INPUT.tsv> <OUTPUT.pdf>.n", call.=FALSE)
}
require('tidyverse')
# load data
read_tsv(args[1])->data
# make plot
pdf(args[2],paper="a4")
ggplot(data,aes(executableName,totalPrice)) + geom_boxplot() + coord_flip() + facet_grid(vars(workflowName))
dev.off()
