# BigBWA
Approaching the Burrows-Wheeler Aligner to Big Data Technologies

# What's BigBWA about? #

**BigBWA** is a tool to run BWA in a Hadooop cluster. **BigBWA** splits the Fastq input reads in pieces and process this pieces in parallel. For now, it supports the following BWA algorithms.

* BWA-MEM paired.
* BWA-ALN paired.
