# Overview

elPrep is a high-performance tool for preparing .sam/.bam files for variant calling in sequencing pipelines. It can be used as a drop-in replacement for SAMtools/Picard, and was extensively tested with the GATK best practices pipeline for variant analysis.

elPrep-mongo is an extension of elPrep that allows for using MongoDB databases as input and/or output targets in elPrep, on top of the in-memory and file representations that elPrep already supports.

For more details on elPrep, please visit the [elPrep github repository](https://github.com/ExaScience/cl-elprep).

A rudimentary example for the use of elPrep-mongo is provided in example.lisp. Please use the input data from the elPrep demo with this example.

A specification for the representation of SAM files in MongoDB databases is also provided as part of this repository.

## Dependencies

The elPrep-mongo implementation depends on [elPrep](https://github.com/ExaScience/cl-elprep) and [cl-mongo-stream](https://github.com/ExaScience/cl-mongo-stream), which have further dependencies that you may need to check. The elPrep-mongo implementation also depends on the string-case library which is available through the [quicklisp](http://www.quicklisp.org) package manager.
