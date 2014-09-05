(let ((elprep:*number-of-threads* 8)
      (mem (elprep:make-sam))
      (sam-mongo (elprep-mongo:make-sam-mongo)))
  (system:set-blocking-gen-num 0 :do-gc nil)
  (format t "Feeding a SAM file into memory while applying some standard filters.~%")
  (elprep:run-pipeline #P"NA12878-chr22.bam" mem
                       :filters (list 'elprep:filter-unmapped-reads
                                      'elprep:add-refid
                                      (elprep:replace-reference-sequence-dictionary-from-sam-file #p"ucsc.hg19.dict")
                                      (elprep:add-or-replace-read-group
                                       (elprep:parse-read-group-from-string "ID:group1 LB:lib1 PL:illumina PU:unit1 SM:sample1"))
                                      'elprep:mark-duplicates)
                       :sorting-order :coordinate)
  (format t "Feeding the in-memory representation of the filtered SAM data into a MongoDB database.~%")
  (elprep:run-pipeline mem sam-mongo)
  (format t "Creating an index in the MongoDB database for coordinate order.~%")
  (elprep-mongo:ensure-index sam-mongo :coordinate)
  (format t "Feeding the MongoDB data into a SAM file in coordinate order.~%")
  (elprep:run-pipeline (elprep-mongo:make-sam-mongo) #p"out-mongo.sam" :sorting-order :coordinate))
