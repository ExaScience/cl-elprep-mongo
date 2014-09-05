(in-package :cl-user)

(defpackage #:elprep-mongo
  (:use #:common-lisp #:elprep #:string-case)
  (:import-from #:lispworks #:sbchar)
  (:import-from #:cl-mongo-stream #:mongo-close)
  (:export
   #:sam-mongo #:make-sam-mongo #:sam-mongo-p
   #:sam-mongo-host
   #:sam-mongo-port
   #:sam-mongo-socket-properties
   #:sam-mongo-db
   #:sam-mongo-hdr
   #:sam-mongo-aln
   #:sam-mongo=

   #:mongo-connect #:mongo-close

   #:sam-alignment-id
   
   #:write-sam-header #:read-sam-header
   #:write-sam-alignment #:read-sam-alignment

   #:ensure-index))
