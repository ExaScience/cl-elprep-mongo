(in-package :elprep-mongo)
(bson:in-bson-syntax)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defvar *optimization*
    '(optimize (speed 3) (space 0) (debug 1) (safety 0)
               (compilation-speed 0)))

  (defvar *fixnum-optimization*
    '(optimize (speed 3) (space 0) (debug 1) (safety 0)
               (compilation-speed 0) (hcl:fixnum-safety 0))))

(defstruct sam-mongo
  "representation of the contents of a SAM file in a MongoDB database"
  (host "localhost" :type string :read-only t)
  (port 27017 :type integer :read-only t)
  (socket-properties '() :type list :read-only t)
  (db  "db"  :type string :read-only t)
  (hdr "hdr" :type string :read-only t)
  (aln "aln" :type string :read-only t))

(declaim (inline sam-mongo=))

(defun sam-mongo= (m1 m2)
  "compare two instances of sam-mongo"
  (declare (sam-mongo m1 m2) #.*optimization*)
  (and (string= (sam-mongo-host m1) (sam-mongo-host m2))
       (=       (sam-mongo-port m1) (sam-mongo-port m2))
       (string= (sam-mongo-db   m1) (sam-mongo-db   m2))
       (string= (sam-mongo-hdr  m1) (sam-mongo-hdr  m2))
       (string= (sam-mongo-aln  m1) (sam-mongo-aln  m2))))

(declaim (inline mongo-connect))

(defun mongo-connect (mc)
  "connect to sam-mongo database"
  (apply 'ms:mongo-connect
         :host (sam-mongo-host mc)
         :port (sam-mongo-port mc)
         (sam-mongo-socket-properties mc)))

(declaim (inline sam-alignment-id (setf sam-alignment-id)))

(defun sam-alignment-id (aln)
  "the MongoDB _id is stored in the xtags of an elprep:sam-alignment"
  (declare (sam-alignment aln) #.*optimization*)
  (getf (sam-alignment-xtags aln) :|_id|))

(defun (setf sam-alignment-id) (new-value aln)
  "the MongoDB _id is stored in the xtags of an elprep:sam-alignment"
  (declare (sam-alignment aln) #.*optimization*)
  (setf (getf (sam-alignment-xtags aln) :|_id|) new-value))

(defun write-sam-header (hdr out)
  "write a sam-header to a BSON buffer"
  (declare (sam-header hdr) (bson:buffer out) #.*fixnum-optimization*)
  (when (sam-header-hd hdr)
    (bson:write-embedded-document :@HD (sam-header-hd hdr) out))
  (when (sam-header-sq hdr)
    (bson:with-embedded-array-output (:@SQ out)
      (loop for plist of-type list in (sam-header-sq hdr)
            for index of-type fixnum from 0 do
            (bson:with-embedded-document-output (index out)
              (loop for (key value) of-type (symbol t) on plist by 'cddr
                    if (eq key :M5) do (bson:write-binary #x05 :M5 value out)
                    else do (bson:write-element key value out))))))
  (when (sam-header-rg hdr)
    (bson:with-embedded-array-output (:@RG out)
      (loop for plist of-type list in (sam-header-rg hdr)
            for index of-type fixnum from 0 do
            (bson:with-embedded-document-output (index out)
              (loop for (key value) of-type (symbol t) on plist by 'cddr
                    if (eq key :DT) do (bson:write-datetime :DT value 0 out)
                    else do (bson:write-element key value out))))))
  (when (sam-header-pg hdr)
    (bson:write-embedded-array :@PG (sam-header-pg hdr) out))
  (when (sam-header-co hdr)
    (bson:write-embedded-array :@CO (sam-header-co hdr) out))
  (loop for (key list) of-type (symbol list) on (sam-header-user-tags hdr) by 'cddr
        when list do (bson:write-embedded-array key list out)))

(defun read-sam-header (in)
  "read a sam-header from a BSON buffer"
  (declare (bson:buffer in) #.*optimization*)
  (let ((bson:*array-type* 'list)
        (bson:*key-package* nil)
        (keyword (find-package :keyword))
        (hdr (make-sam-header)))
    (declare (sam-header hdr))
    (bson:with-document-input (in key value)
      (declare (simple-base-string key))
      (let ((bson:*key-package* keyword))
        (setq value (bson:force-value value)))
      (string-case (key :default (setf (sam-header-user-tags hdr)
                                       (list* value (intern key keyword)
                                              (sam-header-user-tags hdr))))
        ("_id")
        ("@HD" (setf (sam-header-hd hdr) value))
        ("@SQ" (loop for plists of-type list on value
                     for M5 = (getf (car plists) :M5)
                     when M5 do (setf (getf (car plists) :M5) (bson:binary-bytes M5))
                     finally (setf (sam-header-sq hdr) value)))
        ("@RG" (loop for plists of-type list on value
                     for DT = (getf (car plists) :DT)
                     when DT do (setf (getf (car plists) :DT) (bson:datetime-seconds DT))
                     finally (setf (sam-header-rg hdr) value)))
        ("@PG" (setf (sam-header-pg hdr) value))
        ("@CO" (setf (sam-header-co hdr) value))))
    (setf (sam-header-user-tags hdr) (nreverse (sam-header-user-tags hdr)))
    hdr))

(defun write-sam-alignment (aln out)
  "write a sam-alignment to a BSON buffer"
  (declare (sam-alignment aln) (bson:buffer out) #.*optimization*)
  (flet ((write-plist (plist)
           (loop for (key value) of-type (symbol t) on plist by 'cddr do
                 (typecase value
                   (character (let ((string (make-string 1 :initial-element value :element-type 'base-char)))
                                (declare (dynamic-extent string))
                                (bson:write-string key string out)))
                   (number    (bson:write-number key value out))
                   (string    (bson:write-string key value out))
                   (vector    (bson:write-binary #x00 key value out))
                   (list      (bson:write-embedded-array key value out))))))
    (write-plist (sam-alignment-xtags aln))
    (bson:write-string :QNAME  (sam-alignment-qname aln) out)
    (bson:write-int32  :FLAG   (sam-alignment-flag  aln) out)
    (bson:write-string :RNAME  (sam-alignment-rname aln) out)
    (bson:write-int32  :POS    (sam-alignment-pos   aln) out)
    (bson:write-int32  :MAPQ   (sam-alignment-mapq  aln) out)
    (bson:write-string :CIGAR  (sam-alignment-cigar aln) out)
    (bson:write-string :RNEXT  (sam-alignment-rnext aln) out)
    (bson:write-int32  :PNEXT  (sam-alignment-pnext aln) out)
    (bson:write-int32  :TLEN   (sam-alignment-tlen  aln) out)
    (bson:write-string :SEQ    (sam-alignment-seq   aln) out)
    (bson:write-string :QUAL   (sam-alignment-qual  aln) out)
    (bson:write-number :_REFID (sam-alignment-refid aln) out)
    (write-plist (sam-alignment-tags aln))))

(defun read-sam-alignment (in)
  "read a sam-alignment from a BSON buffer"
  (declare (bson:buffer in) #.*optimization*)
  (let ((bson:*array-type* 'list)
        (bson:*key-package* nil)
        (keyword (find-package :keyword))
        (aln (make-sam-alignment)))
    (declare (sam-alignment aln))
    (bson:with-document-input (in key value)
      (declare (simple-base-string key))
      (let ((bson:*key-package* keyword))
        (setq value (bson:force-value value)))
      (string-case (key :default (let ((entry (list (if (and (stringp value)
                                                             (= (length value) 1))
                                                      (sbchar value 0)
                                                      value)
                                                    (intern key keyword))))
                                   (if (char= (sbchar key 0) #\_)
                                     (setf (sam-alignment-xtags aln)
                                           (nconc entry (sam-alignment-xtags aln)))
                                     (setf (sam-alignment-tags aln)
                                           (nconc entry (sam-alignment-tags aln))))))
        ("QNAME"  (setf (sam-alignment-qname aln) value))
        ("FLAG"   (setf (sam-alignment-flag  aln) value))
        ("RNAME"  (setf (sam-alignment-rname aln) value))
        ("POS"    (setf (sam-alignment-pos   aln) value))
        ("MAPQ"   (setf (sam-alignment-mapq  aln) value))
        ("CIGAR"  (setf (sam-alignment-cigar aln) value))
        ("RNEXT"  (setf (sam-alignment-rnext aln) value))
        ("PNEXT"  (setf (sam-alignment-pnext aln) value))
        ("TLEN"   (setf (sam-alignment-tlen  aln) value))
        ("SEQ"    (setf (sam-alignment-seq   aln) value))
        ("QUAL"   (setf (sam-alignment-qual  aln) value))
        ("_REFID" (setf (sam-alignment-refid aln) value))))
    (setf (sam-alignment-tags aln) (nreverse (sam-alignment-tags aln)))
    (setf (sam-alignment-xtags aln) (nreverse (sam-alignment-xtags aln)))
    aln))

(defmethod run-pipeline-in-situ ((sam sam-mongo) &key filters (sorting-order :keep) #|(chunk-size +default-chunk-size+)|#)
  "not implemented"
  (declare (ignore filters sorting-order))
  (error "run-pipeline-in-situ not implemented yet for mongo connections."))

(defmethod run-pipeline ((input sam-mongo) (output sam-mongo) &rest args)
  "in-situ operations currently not implemented"
  (declare (dynamic-extent args))
  (if (sam-mongo= input output)
    (apply 'run-pipeline-in-situ input :destructive t args)
    (call-next-method)))

(declaim (inline read-sam-alignment-from-buffer)
         (hcl:special-dynamic *sam-bson-input-buffer*))

(defun read-sam-alignment-from-buffer (aln)
  "turn a binary simple-base-string representation of an alignment into a sam-alignment instance"
  (with-input-from-string (stream aln)
    (let ((buffer *sam-bson-input-buffer*))
      (bson:reinitialize-buffer buffer :stream stream)
      (prog1 (read-sam-alignment buffer)
        (bson:reinitialize-buffer buffer)))))

(declaim (hcl:special-dynamic *mongo-buffer*))

(defmethod run-pipeline ((input sam-mongo) output &rest args &key
                         filters (destructive :default) (sorting-order :keep) (chunk-size +default-chunk-size+) (compression nil compression-p))
  "run-pipeline with input from a sam-mongo database.
   :sorting-order :queryname may query by the MongoDB field QNAME, unless _id is already sorted by queryname.
   :sorting-order :coordinate may query by the MongoDB fields _REFID and POS, unless _id is already sorted by coordinate.
   for queries by QNAME or _REFID/POS, proper indexes are probably necessary."
  (declare (dynamic-extent args))
  (let ((in (mongo-connect input)))
    (unwind-protect
        (let (header original-sorting-order)
          (ms:with-document-stream (in n) (in (sam-mongo-db input) (sam-mongo-hdr input))
            (assert (= n 1))
            (setq header (read-sam-header in))
            (setq original-sorting-order (getf (sam-header-hd header) :so "unknown")))
          (with-thread-filters (thread-filters global-init global-exit) (filters header)
            (setq sorting-order (effective-sorting-order sorting-order header original-sorting-order))
            (when (and (null thread-filters)
                       (typep output 'sam-mongo)
                       (member sorting-order '(:keep :unknown :unsorted)))
              (let ((buffer (mongo-connect output)))
                (unwind-protect
                    (progn
                      (when compression-p
                        (ms:create buffer (sam-mongo-db output) (sam-mongo-aln output) :compression compression))
                      (ms:copydb buffer (sam-mongo-db input) (sam-mongo-db output)
                                 :fromhost (format nil "~A:~A" (sam-mongo-host input) (sam-mongo-port input)))
                      (ms:update buffer (sam-mongo-db output) (sam-mongo-hdr output) #D()
                                 (lambda (buffer) (write-sam-header header buffer)) :flags ms:+upsert+)
                      (mongo-close output))))
              (return-from run-pipeline output))
            (with-output-functions 
                (output-filter wrap-thread receive-chunk)
                (apply 'get-output-functions output header :sorting-order :keep args)
              (global-init)
              (unwind-protect
                  (call-with-processes
                   (max 1 (- *number-of-threads* 2))
                   "MongoDB filter process"
                   (lambda (mailbox)
                     (wrap-thread
                      (lambda ()
                        (with-alignment-filters (aln-filters local-init local-exit) (thread-filters)
                          (if (and (null aln-filters)
                                   (typep output 'sam-mongo)
                                   (member sorting-order '(:keep :unknown :unsorted)))
                            (loop with buffer = *mongo-buffer*
                                  for chunk = (mp:mailbox-read mailbox) until (eq chunk :eop)
                                  for alns = (cdr chunk) do
                                  (loop while alns do
                                        (ms:bulk-insert buffer (sam-mongo-db output) (sam-mongo-aln output)
                                                        (lambda (buffer)
                                                          (loop for index below 64
                                                                for aln = (pop alns) while aln do
                                                                (bson:write-embedded-document index aln buffer)))
                                                        :write-concern #D("w" 0))))
                            (let ((*sam-bson-input-buffer* (bson:make-buffer))
                                  (chunk-filter (create-chunk-filter 'read-sam-alignment-from-buffer aln-filters output-filter t)))
                              (local-init)
                              (unwind-protect
                                  (loop for chunk = (mp:mailbox-read mailbox) until (eq chunk :eop) do
                                        (setf (cdr chunk) (funcall chunk-filter (cdr chunk)))
                                        (receive-chunk chunk))
                                (local-exit))))))))
                   (lambda (mailboxes)
                     (let ((serial -1) (mailbox-ring mailboxes))
                       (ms:with-document-batches (documents)
                           (in (sam-mongo-db input) (sam-mongo-aln input)
                               :flags ms:+exhaust+
                               :query (ecase sorting-order
                                        ((:keep :unknown) #D("$query" () "$orderby" ("_id" 1)))
                                        (:coordinate      #D("$query" () "$orderby" ("_REFID" 1 "POS" 1)))
                                        (:queryname       #D("$query" () "$orderby" ("QNAME" 1)))
                                        (:unsorted        #D()))
                               :return chunk-size
                               :read (bson:read-document-batch 'bson:read-binary-string-document))
                         (mp:mailbox-send (car mailbox-ring) (cons (incf serial) documents))
                         (setq mailbox-ring (or (cdr mailbox-ring) mailboxes))))))
                (global-exit)))))
      (unless (or (not destructive) (eq destructive :default))
        (ms:drop-database in (sam-mongo-db input)))
      (mongo-close in))))

(defmethod get-output-functions ((output sam-mongo) header &key (sorting-order :keep) (chunk-size +default-chunk-size+) (compression nil compression-p))
  "output to a sam-mongo database"
  (ecase sorting-order
    ((:keep :unknown :unsorted)
     (let ((buffer (mongo-connect output)))
       (unwind-protect
           (progn
             (ms:update buffer (sam-mongo-db output) (sam-mongo-hdr output) #D()
                        (lambda (buffer) (write-sam-header header buffer)) :flags ms:+upsert+)
             (when compression-p
               (ms:create buffer (sam-mongo-db output) (sam-mongo-aln output) :compression compression)))
         (mongo-close buffer)))
     (values nil
             (lambda (thunk)
               (let* ((buffer (mongo-connect output))
                      (*mongo-buffer* buffer))
                 (unwind-protect (funcall thunk)
                   (mongo-close buffer))))
             (let ((add-refid (funcall (add-refid header))))
               (lambda (chunk)
                 (loop with buffer = *mongo-buffer*
                       with id = (1- (* (car chunk) chunk-size))
                       with alns = (cdr chunk)
                       while alns do
                       ;; performing the writes directly in the threads is the most efficient solution
                       ;; TODO: make sure we can use one more thread in case of multi-threading
                       ;; requires a protocol in elprep to indicate how many threads are used / needed
                       (ms:insert buffer (sam-mongo-db output) (sam-mongo-aln output)
                                  (lambda (buffer)
                                    (loop repeat 64
                                          for aln = (pop alns) while aln do
                                          (setf (sam-alignment-id aln) (incf id))
                                          (funcall add-refid aln)
                                          (bson:write-document (lambda (buffer) (write-sam-alignment aln buffer)) buffer)))))))
             (lambda () output)))))

(defun ensure-index (sam-mongo sorting-order)
  "create a MongoDB index for a sam-mongo database for the given sorting order (:coordinate or :queryname)"
  (case sorting-order
    ((:coordinate :queryname)
     (let ((out (mongo-connect sam-mongo)))
       (unwind-protect
           (case sorting-order
             (:coordinate (ms:ensure-index out (sam-mongo-db sam-mongo) (sam-mongo-aln sam-mongo)
                                           '("_REFID" 1 "POS" 1) :name "COORDINATE_INDEX"))
             (:queryname  (ms:ensure-index out (sam-mongo-db sam-mongo) (sam-mongo-aln sam-mongo) '("QNAME" 1))))
         (mongo-close out))))))

