(asdf:defsystem #:elprep-mongo
  :version "1.0"
  :author "Charlotte Herzeel (Imec), Pascal Costanza (Intel Corporation)"
  :licence
  "Copyright (c) 2014, Imec and Intel Corporation. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
 notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
 notice, this list of conditions and the following disclaimer in the
 documentation and/or other materials provided with the distribution.
* Neither the name of Imec or Intel Corporation nor the names of its
 contributors may be used to endorse or promote products derived from
 this software without specific prior written permission."
  :components
  ((:file "elprep-mongo-package")
   (:file "elprep-mongo" :depends-on ("elprep-mongo-package")))
  :depends-on ("bson" "cl-mongo-stream" "elprep" "string-case"))
