To build a local copy of the DEA docs, install the programs in
requirements-docs.txt and run 'make html'. If you use the conda package manager
these commands suffice::

  git clone git@github.com:GeoscienceAustralia/digitalearthau.git
  cd digitalearthau/docs
  conda create -n deadocs --file requirements-docs.txt
  conda activate deadocs
  make html
  open build/html/index.html