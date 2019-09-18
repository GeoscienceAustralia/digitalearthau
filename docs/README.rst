To build a local copy of the DEA docs, install the programs in
requirements-docs.txt and run 'make fetchnotebooks html'.
After building for the first time, you only need to run 'make html'.
If you use the conda package manager these commands suffice::

  git clone git@github.com:GeoscienceAustralia/digitalearthau.git
  cd digitalearthau/docs
  conda create -c conda-forge -n deadocs --file requirements-docs.txt
  conda activate deadocs
  make fetchnotebooks html
  open _build/html/index.html
