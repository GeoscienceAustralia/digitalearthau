# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = DigitalEarthAustralia
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

fetchnotebooks:
	[ -d notebooks ] || git clone https://github.com/GeoscienceAustralia/dea-notebooks.git notebooks
	cd notebooks && git pull
#	rm -rf notebooks
#	git clone --depth 1 

livehtml:
	sphinx-autobuild -b html -p 8123 --watch . --ignore _build --ignore "*_tmp_*" --ignore "*_old_*" . $(ALLSPHINXOPTS) $(BUILDDIR)/html
