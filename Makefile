#
# Copyright (c) 2015, Joyent, Inc. All rights reserved.
#
# Makefile: top-level Makefile
#
# This Makefile contains only repo-specific logic and uses included makefiles
# to supply common targets (javascriptlint, jsstyle, restdown, etc.), which are
# used by other repos as well.
#

#
# Tools
#
# The "manpages" target is not run as part of a normal build, so both the man
# page sources and generated man pages are committed into this repo.  This is
# suboptimal, but we've gone this route because the tool is not always easily
# provided in development environments.  To install the tool, see
# https://github.com/sunaku/md2man.
#
MD2MAN		 = md2man-roff
NPM		 = npm

#
# Files
#
JS_FILES	:= bin/manta-hk \
    $(shell find lib -name '*.js' \
        -not -path 'lib/job-assets/*/node_modules/*')
JSL_FILES_NODE	 = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSL_CONF_NODE	 = tools/jsl.node.conf

MAN_ROOT	 = docs/man
MAN_PAGES	 = $(notdir $(wildcard $(MAN_ROOT)/*.md))
MAN_OUTDIR	 = man/man1
MAN_OUTPAGES	 = $(MAN_PAGES:%.md=$(MAN_OUTDIR)/%.1)

.PHONY: all
all:
	$(NPM) install

.PHONY: manpages
manpages: $(MAN_OUTPAGES)

$(MAN_OUTPAGES): $(MAN_OUTDIR)/%.1: $(MAN_ROOT)/%.md | $(MAN_OUTDIR)
	$(MD2MAN) $^ > $@

$(MAN_OUTDIR):
	mkdir -p $@

include ./Makefile.targ
