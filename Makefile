PG_CONFIG    = pg_config
PKG_CONFIG   = pkg-config

EXTENSION    = dbms
EXT_VERSION  = 0.0
VTESTS       = $(shell bin/tests ${VERSION})

DATA_built   = ${EXTENSION}--$(EXT_VERSION).sql

#REGRESS      = init ${VTESTS}
REGRESS      = init logger 
#REGRESS      = ($shell bin/tests)
REGRESS_OPTS = --inputdir=test

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(DATA_built): $(EXTENSION).sql
	@echo "Building extension version" $(EXT_VERSION) "for Postgres version" $(VERSION)
	VERSION=${VERSION} ./bin/pgsqlpp $^ >$@

testall.sh:
	pg_lsclusters -h | perl -ne '@_=split("\\s+",$$_); print "make PGPORT=$$_[2] PG_CONFIG=/usr/lib/postgresql/$$_[0]/bin/pg_config clean install installcheck\n";' > testall.sh

.PHONY: electric
electric: $(EXTENSION).sql
	@echo "Generating Electric extension SQL for Postgres version" $(VERSION)
	$(eval tmpfile := $(shell mktemp --suffix=.sql))
	$(eval outfile = electric-${EXTENSION}-${VERSION}.sql)
	VERSION=${VERSION} ./bin/pgsqlpp $^ > $(tmpfile)
	elixir ./bin/electric.exs --in ${tmpfile} --out $(outfile)
	@echo "Extension file written to " $(outfile)

