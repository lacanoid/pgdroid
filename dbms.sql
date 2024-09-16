--
--  DBMS administration functions
--  version 0.0 lacanoid@ljudmila.org
--
---------------------------------------------------

SET client_min_messages = warning;

---------------------------------------------------

-- DROP FUNCTION dbms.notice_ddl();

CREATE TABLE IF NOT EXISTS dbms.ddl_log (
	ts timestamptz NULL,
	"role" text NULL,
	inet inet NULL,
	app text NULL,
	tag text NULL,
	command text NULL,
	"data" jsonb NULL
);

-- atom.audit definition

CREATE TABLE IF NOT EXISTS dbms.dml_log (
	txid int8 DEFAULT txid_current() NULL,
	relid regclass NULL,
	ctime timestamp DEFAULT now() NULL,
  pid integer default pg_backend_pid(),
	usename regrole DEFAULT current_role::regrole NULL,
	pkey jsonb NULL,
	old_data jsonb NULL,
	new_data jsonb NULL,
	seq serial4 NOT NULL
);
CREATE INDEX audit_ctime_idx ON dbms.dml_log USING btree (ctime)
;

---
CREATE OR REPLACE FUNCTION dbms.trigger_ddl()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $function$
declare 
  js json;
  qq text;
begin
	qq := trim(both from current_query());

	if (
	  qq ~* '^refresh\s+materialized\s+view\s+[^;]+;?\s*$'
	  or TG_TAG like 'REFRESH MATERIALIZED VIEW'
	)
	then return; end if;

	with info as (
      select classid::regclass,objid,objsubid,
             object_type,schema_name,object_identity,
             in_extension
        from pg_event_trigger_ddl_commands()
    )
  	select json_agg(row_to_json(info)) 
       from info 
       into js;
      
insert into dbms.ddl_log (ts,role,inet,app,tag,command,data)
values (current_timestamp,
        current_role::regrole,
		inet_client_addr(),
		(select setting from pg_settings where name='application_name'),
		TG_TAG,qq,js
       );
end;
$function$
;
CREATE EVENT TRIGGER ddl_log ON ddl_command_end
	EXECUTE FUNCTION dbms.trigger_ddl();

-------------------------------------------------------

CREATE OR REPLACE FUNCTION dbms.trigger_dml()
 RETURNS trigger
 LANGUAGE plperlu
AS $function$
use JSON;

my $log_table = ${$_TD->{args}}[0];
if(!$log_table) {
  elog(ERROR,'You must provide table name as argument to trigger function');
}
my $regclass=$_TD->{'relid'};
my $new = $_TD->{'new'};
my $old = $_TD->{'old'};
my $new_data = {};
my $old_data = {};

for my $i (keys(%$new)) {
  if("$new->{$i}" ne "$old->{$i}") {
    $new_data->{$i}=defined($new->{$i})?"$new->{$i}":undef; 
    $old_data->{$i}=defined($old->{$i})?"$old->{$i}":undef;
  }
}

if(!%{$new_data}) { elog NOTICE,'SKIP'; return 'SKIP'; }

my @pkey; my $key={};
my $pkey = spi_exec_prepared(
	spi_prepare('select * from unnest(dbms.primary_key($1)) as name','oid'),
	$regclass)->{rows};
for my $i (@{$pkey}) { $key->{$i->{'name'}}=$old->{$i->{'name'}}; }
# elog NOTICE,encode_json($key);

my $new_data = JSON->new->utf8->allow_blessed()->encode($new_data);
my $old_data = JSON->new->utf8->allow_blessed()->encode($old_data);
my $query = qq{INSERT INTO $log_table (relid,pkey,old_data,new_data) }.q{VALUES ($1::oid,$2,$3,$4)};
my $p = spi_prepare($query,'oid','jsonb','jsonb','jsonb');
spi_exec_prepared($p,$regclass,encode_json($key),$old_data,$new_data);

return;
$function$
;
-- DROP FUNCTION dbms.attribute_names(regclass, _int2);

CREATE OR REPLACE FUNCTION dbms.attribute_names(regclass, smallint[])
 RETURNS name[]
 LANGUAGE sql
 STABLE
AS $function$
select array_agg(column_name::name)
from (
 select 
  a.attname as column_name
 from pg_attribute a
 join unnest($2) unnest(i) on (i=a.attnum)
 where a.attrelid=$1
) as n
$function$
;
-- DROP FUNCTION dbms.attribute_types(regclass, _int2);

CREATE OR REPLACE FUNCTION dbms.attribute_types(regclass, smallint[])
 RETURNS text[]
 LANGUAGE sql
 STABLE
AS $function$
select array_agg(column_type::text)
from (
 select 
  a.attname as column_name,
  format_type(a.atttypid,NULL) as column_type
 from pg_attribute a
 join unnest($2) unnest(i) on (i=a.attnum)
 where a.attrelid=$1
) as n
$function$
;


CREATE OR REPLACE VIEW dbms.unique_keys
AS SELECT s.nspname AS table_schema,
    c.relname AS table_name,
    c2.conname AS constraint_name,
        CASE c2.contype
            WHEN 'p'::"char" THEN 'PRIMARY KEY'::text
            WHEN 'u'::"char" THEN 'UNIQUE'::text
            ELSE NULL::text
        END AS constraint_type,
    dbms.attribute_names(c.oid::regclass, c2.conkey) AS attribute_names,
    dbms.attribute_types(c.oid::regclass, c2.conkey) AS attribute_types,
    c.oid AS sysid
   FROM pg_constraint c2
     JOIN pg_class c ON c.oid = c2.conrelid
     JOIN pg_namespace s ON s.oid = c.relnamespace
     JOIN pg_namespace s2 ON s2.oid = c2.connamespace
  WHERE (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", ''::"char"])) AND (c2.contype = ANY (ARRAY['p'::"char", 'u'::"char"]))
;

CREATE OR REPLACE FUNCTION dbms.primary_key(my_class regclass)
 RETURNS name[]
 LANGUAGE sql
AS $function$
select attribute_names 
  from dbms.unique_keys
 where sysid=$1
   and constraint_type='PRIMARY KEY'
 $function$
;
------------------------------------------------------

CREATE DOMAIN dbms."sql_identifier" AS character varying
	COLLATE "default";
COMMENT ON TYPE dbms."sql_identifier" IS 'SQL object identifier';


CREATE OR REPLACE FUNCTION dbms.pg_get_columns(regclass, 
OUT namespace name, OUT class_name name, OUT name name, 
OUT ord smallint, OUT type text, OUT size integer, OUT not_null boolean, 
OUT "default" text, OUT comment text, OUT primary_key name, OUT ndims integer, 
OUT is_local boolean, OUT storage text, OUT sql_identifier dbms.sql_identifier,
 OUT nuls boolean, OUT regclass oid, OUT definition text)
 RETURNS SETOF record
 LANGUAGE sql
AS $function$
 SELECT s.nspname AS namespace, 
        c.relname AS class_name, 
        a.attname AS name, 
        a.attnum AS ord, 
        format_type(t.oid, NULL::integer) AS type, 
        CASE
            WHEN (a.atttypmod - 4) > 0 THEN a.atttypmod - 4
            ELSE NULL::integer
        END AS size, a.attnotnull AS not_null, 
        pg_get_expr(def.adbin,def.adrelid) AS "default", 
        col_description(c.oid, a.attnum::integer) AS comment, 
        con.conname AS primary_key, 
        a.attndims as ndims,
        a.attislocal AS is_local, 
        a.attstorage::text as storage, 
        ((c.oid::regclass)::text || '.' || quote_ident(a.attname))::dbms.sql_identifier AS sql_identifier,
        CASE t.typname
            WHEN 'numeric'::name THEN false
            WHEN 'bool'::name THEN false
            ELSE true
        END AS nuls, 
        c.oid AS regclass, 
        (((quote_ident(a.attname::text) || ' '::text) || format_type(t.oid, NULL::integer)) || 
        CASE
            WHEN (a.atttypmod - 4) > 65536 THEN ((('('::text || (((a.atttypmod - 4) / 65536)::text)) || ','::text) || (((a.atttypmod - 4) % 65536)::text)) || ')'::text
            WHEN (a.atttypmod - 4) > 0 THEN ('('::text || ((a.atttypmod - 4)::text)) || ')'::text
            ELSE ''::text
        END) || 
        CASE
            WHEN a.attnotnull THEN ' NOT NULL'::text
            ELSE ''::text
        END AS definition

   FROM pg_class c
   JOIN pg_namespace s ON s.oid = c.relnamespace
   JOIN pg_attribute a ON c.oid = a.attrelid
   LEFT JOIN pg_attrdef def ON c.oid = def.adrelid AND a.attnum = def.adnum
   LEFT JOIN pg_constraint con ON con.conrelid = c.oid AND (a.attnum = ANY (con.conkey)) AND con.contype = 'p'::"char"
   LEFT JOIN pg_type t ON t.oid = a.atttypid
   JOIN pg_namespace tn ON tn.oid = t.typnamespace
  WHERE (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", ''::"char", 'c'::"char"])) AND a.attnum > 0 
  AND NOT a.attisdropped AND has_table_privilege(c.oid, 'select'::text) AND has_schema_privilege(s.oid, 'usage'::text)
  AND c.oid = $1
  ORDER BY s.nspname, c.relname, a.attnum;
$function$
;

CREATE OR REPLACE VIEW dbms.catalog_usage
AS SELECT tab.sql_identifier,
    pg_relation_size(tab.sql_identifier::regclass) AS pg_relation_size,
    pg_total_relation_size(tab.sql_identifier::regclass) AS pg_total_relation_size,
    pg_size_pretty(pg_total_relation_size(tab.sql_identifier::regclass)) AS pg_size_pretty,
    c.reltuples::bigint AS tuples,
    pg_stat_get_live_tuples(tab.sql_identifier::regclass::oid) AS live,
    pg_stat_get_dead_tuples(tab.sql_identifier::regclass::oid) AS dead
   FROM ( SELECT (quote_ident(tables.table_schema::text) || '.'::text) || quote_ident(tables.table_name::text) AS sql_identifier
           FROM information_schema.tables
          WHERE tables.table_type::text = 'BASE TABLE'::text AND tables.table_schema::text !~~ 'pg_catalog'::text) tab
     JOIN pg_class c ON c.oid = tab.sql_identifier::regclass::oid
  ORDER BY (pg_total_relation_size(tab.sql_identifier::regclass)) DESC
;

--- JSON update stuff

CREATE OR REPLACE FUNCTION dbms.json_insert(my_regclass regclass, my_json text, extend boolean default false)
 RETURNS bigint
 LANGUAGE plperlu
AS $function$
use strict;
use JSON;
my ($regclass,$json,$extend)=@_;

my $obj=JSON->new->allow_nonref->decode($json);

if(!defined($obj)) { elog(ERROR,'Empty JSON'); return undef; } 
if(ref($obj) ne 'ARRAY') { 
   if(ref($obj) ne 'HASH') {
	elog(ERROR,'JSON neither ARRAY nor HASH'); return undef; 
   } else { $obj = [$obj]; }
}

my $n=0;
my %nid;
my %q=(
 'columns'        => spi_prepare('select * from dbms.pg_get_columns($1)','regclass'),
);

my %cols;
if($extend eq 't') {
 my $cols = spi_exec_prepared($q{'columns'},$regclass)->{rows};
 for my $i (@{$cols}) { 
  $cols{$i->{'name'}}=$i; 
  # elog(NOTICE,Dumper($i)); 
 }
}

for my $i (@{$obj}) {
  my @keys;
  my @vals;
  # check if pkey for record is defined
  # make update statement
  for my $j (sort(keys(%{$i}))) {
    if($extend eq 't' && !defined($cols{$j})) {
       $cols{$j}={};
       my $ddl="ALTER TABLE $regclass ADD ".(quote_ident($j))." text";
       elog(NOTICE,$ddl);
       spi_exec_query($ddl);
    }
    push @keys,quote_ident($j);
    push @vals,quote_nullable($i->{$j});
  }
  if(@vals) {
    my $keys = join(', ',@keys);
    my $vals = join(', ',@vals);
    my $sql="INSERT INTO $regclass ($keys) VALUES ($vals)";
    elog(NOTICE,$sql);
    my $rv=spi_exec_query($sql);
    $n++;
  }
}
return $n;

$function$
;

-- DROP FUNCTION dbms.json_save(regclass, text, bool);

CREATE OR REPLACE FUNCTION dbms.json_save(my_regclass regclass, my_json text, extend boolean)
 RETURNS bigint
 LANGUAGE plperlu
AS $function$
use strict;
use JSON;
my ($regclass,$json,$extend)=@_;

my $obj=JSON->new->allow_nonref->decode($json);

if(!defined($obj)) { elog(ERROR,'Empty JSON'); return undef; } 
if(ref($obj) ne 'ARRAY') { 
   if(ref($obj) ne 'HASH') {
	elog(ERROR,'JSON neither ARRAY no HASH'); return undef; 
   } else { $obj = [$obj]; }
}

my $n=0;
my %nid;
my %q=(
 'primary key'    => spi_prepare('select * from unnest(dbms.primary_key($1)) as name','regclass'),
 'columns'        => spi_prepare('select * from dbms.pg_get_columns($1)','regclass'),
);

my %cols;
if($extend eq 't') {
 my $cols = spi_exec_prepared($q{'columns'},$regclass)->{rows};
 for my $i (@{$cols}) { 
  $cols{$i->{'name'}}=$i; 
  # elog(NOTICE,Dumper($i)); 
 }
}
my @pkey;
my $pkey = spi_exec_prepared($q{'primary key'},$regclass)->{rows};
for my $i (@{$pkey}) {
  push @pkey,$i->{'name'};
}
unless(@pkey) {
  elog(ERROR,'no primary key');
}

for my $i (@{$obj}) {
  my @cond;
  my @keys;
  my @vals;
  # check if pkey for record is defined
  for my $j (@pkey) {
    if(!defined($i->{$j})) {
      elog(WARNING,"No primary key in JSON! Skiping record.");
      last;
    } else {
      push @cond,quote_ident($j).'='.quote_nullable($i->{$j});
    }
  }
  # make update statement
  for my $j (sort(keys(%{$i}))) {
    if($extend eq 't' && !defined($cols{$j})) {
       $cols{$j}={};
       my $ddl="ALTER TABLE $regclass ADD ".(quote_ident($j))." text";
       elog(NOTICE,$ddl);
       spi_exec_query($ddl);
    }
    push @keys,quote_ident($j);
    push @vals,quote_nullable($i->{$j});
  }
  if(@cond && @vals) {
    my $keys = join(', ',@keys);
    my $vals = join(', ',@vals);
    my $sql="UPDATE $regclass SET ($keys) = ($vals) WHERE ".join(' AND ',@cond);
    elog(NOTICE,$sql);
    my $rv=spi_exec_query($sql);
    $n+=$rv->{processed};
    if($rv->{processed}==0) {
      my $sql2="INSERT INTO $regclass ($keys) VALUES ($vals)";
      elog(NOTICE,$sql2);
      my $rv=spi_exec_query($sql2);
      $n++;
    }
  }
}
return $n;

$function$
;

-- DROP FUNCTION dbms.json_save2(regclass, text, bool);

CREATE OR REPLACE FUNCTION dbms.json_save2(my_regclass regclass, my_json text, extend boolean default false)
 RETURNS bigint
 LANGUAGE plperlu
AS $function$
use strict;
use JSON;
my ($regclass,$json,$extend)=@_;

my $obj=JSON->new->allow_nonref->decode($json);

if(!defined($obj)) { elog(ERROR,'Empty JSON'); return undef; } 
if(ref($obj) ne 'ARRAY') { 
   if(ref($obj) ne 'HASH') {
	elog(ERROR,'JSON neither ARRAY no HASH'); return undef; 
   } else { $obj = [$obj]; }
}

my $n=0;
my %nid;
my %q=(
 'primary key'    => spi_prepare('select * from unnest(dbms.primary_key($1)) as name','regclass'),
 'unique keys'    => spi_prepare('select * from dbms.unique_keys where sysid=$1','regclass'),
 'columns'        => spi_prepare('select * from dbms.pg_get_columns($1)','regclass'),
);

my %cols;
if($extend eq 't') {
 my $cols = spi_exec_prepared($q{'columns'},$regclass)->{rows};
 for my $i (@{$cols}) { 
  $cols{$i->{'name'}}=$i; 
 }
}
my $pkey;
my @ukey;
my $ukeys = spi_exec_prepared($q{'unique keys'},$regclass)->{rows};
for my $i (@{$ukeys}) {
  if($i->{'constraint_type'} eq 'PRIMARY KEY') { $pkey = [@{$i->{'attribute_names'}}]; } 
  else { push @ukey, [@{$i->{'attribute_names'}}]; }
}
unshift @ukey,$pkey;
unless(@ukey) {
  elog(ERROR,'no primary or unique key');
}

for my $i (@{$obj}) {
  my @cond;
  my @keys;
  my @vals;
  # check if pkey for record is defined
  for my $conf (@ukey) {
    for my $a (@{$conf}) {
      if(!defined($i->{$a})) {
        undef(@cond); last;
      } else {
        push @cond,quote_ident($a).'='.quote_nullable($i->{$a});
      }
    }
    if(@cond) { last; }
  }
  if(!@cond) {
    elog(WARNING,"No unique key in JSON! Skiping record.");
  }
  # make update statement
  for my $j (sort(keys(%{$i}))) {
    if($extend eq 't' && !defined($cols{$j})) {
      # add new columns in needed
       $cols{$j}={};
       my $ddl="ALTER TABLE $regclass ADD ".(quote_ident($j))." text";
       elog(NOTICE,$ddl);
       spi_exec_query($ddl);
    }
    push @keys,quote_ident($j);
    push @vals,quote_nullable($i->{$j});
  }
  if(@cond && @vals) {
    my $keys = join(', ',@keys);
    my $vals = join(', ',@vals);
    my $sql="UPDATE $regclass SET ($keys) = ($vals) WHERE ".join(' AND ',@cond);
    elog(NOTICE,$sql);
    my $rv=spi_exec_query($sql);
    $n+=$rv->{processed};
    if($rv->{processed}==0) {
      my $sql2="INSERT INTO $regclass ($keys) VALUES ($vals)";
      elog(NOTICE,$sql2);
      my $rv=spi_exec_query($sql2);
      $n++;
    }
  }
}
return $n;
$function$
;

-- DROP FUNCTION dbms.json_save3(regclass, text, bool, bool);

CREATE OR REPLACE FUNCTION dbms.json_save3(my_regclass regclass, my_json text, do_insert boolean DEFAULT true, do_extend boolean DEFAULT false)
 RETURNS bigint
 LANGUAGE plperlu
AS $function$
use strict;
use JSON;
my ($regclass,$json,$do_insert,$extend)=@_;

my $obj=JSON->new->allow_nonref->decode($json);

if(!defined($obj)) { elog(ERROR,'Empty JSON'); return undef; } 
if(ref($obj) ne 'ARRAY') { 
   if(ref($obj) ne 'HASH') {
	elog(ERROR,'JSON neither ARRAY no HASH'); return undef; 
   } else { $obj = [$obj]; }
}

my $n=0;
my %nid;
my %q=(
 'primary key'    => spi_prepare('select * from unnest(dbms.primary_key($1)) as name','regclass'),
 'unique keys'    => spi_prepare('select * from dbms.unique_keys where sysid=$1','regclass'),
 'columns'        => spi_prepare('select * from dbms.pg_get_columns($1)','regclass'),
);

my %cols;
if($extend eq 't') {
 my $cols = spi_exec_prepared($q{'columns'},$regclass)->{rows};
 for my $i (@{$cols}) { 
  $cols{$i->{'name'}}=$i; 
  # elog(NOTICE,Dumper($i)); 
 }
}
my $pkey;
my @ukey;
my $ukeys = spi_exec_prepared($q{'unique keys'},$regclass)->{rows};
for my $i (@{$ukeys}) {
  if($i->{'constraint_type'} eq 'PRIMARY KEY') { $pkey = [@{$i->{'attribute_names'}}]; } 
  else { push @ukey, [@{$i->{'attribute_names'}}]; }
}
unshift @ukey,$pkey;
unless(@ukey) {
  elog(ERROR,'no primary or unique key');
}

for my $i (@{$obj}) {
  my @cond;
  my @keys;
  my @vals;
  # check if pkey for record is defined
  for my $conf (@ukey) {
    for my $a (@{$conf}) {
      if(!defined($i->{$a})) {
        undef(@cond); last;
      } else {
        push @cond,quote_ident($a).'='.quote_nullable($i->{$a});
      }
    }
    if(@cond) { last; }
  }
  if(!@cond) {
    elog(WARNING,"No unique key in JSON! Skiping record.");
  }
  # make update statement
  for my $j (sort(keys(%{$i}))) {
    if($extend eq 't' && !defined($cols{$j})) {
      # add new columns in needed
       $cols{$j}={};
       my $ddl="ALTER TABLE $regclass ADD ".(quote_ident($j))." text";
       elog(NOTICE,$ddl);
       spi_exec_query($ddl);
    }
    push @keys,quote_ident($j);
    push @vals,quote_nullable($i->{$j});
  }
  if(@cond && @vals) {
    my $keys = join(', ',@keys);
    my $vals = join(', ',@vals);
    my $sql="UPDATE $regclass SET ($keys) = ($vals) WHERE ".join(' AND ',@cond);
    elog(NOTICE,$sql);
    my $rv=spi_exec_query($sql);
    $n+=$rv->{processed};
    if($rv->{processed}==0) {
	  if($do_insert eq 't') {
        my $sql2="INSERT INTO $regclass ($keys) VALUES ($vals)";
        elog(NOTICE,$sql2);
        my $rv=spi_exec_query($sql2);
        $n++;
      }
    }
  }
}
return $n;

$function$
;

CREATE OR REPLACE FUNCTION dbms.get_proc_info(namespace text, name text, OUT sysid oid, OUT sql_identifier text, OUT argnames text[], OUT argtypes text[], OUT comment text, OUT has_http_acl boolean)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
declare 
 p  record;
 pa record;
 r  record;
begin

 begin
   sysid := regproc(oordbms.sql_identifier(namespace,name))::oid;
   sql_identifier := regproc(sysid)::text;
 exception when others then
   return;
 end;
 
 select * 
   from pg_proc
  where pg_proc.oid = sysid	
   into p;
   
 if p.proargmodes is not null then
  select array_agg(p.proargnames[i]) as iproargnames
  from (
    select i, p.proargmodes[i] as mode
      from generate_series(1,array_length(p.proargmodes,1)) i
     where p.proargmodes[i] in ('i','b')
  ) as pam 
  into pa;
  argnames := pa.iproargnames;
 else
  argnames := p.proargnames;
 end if;

 select array_agg(typ)
 from ( 
   select at::regtype::text as typ
     from unnest(p.proargtypes) as at
 ) as at1 
 into argtypes;

 select exists (
   select a
    from unnest(p.proacl) as a
   where a::text like 'http=%X%/%'
 ) into has_http_acl;

 comment := obj_description(sysid);

 return;
end
$function$
;