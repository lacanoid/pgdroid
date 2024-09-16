create table foo (
    id serial primary key,
    label text unique,
    ctime timestamptz default now(),
    objid int
);
create unique index foo_objid_idx on foo (objid);
create trigger "$changed" after delete or update on foo 
   for each row execute function dbms.trigger_dml('dbms.log_dml');

insert into foo (label,ctime) values ('Hello world!','2001-01-01 00:00:00');
update foo set label = 'New value';
update foo set label = 'Even more new value';

select relid,usename,pkey,old_data,new_data 
  from dbms.log_dml order by ctime;

select dbms.json_save('foo', '{"id":-1,"label":"Deuxieme labelle"}', true, true);
select dbms.json_save('foo', '{"id":-2,"label":"Troixeme labelle","sublabel":"Ca ira ..."}', true, true);

select dbms.json_save('foo', '{"id":-3,"label":"Number 4","sublabel":"...."}', true);
select dbms.json_save('foo', '{"id":-3,"label":"Number 5","sublabel":"12345"}', true);

select dbms.json_save('foo', '{"id":-3,"label":"Number 6","sublabel":"......"}', false, true);
select dbms.json_save('foo', '{"id":-3,"label":"Number 7","sublabel":"1234567"}', false, true);

select dbms.json_save('foo', '{"label":"Krneki","type":"improved"}',true,true);
select dbms.json_save('foo', '{"id":2, "label":"Xrneki","type":"improved+","sublabel":"that"}',false,true);
-- select dbms.json_save('foo', '{"id":2, "label":"Zrneki","type":"+improved+","sublabel":"that","types":["obi","wan","kenobi"]}',true); --broken

select dbms.json_save('foo', '{"label":"json_save3 test"}',true);
select dbms.json_save('foo', '{"id":3,"label":"json_save3 test ++"}',true);
select dbms.json_save('foo', '{"id":3,"label":"json_save3 test +++","subtype":"extra"}',true,true);

select id,label from foo;

select relid,usename,pkey,old_data,new_data from dbms.log_dml;

