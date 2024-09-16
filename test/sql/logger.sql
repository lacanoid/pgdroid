create table foo (
    id serial primary key,
    label text,
    ctime timestamptz default now()
);

create trigger "$changed" after delete or update on foo 
   for each row execute function dbms.trigger_dml('dbms.dml_log');

insert into foo (label,ctime) values ('Hello world!','2001-01-01 00:00:00');
update foo set label = 'New value';
update foo set label = 'Even more new value';

select relid,usename,pkey,old_data,new_data 
  from dbms.dml_log order by ctime;

select dbms.json_insert('foo', '{"id":-1,"label":"Deuxieme labelle"}', true);
select dbms.json_insert('foo', '{"id":-2,"label":"Troixeme labelle","sublabel":"Ca ira ..."}', true);

select dbms.json_save('foo', '{"id":-3,"label":"Number 4","sublabel":"...."}', true);
select dbms.json_save('foo', '{"id":-3,"label":"Number 5","sublabel":"12345"}', true);

select dbms.json_save2('foo', '{"id":-3,"label":"Number 6","sublabel":"......"}', true);
select dbms.json_save2('foo', '{"id":-3,"label":"Number 7","sublabel":"1234567"}', true);

select dbms.json_insert('foo', '{"label":"Krneki","type":"improved"}',true);
select dbms.json_save2('foo', '{"id":2, "label":"Xrneki","type":"improved+","sublabel":"that"}',true);
#select dbms.json_save2('foo', '{"id":2, "label":"Zrneki","type":"+improved+","sublabel":"that","types":["obi","wan","kenobi"]}',true); --broken

select relid,usename,pkey,old_data,new_data from dbms.dml_log;

