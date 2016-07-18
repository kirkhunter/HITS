
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;


-- Load the data from the input file. Create tables:

create table links (page_id bigint, link array<int>)
	row format delimited
	fields terminated by '\t'
	collection items teriminated by ' '
	stored as textfile
	location 's3n://daataa/links/';

create table titles (page_id bigint, title string)
	row format delimited
	fields terminated by '\t'
	stored as textfile
	location 's3n://daataa/titles/';


-- Table with name hubs and schema: <page id, page title, hub score> 

create table hubs (page_id bigint, page_title string, hub_score float);

-- Table with name auths and schema: <page id, page title, auth score>

create table auths (page_id bigint, page_title string, auth_score float);

-- Table with name out-links and schema <from_page, to_page>

create table out_links (from_page bigint, to_page bigint);



-- Query to remove dead-end links from table out_links,
-- i.e., pairs where to_page has no outlinks.
insert overwrite table out_links 
  select samptable.index from_page, expldd.* to_page
    from (select index, link from links) samptable
            lateral view explode(link) expldd;

insert overwrite out_links
	select a.from_page from_page, a.to_page to_page from 
	  (select from_page, to_page from out_links) a 
	   left join
	  (select b.to_page to_page from
	     (select distinct to_page from out_links) b
	      left join 
	     (select distinct from_page from out_links) c 
	      on (b.to_page = c.from_page) 
	      where c.from_page is not null
	  	) d
	   on (a.to_page = d.to_page)
	   where d.to_page is not null
	   order by from_page, to_page;


-- Count the number of rows in table out_links, N.
select count(1) from out_links;  -- 130108463


-- Update the values of hub score in table hubs, by initializing all page 
-- scores to 1.
insert overwrite table hubs
  select a.page_id page_id, b.title page_title, b.hub_score hub_score from 
	(select page_id from links) a 
	inner join 
	(select page_id, title, 1 hub_score from titles) b
	on (a.page_id = b.page_id);


-- Iteration 1: Compute the hub and authority scores for each page using 
-- equations 2 and 3 below. Then apply equations 4 and 5.

insert overwrite table auths 
  select c.to_page page_id, d.title page_title, c.auth_score auth_score
    from 
      (select a.to_page, sum(b.hub_score) auth_score 
         from 
           (select to_page, from_page from out_links) a   -- sum of all hub scores
            left join                                     -- of pages that point to it
           (select page_id, hub_score from hubs) b 
            on (a.from_page = b.page_id) 
         group by a.to_page) c 
      left join 
     (select page_id, title from titles) d 
      on (c.to_page = d.page_id);

insert overwrite table hubs 
  select c.page_id, d.page_title, c.hub_score
    from 
      (select b.to_page page_id, sum(a.auth_score) hub_score
         from
           (select page_id, auth_score from auths) a 
	        left join
	       (select from_page, to_page from out_links) b
	        on (a.page_id = b.to_page)
	        group by b.to_page) c
	   left join
	  (select page_id, page_title from auths) d 
	   on (c.page_id = d.page_id);

insert overwrite table auths
  select a.page_id page_id, a.page_title page_title, 
    a.auth_score / b.norm auth_score 
    from 
	  (select * from auths) a 
	   left join 
	  (select pow(sum(pow(auth_score), 2), 0.5) norm from auths) b
	   on (1 = 1);

insert overwrite table hubs 
  select a.page_id, a.page_title, a.hub_score / b.norm hub_score 
    from 
      (select * from hubs) a 
       left join 
      (select pow(sum(pow(hub_score, 2)), 0.5) norm from hubs) b 
       on (1 = 1);

-- Display the top 20 pages with the highest auth score; output their title, 
-- their auth score and their hub score.
select * from ( 
	select a.page_id page_id, a.page_title page_title, 
	  a.auth_score auth_score, b.hub_score hub_score
	  from
		(select page_id, page_title, auth_score from auths) a 
		inner join
		(select page_id, hub_score from hubs) b
		on (a.page_id = b.page_id) ) inner_q
		order by auth_score desc limit 20;

-- Display the top 20 pages with the highest hub score; output their title, 
-- their auth score and their hub score.
select * from ( 
	select a.page_id page_id, a.page_title page_title, 
	  a.auth_score auth_score, b.hub_score hub_score
	  from
		(select page_id, page_title, auth_score from auths) a 
		inner join
		(select page_id, hub_score from hubs) b
		on (a.page_id = b.page_id) ) inner_q
		order by hub_score desc limit 20;

-- Repeat the score updating process for 8 times, and report a) and b) after 
-- the end of the 8-th iteration.

-- Repeat the following 8-times:
insert overwrite table auths 
  select inner_q.page_id page_id, c.title page_title, inner_q.auth_score auth_score 
    from 
      (select a.from_page page_id, sum(b.hub_score) auth_score 
         from 
           (select to_page, from_page from out_links) a 
            inner join 
           (select page_id, hub_score from hubs) b 
            on (a.to_page = b.page_id) 
         group by a.page_id) inner_q 
      inner join 
     (select page_id, title from titles) c 
      on (inner_q.page_id = 	c.page_id);

insert overwrite table hubs 
  select inner_q.page_id page_id, c.page_title page_title, 
    inner_q.hub_score hub_score
    from 
      (select b.to_page page_id, sum(a.auth_score) hub_score
         from
           (select page_id, auth_score from auths) a 
	        inner join
	       (select from_page, to_page from out_links) b
	        on (a.page_id = b.to_page)
	        group by b.to_page) inner_q
	   inner join
	  (select page_id, page_title from auths) c 
	   on (inner_q.page_id = c.page_id);

insert overwrite table auths
  select a.page_id page_id, a.page_title page_title, 
    a.auth_score / b.norm auth_score 
    from 
    (select * from auths) a 
     left join 
    (select pow(sum(pow(auth_score, 2)), 0.5) norm from auths) b
     on (1 = 1);

insert overwrite table hubs 
  select a.page_id, a.page_title, a.hub_score / b.norm hub_score 
    from 
      (select * from hubs) a 
       left join 
      (select pow(sum(pow(hub_score, 2)), 0.5) norm from hubs) b 
       on (1 = 1);



