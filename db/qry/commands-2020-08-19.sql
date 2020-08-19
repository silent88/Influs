USE insta_20200417;

SELECT igid,c, sum(c) over (order by c rows between unbounded preceding and current row)/2013862*100 AS cumsumper FROM influs_propagations_count ORDER BY c DESC LIMIT 10;

SELECT rank, count(*) FROM influs_propagations_count GROUP BY rank;


UPDATE influs_propagations_count Set rank='D' WHERE c<718;
UPDATE influs_propagations_count Set rank='E' WHERE c<408;
UPDATE influs_propagations_count Set rank='F' WHERE c<78;

SELECT rank, count(*), sum(c), sum(c)/2013862*100
FROM influs_propagations_count GROUP BY rank;

SELECT rank, count(*), sum(c), sum(c)/2013862*100
, sum(influs.followed) FROM influs_propagations_count INNER JOIN influs ON
influs.igid=influs_propagations_count.igid GROUP BY rank;

SELECT influs.igid, c, rank, influs.followed FROM
influs_propagations_count INNER JOIN influs ON influs.igid=influs_propagations_count.igid WHERE influs.followed > 500000 AND c >=5 ORDER BY influs.followed DESC LIMIT 20;
4965

INSERT INTO influs_vector SELECT influs.igid FROM influs_propagations_count INNER JOIN influs ON influs.igid=influs_propagations_count.igid WHERE influs.followed > 500000 AND c >=5 ORDER BY influs.followed DESC;

CREATE VIEW v_influs_vector_decorator AS SELECT influs.* FROM influs_vector INNER JOIN influs ON influs_vector.igid=influs.igid;

SELECT * FROM v_influs_vector_decorator WHERE faces=0 ORDER BY following DESC;

CREATE TABLE `influs_vector` (
  `igid` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`igid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `contracts` (
  `igid` bigint(20) unsigned NOT NULL,
  `webproperty_id` varchar(32) NOT NULL,
  PRIMARY KEY (`igid`, `webproperty_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE VIEW v_contracts AS SELECT DISTINCT igid,
webproperty_id FROM propagations;

INSERT INTO influs_propagations_count (SELECT * FROM v_influs_propagations_count);

INSERT INTO webproperties (SELECT * FROM v_webproperties);

INSERT INTO contracts (SELECT * FROM v_contracts);

SHOW CREATE VIEW v_influs_propagations_count;

SELECT webproperties.* FROM influs_vector INNER JOIN contracts ON influs_vector.igid=contracts.igid INNER JOIN webproperties ON contracts.webproperty_id=webproperties.webproperty_id LIMIT 20;

SELECT webproperties.webproperty_id, webproperty, COUNT(*) AS c FROM contracts INNER JOIN webproperties ON contracts.webproperty_id=webproperties.webproperty_id GROUP BY webproperty ORDER BY c DESC LIMIT 20;

CREATE VIEW v_webproperties_contracts_count AS SELECT webproperties.webproperty_id, COUNT(*) AS c, NULL AS rank FROM contracts INNER JOIN webproperties ON contracts.webproperty_id=webproperties.webproperty_id GROUP BY webproperties.webproperty_id ORDER BY c DESC;

INSERT INTO webproperties_contracts_count (SELECT * FROM v_webproperties_contracts_count);

CREATE TABLE `webproperties_contracts_count` (
  `webproperty_id` varchar(32) NOT NULL,
  `c` int(11) DEFAULT NULL,
  `rank` varchar(1) DEFAULT NULL,
PRIMARY KEY (`webproperty_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `webproperties_contracts_exposure` (
  `webproperty_id` varchar(32) NOT NULL,
  `exposure` int(100) DEFAULT NULL,
  `rank` varchar(1) DEFAULT NULL,
PRIMARY KEY (`webproperty_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `webproperties_vector` (
  `webproperty_id` varchar(32) NOT NULL,
PRIMARY KEY (`webproperty_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE VIEW v_influs_contracts_decorator AS
SELECT contracts.webproperty_id, webproperty, contracts.igid, influs.nick, influs.followed FROM contracts INNER JOIN webproperties ON contracts.webproperty_id=webproperties.webproperty_id INNER JOIN influs ON contracts.igid=influs.igid ORDER BY igid;

CREATE VIEW v_webproperties_contracts_exposure AS
SELECT contracts.webproperty_id, SUM(influs.followed) AS exposure, NULL AS rank FROM contracts INNER JOIN webproperties ON contracts.webproperty_id=webproperties.webproperty_id INNER JOIN influs ON contracts.igid=influs.igid GROUP BY contracts.webproperty_id;

INSERT INTO webproperties_contracts_exposure (SELECT * FROM v_webproperties_contracts_exposure);

CREATE VIEW v_influs_stories_count_v2 AS
(SELECT stories.igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM stories INNER JOIN influs_propagations_count ON stories.igid=influs_propagations_count.igid GROUP BY stories.igid)
;
INSERT INTO influs_stories_count (SELECT * FROM v_influs_stories_count_v2);
SELECT * FROM influs_stories_count LIMIT 10;

CREATE VIEW v_influs_stories_count_v3_1 AS
(SELECT stories.igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM stories INNER JOIN influs_propagations_count ON stories.igid=influs_propagations_count.igid 
WHERE influs_propagations_count.rank IN ('A', 'B') 
GROUP BY stories.igid)
;

INSERT INTO influs_stories_count (SELECT * FROM v_influs_stories_count_v3_1);
INSERT INTO influs_stories_count (SELECT * FROM v_influs_stories_count_v3_2);
INSERT INTO influs_stories_count (SELECT * FROM v_influs_stories_count_v3_3);
INSERT INTO influs_stories_count (SELECT * FROM v_influs_stories_count_v4_1);
INSERT INTO influs_stories_count (SELECT * FROM v_influs_stories_count_v4_2);

SELECT * FROM influs_stories_count LIMIT 10;

CREATE VIEW v_influs_stories_count_v3_2 AS
(SELECT stories.igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM stories INNER JOIN influs_propagations_count ON stories.igid=influs_propagations_count.igid
WHERE influs_propagations_count.rank IN ('C', 'D')
GROUP BY stories.igid)
;

CREATE VIEW v_influs_stories_count_v3_3 AS
(SELECT stories.igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM stories INNER JOIN influs_propagations_count ON stories.igid=influs_propagations_count.igid
WHERE influs_propagations_count.rank='E'
GROUP BY stories.igid)
;

CREATE VIEW v_influs_stories_count_v4_1 AS
(SELECT stories.igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM stories INNER JOIN influs_propagations_count ON stories.igid=influs_propagations_count.igid
WHERE influs_propagations_count.rank IN ('C')
GROUP BY stories.igid)
;

CREATE VIEW v_influs_stories_count_v4_2 AS
(SELECT stories.igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM stories INNER JOIN influs_propagations_count ON stories.igid=influs_propagations_count.igid
WHERE influs_propagations_count.rank IN ('D')
GROUP BY stories.igid)
;

CREATE VIEW v_influs_propagation_rate AS
(SELECT influs_propagations_count.igid, influs_propagations_count.c AS propagations_c, influs_stories_count.c AS stories_c, influs_propagations_count.c/influs_stories_count.c*100 AS propagation_rate, influs_propagations_count.rank AS propagations_rank, influs_stories_count.rank AS stories_rank
FROM influs_propagations_count INNER JOIN influs_stories_count ON influs_propagations_count.igid=influs_stories_count.igid
ORDER BY influs_propagations_count.c DESC
) ;

SELECT webproperty_id, exposure, sum(exposure) over (order by exposure rows between unbounded preceding and current row)/69192307635*100 AS cumsumper, rank FROM webproperties_contracts_exposure ORDER BY exposure DESC LIMIT 10;

UPDATE webproperties_contracts_exposure Set rank='A' WHERE exposure>308312759;
UPDATE webproperties_contracts_exposure Set rank='B' WHERE exposure>83063874;
UPDATE webproperties_contracts_exposure Set rank='C' WHERE exposure>16142699;
UPDATE webproperties_contracts_exposure Set rank='D' WHERE exposure<=16142699;
UPDATE webproperties_contracts_exposure Set rank='E' WHERE exposure<6841285;

SELECT webproperty_id, c, sum(c) over (order by c rows between unbounded preceding and current row)/111741*100 AS cumsumper, rank FROM webproperties_contracts_count ORDER BY c DESC LIMIT 10;

UPDATE webproperties_contracts_count Set rank='A' WHERE c>99;
UPDATE webproperties_contracts_count Set rank='B' WHERE c>21;
UPDATE webproperties_contracts_count Set rank='C' WHERE c>4;
UPDATE webproperties_contracts_count Set rank='D' WHERE c=4;
UPDATE webproperties_contracts_count Set rank='E' WHERE c<=3;

INTERPRET RANK
CREATE VIEW v_summary_influs_propagations_count AS
SELECT rank, MIN(c) AS min, MAX(c) AS max, count(*) AS c, SUM(c) AS sum FROM influs_propagations_count GROUP BY rank;
CREATE VIEW v_summary_webproperties_contracts_count AS
SELECT rank, MIN(c) AS min, MAX(c) AS max, count(*) AS c, SUM(c) AS sum FROM webproperties_contracts_count GROUP BY rank;
CREATE VIEW v_summary_webproperties_contracts_exposure AS
SELECT rank, MIN(exposure) AS min, MAX(exposure) AS max, count(*) AS c, SUM(exposure) AS sum FROM webproperties_contracts_exposure GROUP BY rank;



SELECT webproperties.webproperty_id, webproperty, webproperties_contracts_count.c AS influencers_used,  webproperties_contracts_exposure.exposure AS exposure_gained, webproperties_contracts_count.rank AS influencers_used_rank, webproperties_contracts_exposure.rank AS exposure_gained_rank, 
CONCAT(webproperties_contracts_count.rank, webproperties_contracts_exposure.rank) AS havrila_index
FROM webproperties
INNER JOIN webproperties_contracts_count ON
webproperties.webproperty_id=webproperties_contracts_count.webproperty_id INNER JOIN webproperties_contracts_exposure ON webproperties.webproperty_id=webproperties_contracts_exposure.webproperty_id 
WHERE webproperties_contracts_count.rank='C' AND webproperties_contracts_exposure.rank='C'
ORDER BY exposure_gained DESC, influencers_used DESC LIMIT 10;

CREATE VIEW v_webproperties_api AS
SELECT webproperties.webproperty_id, webproperty, webproperties_contracts_count.c AS influencers_used,  webproperties_contracts_exposure.exposure AS exposure_gained, webproperties_contracts_count.rank AS influencers_used_rank, webproperties_contracts_exposure.rank AS exposure_gained_rank, 
CONCAT(webproperties_contracts_count.rank, webproperties_contracts_exposure.rank) AS havrila_index
FROM webproperties
INNER JOIN webproperties_contracts_count ON
webproperties.webproperty_id=webproperties_contracts_count.webproperty_id INNER JOIN webproperties_contracts_exposure ON webproperties.webproperty_id=webproperties_contracts_exposure.webproperty_id 
ORDER BY exposure_gained DESC, influencers_used DESC;

SELECT * FROM v_webproperties_api WHERE influencers_used_rank='C' AND exposure_gained_rank='C';

CREATE VIEW v_webproperties_kpi_2 AS
SELECT v_webproperties_api.*,  REGEXP_REPLACE(
webproperties.webproperty, '^https?://','http://92.107.122.12:5662/?campaign=') AS naked
FROM webproperties 
INNER JOIN v_webproperties_api ON webproperties.webproperty_id=v_webproperties_api.webproperty_id

SELECT DISTINCT nick, followed, stories.igid, REGEXP_SUBSTR(real_cta_url, '.*//[^/]*') AS webproperty, MD5(REGEXP_SUBSTR(real_cta_url, '.*//[^/]*')) AS webproperty_id, DATE(taken_at) as day FROM stories LEFT JOIN influs ON stories.igid = influs.igid WHERE shoptypeid <> 0 LIMIT 10;
;

CREATE TABLE `campaigns` (
`nick` varchar(30) NOT NULL,
`followed` int(11) DEFAULT NULL,
  `igid` bigint(20) unsigned NOT NULL,
  `webproperty` varchar(2500) DEFAULT NULL,
 `day` date NOT NULL,
  KEY `igid` (`igid`),
  KEY `day` (`day`),
  KEY `webproperty` (`webproperty`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

SELECT influs.nick, influs.igid, contracts.webproperty_id, webproperties.webproperty
FROM contracts
INNER JOIN influs ON contracts.igid=influs.igid
INNER JOIN webproperties ON contracts.webproperty_id=webproperties.webproperty_id
WHERE influs.faces=0
--AND influs.igid=2190881642
ORDER BY following DESC
;

SELECT nick, igid, c
FROM
(
    SELECT influs.nick, influs.igid, COUNT(*) AS c
    FROM contracts
    INNER JOIN influs ON contracts.igid=influs.igid
    WHERE influs.faces=0
    GROUP BY influs.igid
    ORDER BY following DESC
) t
WHERE c=1
;


SELECT influs.nick, influs.igid, contracts.webproperty_id, webproperties.webproperty
FROM contracts
INNER JOIN influs ON contracts.igid=influs.igid
INNER JOIN webproperties ON contracts.webproperty_id=webproperties.webproperty_id
INNER JOIN
    (
        SELECT igid
        FROM
        (
            SELECT contracts.igid, COUNT(*) AS c
            FROM contracts
            INNER JOIN influs ON contracts.igid=influs.igid
            WHERE influs.faces=0
            GROUP BY influs.igid
        ) t
        WHERE c=1
    ) t2
    ON t2.igid=contracts.igid
;

CREATE VIEW v_influs_own_store_v2 AS (
SELECT influs.igid, webproperties.webproperty_id
FROM contracts
INNER JOIN influs ON contracts.igid=influs.igid
INNER JOIN webproperties ON contracts.webproperty_id=webproperties.webproperty_id
INNER JOIN
    (
        SELECT igid
        FROM
        (
            SELECT contracts.igid, COUNT(*) AS c
            FROM contracts
            INNER JOIN influs ON contracts.igid=influs.igid
            WHERE influs.faces=0
            GROUP BY influs.igid
        ) t
        WHERE c=1
    ) t2
    ON t2.igid=contracts.igid
);

CREATE TABLE `influs_own_store` (
  `igid` bigint(20) unsigned NOT NULL,
  `webproperty_id` varchar(32) NOT NULL,
  PRIMARY KEY (`igid`, `webproperty_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

INSERT INTO influs_own_store (SELECT * FROM v_influs_own_store);


CREATE TABLE `influs_vector_faceless` (
  `igid` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`igid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE VIEW v_influs_vector_faceless_v2 AS
SELECT DISTINCT influs.igid
FROM influs
INNER JOIN contracts ON contracts.igid=influs.igid
WHERE influs.faces=0
ORDER BY following DESC
;

INSERT INTO influs_vector_faceless (SELECT * FROM v_influs_vector_faceless);
