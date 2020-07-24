USE insta_20200417;
USE information_schema;

PRIVILAGES
SELECT * FROM information_schema.USER_PRIVILEGES;
SELECT * FROM information_schema.TABLE_PRIVILEGES;

EXAMPLES

SELECT DISTINCT nick, followed, stories.igid,REGEXP_SUBSTR(real_cta_url, '.*//[^/]*') , DATE(taken_at) as day FROM stories LEFT JOIN influs ON stories.igid = influs.igid where shop1 = 1 OR shop2 = 1 OR shoptypeid = 1 OR presta = 1 OR magento = 1 OR bigcommerce = 1 or opencart = 1;

SELECT DISTINCT nick, followed, stories.igid, REGEXP_SUBSTR(real_cta_url, '.*//[^/]*') AS webproperty, MD5(REGEXP_SUBSTR(real_cta_url, '.*//[^/]*')) AS webproperty_id, DATE(taken_at) as day FROM stories LEFT JOIN influs ON stories.igid = influs.igid WHERE shoptypeid <> 0 LIMIT 10;

PROPAGATIONS

INSERT INTO propagations 
CREATE VIEW v_propagations AS
(SELECT pk, igid, taken_at, shoptypeid, real_cta_url, REGEXP_SUBSTR(real_cta_url, '.*//[^/]*') AS webproperty, MD5(REGEXP_SUBSTR(real_cta_url, '.*//[^/]*')) AS webproperty_id FROM stories WHERE shoptypeid <> 0);
INSERT INTO propagations (SELECT * FROM v_propagations);
SELECT * FROM propagations LIMIT 10;

SELECT DISTINCT nick, followed, propagations.igid, webproperty, DATE(taken_at) as day 
FROM propagations LEFT JOIN influs ON propagations.igid=influs.igid
LIMIT 10
;


----------------------------------------------------


SELECT igid, real_cta_url, REGEXP_SUBSTR(real_cta_url, '.*//[^/]*'), taken_at FROM insta_20200417.stories WHERE shoptypeid <> 0 AND igid = 25257227 LIMIT 10;



SELECT igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM insta_20200417.stories WHERE shoptypeid <> 0 AND igid = 25257227 GROUP BY igid LIMIT 10;

INSERT INTO influs_propagations_count (*) 

SELECT igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM (SELECT igid, taken_at FROM insta_20200417.stories WHERE shoptypeid <> 0 LIMIT 10000) AS t GROUP BY igid ORDER BY c DESC LIMIT 10
);
;

SELECT igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM insta_20200417.stories WHERE shoptypeid <> 0 AND igid IN (25257227, 422442220, 406481723) GROUP BY igid;

INSERT INTO influs_propagations_count (igid,c,earliest,latest,rank) (SELECT igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM (SELECT igid, taken_at FROM insta_20200417.stories WHERE shoptypeid <> 0 LIMIT 1000) AS t GROUP BY igid ORDER BY c DESC);


----------------------------------------------------

CREATE VIEW v_influs_propagations_count AS
(SELECT igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM insta_20200417.stories WHERE shoptypeid <> 0 GROUP BY igid)
;
INSERT INTO influs_propagations_count (SELECT * FROM v_influs_propagations_count);
SELECT * FROM influs_propagations_count LIMIT 10;


CREATE VIEW v_influs_stories_count AS
(SELECT igid, COUNT(*) AS c, MIN(taken_at) AS earliest, MAX(taken_at) AS latest, NULL AS rank FROM insta_20200417.stories GROUP BY igid)
;
INSERT INTO influs_stories_count (SELECT * FROM v_influs_stories_count);
SELECT * FROM influs_stories_count LIMIT 10;



CREATE VIEW v_webproperties AS
(SELECT DISTINCT webproperty_id, webproperty FROM propagations)
;
INSERT INTO webproperties (SELECT * FROM v_webproperties);
SELECT * FROM webproperties LIMIT 10;


----------------------------------------------------


CREATE STATEMENTS

CREATE TABLE `stories` (
  `pk` bigint(20) unsigned NOT NULL,
  `igid` bigint(20) unsigned NOT NULL,
  `taken_at` datetime NOT NULL,
  `expiring_at` datetime NOT NULL,
  `media_type` tinyint(4) NOT NULL,
  `img_w` int(11) DEFAULT NULL,
  `img_h` int(11) DEFAULT NULL,
  `img_url` varchar(1024) DEFAULT NULL,
  `video_w` int(11) DEFAULT NULL,
  `video_h` int(11) DEFAULT NULL,
  `video_url` varchar(1024) DEFAULT NULL,
  `cta_url` varchar(1024) DEFAULT NULL,
  `real_cta_url` varchar(2500) DEFAULT NULL,
  `shoptypeid` tinyint(4) NOT NULL DEFAULT 0,
  `shop1` tinyint(4) DEFAULT NULL,
  `shop2` tinyint(4) DEFAULT NULL,
  `presta` tinyint(4) DEFAULT NULL,
  `magento` tinyint(4) DEFAULT NULL,
  `opencart` tinyint(4) DEFAULT NULL,
  `bigcommerce` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `igid` (`igid`),
  KEY `cta_url` (`cta_url`(768)),
  KEY `shop1` (`shop1`),
  KEY `shoptypeid` (`shoptypeid`),
  KEY `taken_at` (`taken_at`),
  KEY `real_cta_url` (`real_cta_url`(768)),
  KEY `Index 8` (`expiring_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

CREATE TABLE `influs_propagations_count` (

CREATE TABLE `influs_stories_count` (
  `igid` bigint(20) unsigned NOT NULL,
  `c` int(11) DEFAULT NULL,
  `earliest` datetime NOT NULL,
  `latest` datetime NOT NULL,
  `rank` varchar(1) DEFAULT NULL,
  PRIMARY KEY (`igid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `propagations` (
  `pk` bigint(20) unsigned NOT NULL,
  `igid` bigint(20) unsigned NOT NULL,
  `taken_at` datetime NOT NULL,
  `shoptypeid` tinyint(4) NOT NULL,
  `real_cta_url` varchar(2500) DEFAULT NULL,
  `webproperty` varchar(2500) DEFAULT NULL,
  `webproperty_id` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `igid` (`igid`),
  KEY `taken_at` (`taken_at`),
  KEY `webproperty` (`webproperty`),
  KEY `webproperty_id` (`webproperty_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `webproperties` (
  `webproperty_id` varchar(32) NOT NULL,
  `webproperty` varchar(2500) NOT NULL,
  PRIMARY KEY (`webproperty_id`),
  KEY `webproperty` (`webproperty`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;