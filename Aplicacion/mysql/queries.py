DROP_TABLES=('DROP TABLE IF EXISTS temporal;')

TEMPORAL_CREATION=('CREATE TABLE temporal('
'country_name varchar(250), '
'country_code varchar(20), '
'`1990` FLOAT, '
'`2000` FLOAT, '
'`2001` FLOAT, '
'`2002` FLOAT, '
'`2003` FLOAT, '
'`2004` FLOAT, '
'`2005` FLOAT, '
'`2006` FLOAT, '
'`2007` FLOAT, '
'`2008` FLOAT, '
'`2009` FLOAT, '
'`2010` FLOAT, '
'`2011` FLOAT, '
'`2012` FLOAT, '
'`2013` FLOAT, '
'`2014` FLOAT, '
'`2015` FLOAT, '
'`2016` FLOAT, '
'`2017` FLOAT, '
'`2018` FLOAT, '
'`2019` FLOAT, '
'`2020` FLOAT, '
'`2021` FLOAT);')



COLLATE=('ALTER TABLE temporal CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci')
