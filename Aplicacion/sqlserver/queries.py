DROP_TABLES=('DROP TABLE IF EXISTS temporal;')

TEMPORAL_CREATION=('CREATE TABLE temporal('
'country_name varchar(250), '
'country_code varchar(20), '
'"1990" FLOAT, '
'"2000" FLOAT, '
'"2001" FLOAT, '
'"2002" FLOAT, '
'"2003" FLOAT, '
'"2004" FLOAT, '
'"2005" FLOAT, '
'"2006" FLOAT, '
'"2007" FLOAT, '
'"2008" FLOAT, '
'"2009" FLOAT, '
'"2010" FLOAT, '
'"2011" FLOAT, '
'"2012" FLOAT, '
'"2013" FLOAT, '
'"2014" FLOAT, '
'"2015" FLOAT, '
'"2016" FLOAT, '
'"2017" FLOAT, '
'"2018" FLOAT, '
'"2019" FLOAT, '
'"2020" FLOAT, '
'"2021" FLOAT);')



COLLATE=('ALTER TABLE temporal CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci')
GENERO=('CREATE TABLE genre(id int NOT NULL AUTO_INCREMENT, name varchar(50), PRIMARY KEY(id))')
ARTISTA=('CREATE TABLE artist(id int NOT NULL AUTO_INCREMENT, name varchar(100),PRIMARY KEY(id))')
CANCION=('CREATE TABLE song(id int NOT NULL AUTO_INCREMENT, name VARCHAR(500), duration varchar(8),year int,explicit varchar(50), reproductions int,PRIMARY KEY(id))')
CANCION_GENERO=('CREATE TABLE song_genre(id_song int, id_genre int, FOREIGN KEY (id_song) REFERENCES song(id), FOREIGN KEY (id_genre) REFERENCES genre(id), PRIMARY KEY song_genre (id_song, id_genre))')
CANCION_ARTISTA=('CREATE TABLE song_artist(id_song int, id_artist int, FOREIGN KEY (id_song) REFERENCES song(id), FOREIGN KEY (id_artist) REFERENCES artist(id), PRIMARY KEY song_artist(id_song,id_artist))')

COLLATE_SONG=('ALTER TABLE song CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci')
LLENAR_GENERO=('insert into genre (name) select distinct genre from temporal order by 1;')
LLENAR_ARTISTA=('insert into artist (name) select distinct artist from temporal order by artist asc;')
LLENAR_CANCION=('insert into song (name,duration,year,explicit,reproductions) select song,duration_ms,year,explicit,count(song) from temporal GROUP BY song,duration_ms,year,explicit;')
OBTENER_CANCIONES=('select artist,song,duration_ms,explicit,year,genre,count(song) from temporal group by artist,song,duration_ms,explicit,year,genre')

TOP10_ARTISTAS_REPRODUCCIONES=('select a.name,count(sa.id_artist) as reproducciones ' 
'from artist a ' 
	'inner join song_artist sa on sa.id_artist =a.id ' 
'group by a.name ' 
'order by 2 DESC '
'Limit 10')

CANCIONES_MAS_REPRODUCIDAS=('select name,duration,year,explicit,reproductions from song s order by reproductions DESC limit 10')
TOP5_GENEROS_MAS_REPRODUCIDOS=('select g.name, count(sg.id_genre) as reproducciones '
	'from song_genre sg '
	'inner join genre g on sg.id_genre = g.id '
'GROUP by g.name '
'order by 2 DESC '
'limit 5')

ARTISTA_MAS_REPRODUCIDO_GENERO=('select g.name,a.name,count(a.name) '
	'from song_genre sg '
	'inner join genre g on sg.id_genre = g.id '
	'inner join song_artist sa on sa.id_song = sg.id_song '
	'inner join artist a on sa.id_artist = sa.id_artist '
'group by g.name,a.name '
'order by 3 DESC LIMIT 10 ')

CANCION_REPRODUCIDA_GENERO=('select g.name,s.name,a.name,count(s.name) as reproducciones '
	'from song_genre sg '
	'inner join genre g on sg.id_genre = g.id '
	'inner join song s on s.id = sg.id_song '
	'inner join song_artist sa on sa.id_song = s.id '
	'inner join artist a on sa.id_artist = a.id '
'group by g.name,s.name,a.name '
'order by 4 DESC LIMIT 10')

CANCION_REPRODUCIDA_ANIO_LANZAMIENTO=('select a.name as artist,s.name,s.year, reproductions  '
	'from song s '
		'inner join song_artist sa on s.id =sa.id_song '
		'inner join artist a on a.id = sa.id_artist '
'order by 4 DESC LIMIT 10')

ARTISTAS_POPULARES=('select a.name, count(sa.id_song) '
'from artist a '
	'inner join song_artist sa on sa.id_artist = a.id '
'group by a.name '
'order by 2 desc '
'limit 10 ')

CANCIONES_POPULARES=('select s.name, s.reproductions '
	'from song s '
'order by 2 desc '
'limit 10')

TOP_5_GENEROS_POPULARES=('select g.name, count(sg.id_genre) as popularidad  '
	'from genre g '
	'inner join song_genre sg on sg.id_genre = g.id '
'group by g.name '
'order by 2 desc '
'limit 5')

CANCIONES_MAS_REPRODUCIDA_EXPLICITO=('select s.name,g.name as genero, count(sg.id_song) as reproducciones '
	'from song s '
	'inner join song_genre sg on sg.id_song = s.id '
	'inner join genre g on g.id = sg.id_genre '
'where s.explicit =1 '
'group by s.name, g.name '
'order by 3 desc '
'limit 1')
