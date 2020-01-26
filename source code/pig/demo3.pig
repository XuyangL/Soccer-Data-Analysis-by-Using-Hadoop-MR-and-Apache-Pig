REGISTER /home/max/Desktop/piggybank-0.17.0.jar;
rawData = load '/home/max/Desktop/finaldataset/england/season-1819_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
refineData = FOREACH rawData GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch = GROUP refineData BY homeTeam;
awayMatch = GROUP refineData BY awayTeam;
reHomeMatch = FOREACH homeMatch GENERATE flatten($1);
reAwayMatch = FOREACH awayMatch GENERATE flatten($1);
reverseAwayMatch = FOREACH reAwayMatch GENERATE $1,$0,$3,$2;
fullMatch = UNION reHomeMatch,reverseAwayMatch;
fullMatch2 = FOREACH fullMatch GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer = GROUP fullMatch2 BY $0;
re = FOREACH teamPer GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
re2 = ORDER re BY $7 DESC;
DUMP re2;




