REGISTER /home/max/Desktop/piggybank-0.17.0.jar;
rawData1 = load '/home/max/Desktop/finaldataset/spain/season-0910_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData2 = load '/home/max/Desktop/finaldataset/spain/season-1011_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData3 = load '/home/max/Desktop/finaldataset/spain/season-1112_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData4 = load '/home/max/Desktop/finaldataset/spain/season-1213_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData5 = load '/home/max/Desktop/finaldataset/spain/season-1314_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData6 = load '/home/max/Desktop/finaldataset/spain/season-1415_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData7 = load '/home/max/Desktop/finaldataset/spain/season-1516_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData8 = load '/home/max/Desktop/finaldataset/spain/season-1617_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData9 = load '/home/max/Desktop/finaldataset/spain/season-1718_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
rawData10 = load '/home/max/Desktop/finaldataset/spain/season-1819_csv.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

refineData1 = FOREACH rawData1 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch1 = GROUP refineData1 BY homeTeam;
awayMatch1 = GROUP refineData1 BY awayTeam;
reHomeMatch1 = FOREACH homeMatch1 GENERATE flatten($1);
reAwayMatch1 = FOREACH awayMatch1 GENERATE flatten($1);
reverseAwayMatch1 = FOREACH reAwayMatch1 GENERATE $1,$0,$3,$2;
fullMatch1 = UNION reHomeMatch1,reverseAwayMatch1;
fullMatch21 = FOREACH fullMatch1 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer1 = GROUP fullMatch21 BY $0;
re1 = FOREACH teamPer1 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result1 = ORDER re1 BY $7 DESC;
finalresult1 = LIMIT result1 1;


refineData2 = FOREACH rawData2 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch2 = GROUP refineData2 BY homeTeam;
awayMatch2 = GROUP refineData2 BY awayTeam;
reHomeMatch2 = FOREACH homeMatch2 GENERATE flatten($1);
reAwayMatch2 = FOREACH awayMatch2 GENERATE flatten($1);
reverseAwayMatch2 = FOREACH reAwayMatch2 GENERATE $1,$0,$3,$2;
fullMatch2 = UNION reHomeMatch2,reverseAwayMatch2;
fullMatch22 = FOREACH fullMatch2 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer2 = GROUP fullMatch22 BY $0;
re2 = FOREACH teamPer2 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result2 = ORDER re2 BY $7 DESC;
finalresult2 = LIMIT result2 1;

refineData3 = FOREACH rawData3 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch3 = GROUP refineData3 BY homeTeam;
awayMatch3 = GROUP refineData3 BY awayTeam;
reHomeMatch3 = FOREACH homeMatch3 GENERATE flatten($1);
reAwayMatch3 = FOREACH awayMatch3 GENERATE flatten($1);
reverseAwayMatch3 = FOREACH reAwayMatch3 GENERATE $1,$0,$3,$2;
fullMatch3 = UNION reHomeMatch3,reverseAwayMatch3;
fullMatch23 = FOREACH fullMatch3 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer3 = GROUP fullMatch23 BY $0;
re3 = FOREACH teamPer3 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result3 = ORDER re3 BY $7 DESC;
finalresult3 = LIMIT result3 1;


refineData4 = FOREACH rawData4 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch4 = GROUP refineData4 BY homeTeam;
awayMatch4 = GROUP refineData4 BY awayTeam;
reHomeMatch4 = FOREACH homeMatch4 GENERATE flatten($1);
reAwayMatch4 = FOREACH awayMatch4 GENERATE flatten($1);
reverseAwayMatch4 = FOREACH reAwayMatch4 GENERATE $1,$0,$3,$2;
fullMatch4 = UNION reHomeMatch4,reverseAwayMatch4;
fullMatch24 = FOREACH fullMatch4 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer4 = GROUP fullMatch24 BY $0;
re4 = FOREACH teamPer4 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result4 = ORDER re1 BY $7 DESC;
finalresult4 = LIMIT result4 1;


refineData5 = FOREACH rawData5 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch5 = GROUP refineData5 BY homeTeam;
awayMatch5 = GROUP refineData5 BY awayTeam;
reHomeMatch5 = FOREACH homeMatch5 GENERATE flatten($1);
reAwayMatch5 = FOREACH awayMatch5 GENERATE flatten($1);
reverseAwayMatch5 = FOREACH reAwayMatch5 GENERATE $1,$0,$3,$2;
fullMatch5 = UNION reHomeMatch5,reverseAwayMatch5;
fullMatch25 = FOREACH fullMatch5 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer5 = GROUP fullMatch25 BY $0;
re5 = FOREACH teamPer5 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result5 = ORDER re5 BY $7 DESC;
finalresult5 = LIMIT result5 1;


refineData6 = FOREACH rawData6 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch6 = GROUP refineData6 BY homeTeam;
awayMatch6 = GROUP refineData6 BY awayTeam;
reHomeMatch6 = FOREACH homeMatch6 GENERATE flatten($1);
reAwayMatch6 = FOREACH awayMatch6 GENERATE flatten($1);
reverseAwayMatch6 = FOREACH reAwayMatch6 GENERATE $1,$0,$3,$2;
fullMatch6 = UNION reHomeMatch6,reverseAwayMatch6;
fullMatch26 = FOREACH fullMatch6 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer6 = GROUP fullMatch26 BY $0;
re6 = FOREACH teamPer6 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result6 = ORDER re6 BY $7 DESC;
finalresult6 = LIMIT result1 1;

refineData7 = FOREACH rawData7 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch7 = GROUP refineData7 BY homeTeam;
awayMatch7 = GROUP refineData7 BY awayTeam;
reHomeMatch7 = FOREACH homeMatch7 GENERATE flatten($1);
reAwayMatch7 = FOREACH awayMatch7 GENERATE flatten($1);
reverseAwayMatch7 = FOREACH reAwayMatch7 GENERATE $1,$0,$3,$2;
fullMatch7 = UNION reHomeMatch7,reverseAwayMatch7;
fullMatch27 = FOREACH fullMatch7 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer7 = GROUP fullMatch27 BY $0;
re7 = FOREACH teamPer7 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result7 = ORDER re7 BY $7 DESC;
finalresult7 = LIMIT result7 1;

refineData8 = FOREACH rawData8 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch8 = GROUP refineData8 BY homeTeam;
awayMatch8 = GROUP refineData8 BY awayTeam;
reHomeMatch8 = FOREACH homeMatch8 GENERATE flatten($1);
reAwayMatch8 = FOREACH awayMatch8 GENERATE flatten($1);
reverseAwayMatch8 = FOREACH reAwayMatch8 GENERATE $1,$0,$3,$2;
fullMatch8 = UNION reHomeMatch8,reverseAwayMatch8;
fullMatch28 = FOREACH fullMatch8 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer8 = GROUP fullMatch28 BY $0;
re8 = FOREACH teamPer8 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result8 = ORDER re8 BY $7 DESC;
finalresult8 = LIMIT result8 1;

refineData9 = FOREACH rawData9 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch9 = GROUP refineData9 BY homeTeam;
awayMatch9 = GROUP refineData9 BY awayTeam;
reHomeMatch9 = FOREACH homeMatch9 GENERATE flatten($1);
reAwayMatch9 = FOREACH awayMatch9 GENERATE flatten($1);
reverseAwayMatch9 = FOREACH reAwayMatch9 GENERATE $1,$0,$3,$2;
fullMatch9 = UNION reHomeMatch9,reverseAwayMatch9;
fullMatch29 = FOREACH fullMatch9 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer9 = GROUP fullMatch29 BY $0;
re9 = FOREACH teamPer9 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result9 = ORDER re9 BY $7 DESC;
finalresult9 = LIMIT result9 1;

refineData10 = FOREACH rawData10 GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;
homeMatch10 = GROUP refineData10 BY homeTeam;
awayMatch10 = GROUP refineData10 BY awayTeam;
reHomeMatch10 = FOREACH homeMatch10 GENERATE flatten($1);
reAwayMatch10 = FOREACH awayMatch10 GENERATE flatten($1);
reverseAwayMatch10 = FOREACH reAwayMatch10 GENERATE $1,$0,$3,$2;
fullMatch10 = UNION reHomeMatch10,reverseAwayMatch10;
fullMatch210 = FOREACH fullMatch10 GENERATE $0,$1,($2>$3 ? 1:0),($2==$3 ? 1:0),($2<$3 ? 1:0),$2,$3,($2-$3),($2>$3 ? 3 : ($2<$3 ? 0:1));
teamPer10 = GROUP fullMatch210 BY $0;
re10 = FOREACH teamPer10 GENERATE $0, SUM($1.$2), SUM($1.$3),SUM($1.$4),SUM($1.$5),SUM($1.$6),SUM($1.$7),SUM($1.$8);
result10 = ORDER re10 BY $7 DESC;
finalresult10 = LIMIT result10 1;

finalresultsum = UNION finalresult1,finalresult2,finalresult3,finalresult4,finalresult5,finalresult6,finalresult7,finalresult8,finalresult9,finalresult10;
refinalresultsum = FOREACH finalresultsum GENERATE $0,1;
gfinalresult = GROUP refinalresultsum BY $0;
finalre = FOREACH gfinalresult GENERATE $0, SUM($1.$1);
DUMP finalre;


