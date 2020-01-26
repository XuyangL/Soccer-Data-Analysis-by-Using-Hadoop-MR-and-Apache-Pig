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
rawData = UNION rawData1,rawData2,rawData3,rawData4,rawData5,rawData6,rawData7,rawData8,rawData9,rawData10;
refineData = FOREACH rawData GENERATE $2 AS homeTeam, $3 AS awayTeam, $4 AS homeGoal, $5 AS awayGoal;

homeMatch = Filter refineData BY homeTeam == 'Barcelona';
reverseHomeMatch = FOREACH homeMatch GENERATE $1,$0,$3,$2;
awayMatch = Filter refineData BY awayTeam == 'Barcelona';
fullMatch = Union reverseHomeMatch,awayMatch;
teamPer = GROUP fullMatch BY $0;

reTeamPer = FOREACH teamPer GENERATE flatten($1);
enTeamPer = FOREACH reTeamPer GENERATE $0,($2>$3 ? 0 : ($2<$3 ? 3:1));
gTeamPer = GROUP enTeamPer BY $0;
re = FOREACH gTeamPer GENERATE $0, AVG($1.$1);
result = ORDER re BY $1 ASC;
result2 = LIMIT result 3;
DUMP result2;
