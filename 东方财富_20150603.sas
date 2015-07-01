
**** 东方财富;

**** 创建更新数据程序所在路径全局变量;
%let V_RoutineFunctionPath = H:\备份_20120603\量化\QuantR_new;

%put ****01生成股票列表**********************************************;
%include "&V_RoutineFunctionPath.\01生成股票列表.sas";

%put ****02生成股票交易日表******************************************;
%include "&V_RoutineFunctionPath.\02生成股票交易日表.sas";

proc import out=quant.dfcf_2013_0 datafile="F:\百度云\东方财富\2013关注度.csv" dbms=csv replace;
	getnames=yes;
	datarow=2;
run;
proc import out=quant.dfcf_2014_0 datafile="F:\百度云\东方财富\2014关注度.csv" dbms=csv replace;
	getnames=yes;
	datarow=2;
run;
proc import out=quant.dfcf_2015_0 datafile="F:\百度云\东方财富\2015关注度.csv" dbms=csv replace;
	getnames=yes;
	datarow=2;
run;

proc sql;
	create table quant.dfcf_2013 as
		select substr(stock_code,1,6) as stock_code, stock_name, year(end_date)*10000+month(end_date)*100+day(end_date) as end_date,
			atte, attechg, attechgrate, atterank, attechgrank, attechgraterank
		from quant.dfcf_2013_0
		order by end_date, stock_code;
quit;
proc sql;
	create table quant.dfcf_2014 as
		select substr(stock_code,1,6) as stock_code, stock_name, year(input(end_date, yymmdd10.))*10000+month(input(end_date, yymmdd10.))*100+day(input(end_date, yymmdd10.)) as end_date,
			input(atte, 8.) as atte, input(attechg, 8.) as attechg, input(attechgrate, 8.) as attechgrate,
			input(atterank, 8.) as atterank, input(attechgrank, 8.) as attechgrank, input(attechgraterank, 8.) as attechgraterank
		from quant.dfcf_2014_0
		order by end_date, stock_code;
quit;
proc sql;
	create table quant.dfcf_2015 as
		select substr(stock_code,1,6) as stock_code, stock_name, year(input(end_date, yymmdd10.))*10000+month(input(end_date, yymmdd10.))*100+day(input(end_date, yymmdd10.)) as end_date,
			input(atte, 8.) as atte, input(attechg, 8.) as attechg, input(attechgrate, 8.) as attechgrate,
			input(atterank, 8.) as atterank, input(attechgrank, 8.) as attechgrank, input(attechgraterank, 8.) as attechgraterank
		from quant.dfcf_2015_0
		order by end_date, stock_code;
quit;
data quant.dfcf_0;
	set quant.dfcf_2013 quant.dfcf_2014 quant.dfcf_2015;
run;


proc sql;
	create table dfcf_1 as
		select a.stock_id, b.end_date
		from quant.stock_list a, quant.stock_trademonth_1995 b
		where a.delist=0 and b.end_date>=20130600
		order by b.end_date, a.stock_id;
quit;

%let s1=5;
%let s2=20;
%let l1=10;
%let l2=60;

proc sql;
	create table dfcf_s1 as
		select a.stock_id, a.end_date, b.end_date as end_date2, c.atte
		from dfcf_1 a
			left join quant.stock_tradeday_1995 b0
				on b0.end_date=a.end_date
			left join quant.stock_tradeday_1995 b
				on b.end_date<=a.end_date and b.end_date>b0.prev_&s1.
			left join quant.dfcf_0 c
				on c.end_date=b.end_date and c.stock_code=a.stock_id
		order by a.end_date, a.stock_id, end_date2 desc;
quit;

proc sql;
	create table dfcf_s1 as
		select stock_id, end_date, avg(atte) as atte_s1, count(*) as s1count
		from dfcf_s1
		where atte>0
		group by end_date, stock_id
		order by end_date, stock_id;
quit;

proc sql;
	create table dfcf_s2 as
		select a.stock_id, a.end_date, b.end_date as end_date2, c.atte
		from dfcf_1 a
			left join quant.stock_tradeday_1995 b0
				on b0.end_date=a.end_date
			left join quant.stock_tradeday_1995 b
				on b.end_date<=a.end_date and b.end_date>b0.prev_&s2.
			left join quant.dfcf_0 c
				on c.end_date=b.end_date and c.stock_code=a.stock_id
		order by a.end_date, a.stock_id, end_date2 desc;
quit;

proc sql;
	create table dfcf_s2 as
		select stock_id, end_date, avg(atte) as atte_s2, count(*) as s2count
		from dfcf_s2
		where atte>0
		group by end_date, stock_id
		order by end_date, stock_id;
quit;

proc sql;
	create table dfcf_s_1 as
		select a.stock_id, a.end_date, a.atte_s1, b.atte_s2, a.atte_s1/b.atte_s2-1 as attechg_s
		from dfcf_s1 a
			left join dfcf_s2 b
				on b.stock_id=a.stock_id and b.end_date=a.end_date
		where s1count>=&s1*0.6 and s2count>=&s2*0.6
		order by a.end_date, a.stock_id;
quit;

proc sql;
	create table dfcf_s_2 as
		select a.*, (b2.close*b2.factor)/(b1.close*b1.factor)-1 as ret
		from dfcf_s_1 a
			left join quant.stock_trademonth_1995 d
				on d.prev_1=a.end_date
			left join hq.hqinfo b2
				on b2.stock_code=a.stock_id and b2.d_date=d.end_date and b2.type='A'
			left join hq.hqinfo b1
				on b1.stock_code=a.stock_id and b1.d_date=a.end_date and b1.type='A'
		order by a.end_date, a.stock_id;
quit;

proc corr data = dfcf_s_2 outp = dfcf_s_IC_stat noprint;
	by end_date;
	var attechg_s ret;
run;

proc sql;
	create table dfcf_s_IC_stat as
		select floor(end_date/100) as month, ret as attechg_s_IC
		from dfcf_s_IC_stat
		where _NAME_='attechg_s'
		order by month;
quit;	



proc sql;
	create table dfcf_l1 as
		select a.stock_id, a.end_date, b.end_date as end_date2, c.atte
		from dfcf_1 a
			left join quant.stock_tradeday_1995 b0
				on b0.end_date=a.end_date
			left join quant.stock_tradeday_1995 b
				on b.end_date<=a.end_date and b.end_date>b0.prev_&l1.
			left join quant.dfcf_0 c
				on c.end_date=b.end_date and c.stock_code=a.stock_id
		order by a.end_date, a.stock_id, end_date2 desc;
quit;

proc sql;
	create table dfcf_l1 as
		select stock_id, end_date, avg(atte) as atte_l1, count(*) as l1count
		from dfcf_l1
		where atte>0
		group by end_date, stock_id
		order by end_date, stock_id;
quit;

proc sql;
	create table dfcf_l2 as
		select a.stock_id, a.end_date, b.end_date as end_date2, c.atte
		from dfcf_1 a
			left join quant.stock_tradeday_1995 b0
				on b0.end_date=a.end_date
			left join quant.stock_tradeday_1995 b
				on b.end_date<=a.end_date and b.end_date>b0.prev_&l2.
			left join quant.dfcf_0 c
				on c.end_date=b.end_date and c.stock_code=a.stock_id
		order by a.end_date, a.stock_id, end_date2 desc;
quit;

proc sql;
	create table dfcf_l2 as
		select stock_id, end_date, avg(atte) as atte_l2, count(*) as l2count
		from dfcf_l2
		where atte>0
		group by end_date, stock_id
		order by end_date, stock_id;
quit;

proc sql;
	create table dfcf_l_1 as
		select a.stock_id, a.end_date, a.atte_l1, b.atte_l2, a.atte_l1/b.atte_l2-1 as attechg_l
		from dfcf_l1 a
			left join dfcf_l2 b
				on b.stock_id=a.stock_id and b.end_date=a.end_date
		where l1count>=&l1*0.6 and l2count>=&l2*0.6
		order by a.end_date, a.stock_id;
quit;

proc sql;
	create table dfcf_l_2 as
		select a.*, (b2.close*b2.factor)/(b1.close*b1.factor)-1 as ret
		from dfcf_l_1 a
			left join quant.stock_trademonth_1995 d
				on d.prev_1=a.end_date
			left join hq.hqinfo b2
				on b2.stock_code=a.stock_id and b2.d_date=d.end_date and b2.type='A'
			left join hq.hqinfo b1
				on b1.stock_code=a.stock_id and b1.d_date=a.end_date and b1.type='A'
		order by a.end_date, a.stock_id;
quit;

proc corr data = dfcf_l_2 outp = dfcf_l_IC_stat noprint;
	by end_date;
	var attechg_l ret;
run;

proc sql;
	create table dfcf_l_IC_stat as
		select floor(end_date/100) as month, ret as attechg_l_IC
		from dfcf_l_IC_stat
		where _NAME_='attechg_l'
		order by month;
quit;	
