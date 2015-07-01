/** 同花顺数据整理 */

%LET product_dir = D:\Research\GIT-BACKUP\ths;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 


%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\交易_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";
%INCLUDE "&utils_dir.\组合构建_通用函数.sas";
%INCLUDE "&utils_dir.\其他_通用函数.sas";
%INCLUDE "&utils_dir.\事件研究_通用函数.sas";

options validvarname=any; /* 支持中文变量名 */

%LET env_start_date = 1jan2012;

/****** Step1: 读入数据 **************/
/* Step1-1: 罗列文件名 */
/* use cmd: dir ~\*_d.txt /b/s > ~\file_list.txt */
DATA product.file_list;
	INFILE "&input_dir.\file_list.txt" TRUNCOVER;
	INPUT filepath $100.;
RUN;


/* Step1-2: 汇总每日数据 */
DATA product.raw_data;
	SET product.file_list;
	INFILE dummy FILEVAR = filepath END = done DLM = '09'X TRUNCOVER;
	DO WHILE (not done);
		INPUT date yymmdd8. stock $ click pct $ dod $ ;
		FORMAT date yymmdd10.;
		OUTPUT;
	END;
RUN;

/** 百分比数据 */
DATA product.raw_data_mdf(drop = pct2 dod2);
	SET product.raw_data(rename = (pct = pct2 dod = dod2));
	pct = input(tranwrd(pct2, '%',''),8.);
	dod = input(tranwrd(dod2, '%',''),8.);
RUN;

DATA raw_data;
	SET product.raw_data_mdf;
	log_click = log(click);
RUN;

proc univariate data=raw_data normal; 
    var log_click;
    histogram log_click; 
    probplot log_click;
run; 



/***** Step2: 每日点击量数据描述 ***/
PROC SQL;
	CREATE TABLE product.daily_click AS
	SELECT date, sum(click) AS click
	FROM product.raw_data_mdf
	GROUP BY date
	ORDER BY date;
QUIT;

PROC SGPLOT DATA = product.daily_click;
	HISTOGRAM click;
	DENSITY click;
	TITLE "daily click";
RUN;

proc univariate data=product.daily_click normal; 
    var click;
     histogram click; 
    probplot click;
run; 

/* 取对数 */
DATA product.daily_click;
	SET product.daily_click;
	log_click = log(click);
RUN;

PROC SGPLOT DATA = product.daily_click;
	HISTOGRAM log_click;
	DENSITY log_click;
	TITLE "daily log click";
RUN;

proc univariate data=product.daily_click normal; 
    var log_click;
    histogram log_click; 
    probplot log_click;
run; 


/***** 分析1：每日点击量与当天(前一日)的市场收益率是否相关 */
/** 市场收益率设定为: log(index) - log(前五天average_index) **/
PROC SQL;
	CREATE TABLE sszz AS
	SELECT end_date, stock_code, close
	FROM index_hqinfo
	WHERE stock_code = "000001"
	ORDER BY end_Date;
QUIT;
DATA sszz;
	SET sszz;
	id = _N_;
RUN;
PROC SQL;
	CREATE TABLE sszz_expand AS
	SELECT end_date, close, mean(lag_close) AS lag_close
	FROM 
	(
		SELECT A.*, B.close AS lag_close
		FROM sszz A LEFT JOIN sszz B
		ON 1<= A.id - B.id <=5
	)
	GROUP BY end_date, close
	ORDER BY end_date;
QUIT;
 DATA sszz_expand;
 	SET sszz_expand;
	return = log(close) - log(lag_close);
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.click AS pre_click
	FROM daily_click A LEFT JOIN daily_click B
	ON B.date < A.date
	GROUP BY A.date
	HAVING B.date = max(B.date)
	ORDER BY A.date;
QUIT;

PROC SQL;
	CREATE TABLE daily_click_expand AS
	SELECT A.*,log(A.click)-log(A.pre_click) AS click_change,  B.return
	FROM tmp A LEFT JOIN sszz_expand B
	ON A.date = B.end_date
	ORDER BY A.date;
QUIT;

/** 前一天的收益率 **/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.return AS lag_return
	FROM daily_click_expand A LEFT JOIN daily_click_expand B
	ON A.date > B.date
	GROUP BY A.date
	HAVING B.date = max(B.date)
	ORDER BY A.date;
QUIT;
DATA daily_click_expand;
	SET tmp;
RUN;

PROC SGPLOT DATA = daily_click_expand;
	SERIES x = date y = click_change;
	SERIES x = date y = return/y2axis;
RUN;
/** 收益率接近正态 */
proc univariate data=daily_click_expand normal; 
    var return;
    histogram return; 
    probplot return;
run;
DATA daily_click_expand;
	SET daily_click_expand;
	IF lag_return < -0.02 THEN type = 3;
	ELSE IF lag_return < -0.01 THEN type = 4;
	ELSE IF lag_return < 0 THEN type = 5;
	ELSE IF lag_return = 0 THEN type = 6;
	ELSE IF lag_return < 0.01 THEN type = 7;
	ELSE IF lag_return < 0.02 THEN type = 8;
	ELSE type = 11;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT type, mean(click_change) AS click_change, count(1) AS nobs
	FROM daily_click_expand
	GROUP BY type
	ORDER BY type;
QUIT;


/**** Step3: 定义"abnormal attention" ***/




SYMBOL1 V = '' I = join;
PROC GPLOT DATA = product.daily_click;
	PLOT click * date;
RUN;

/** 1-识别： 总的点击量之间有明显的自相关性 */
PROC ARIMA DATA = product.daily_click;
	IDENTIFY VAR = click(1) nlag = 10;
	RUN;
QUIT;

/****2- 日周期效应是否显著 */
/** 没有明显的日周期效应 **/
DATA daily_click;
	SET product.daily_click;
	wd = weekday(date);
	year = year(date);
	week = week(date);
	month = month(date);
RUN;
PROC SORT DATA = daily_click;
	BY year week;
RUN;
PROC TRANSPOSE DATA = daily_click prefix = pre OUT = daily_click2;
	BY year week;
	ID wd;
	VAR click;
RUN;

PROC SGPLOT DATA = daily_click2(WHERE = (year = 2014));
	SERIES x = week y = pre2;
	SERIES x = week y = pre3;
	SERIES x = week y = pre4;
	SERIES x = week y = pre5;
	SERIES x = week y = pre6;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT year, wd, mean(click)
	FROM daily_click
	GROUP BY year, wd;
QUIT;

/*** 3-识别：总的点击量之间(月度)是否有显著的自相关性 */
PROC SQL;
	CREATE TABLE month_click AS
	SELECT year, month, sum(click) AS click, mdy(month,1,year) AS yearmonth FORMAT yymmdd10.
	FROM daily_click
	GROUP BY year, month;
QUIT;

SYMBOL1 V = '' I = join;
PROC GPLOT DATA = month_click;
	PLOT click*yearmonth;
RUN;
PROC ARIMA DATA = month_click;
	IDENTIFY VAR = click nlag = 6;
	RUN;
QUIT;
