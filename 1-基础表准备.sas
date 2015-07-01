/*** 基础表准备 **/



/*** 因子检验 **/
%LET product_dir = D:\Research\GIT-BACKUP\ths;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 

%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

/** 东方财富数据(已到本地) */
%LET dfcf_dir = D:\Research\GIT-BACKUP\dfcf;
LIBNAME quant "&dfcf_dir.\sasdata";


%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\交易_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";
%INCLUDE "&utils_dir.\组合构建_通用函数.sas";
%INCLUDE "&utils_dir.\其他_通用函数.sas";
%INCLUDE "&utils_dir.\事件研究_通用函数.sas";
%INCLUDE "&utils_dir.\因子有效性_通用函数.sas";
%INCLUDE "&utils_dir.\计量_通用函数.sas";

options validvarname=any; /* 支持中文变量名 */

%LET pfolio_env_start_date = 15dec2011;

/******************************* 基础表*******/
PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT end_date, stock_code, close, pre_close, factor
	FROM product.hqinfo
	ORDER BY end_date, stock_code;
QUIT;

/** 2- 交易日 **/
PROC SQL;
	CREATE TABLE busday AS
	SELECT distinct end_date AS date
	FROM hqinfo
	ORDER BY end_date;
QUIT;

/*DATA stock_info_table(drop = F17_1090 F18_1090);*/
/*	SET product.stock_info_table;*/
/*	list_date = input(F17_1090,yymmdd8.);*/
/*	delist_date = input(F18_1090,yymmdd8.);*/
/*	IF index(stock_name,'ST') THEN is_st = 1;*/
/*	ELSE is_st = 0;*/
/*	FORMAT list_date delist_date mmddyy10.;*/
/*RUN;*/

DATA stock_info_table;
	SET product.stock_info_table;
RUN;


PROC SQL;
	CREATE TABLE market_table AS
	SELECT A.*, B.is_st, 0 AS is_halt, 0 AS is_limit
	FROM product.hqinfo A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.stock_code, A.end_date;
QUIT;

PROC SQL;
	UPDATE market_table 
		SET is_halt = 1 WHERE missing(vol) OR vol = 0 OR (not missing (vol) AND vol ~= 0 AND istrade = 0);  /* 是否停牌*/
	UPDATE market_table    /* 涨跌停标志 */
		SET is_limit = CASE 
							WHEN close = high AND close = low AND close = open AND close > pre_close THEN 1
							WHEN close = high AND close = low AND close = open AND close < pre_close THEN 2
			 				WHEN (close >= round(pre_close * 1.15,0.01) AND is_st = 0 )  /* 放宽：1.1 -> 1.15 */
							OR ( close >= round(pre_close * 1.09,0.01) AND is_st = 1) THEN 3   /* 放宽：1.05 -> 1.09 */
							WHEN (close <= round(pre_close * 0.85,0.01) AND is_st = 0 )  /* 放宽：0.9 -> 0.85 */
							OR ( close <= round(pre_close * 0.91,0.01) AND is_st = 1) THEN 4  /* 放宽：0.95 -> 0.91*/
							ELSE 0
						END;
QUIT;



/* 是否复牌 */
DATA  market_table(keep = end_date stock_code is_halt is_limit is_resumption);
	SET  market_table;
	BY stock_code;
	last_is_halt = lag(is_halt);
	IF first.stock_code THEN last_is_halt = .;
	IF last_is_halt = 1 AND is_halt = 0 THEN is_resumption = 1;
	ELSE is_resumption = 0;
RUN;

/** 对于停牌日期，寻找最近的非停牌日期(最远365天) **/
PROC SQL;
	CREATE TABLE market_data_append AS
	SELECT A.stock_code, A.end_date, B.end_date AS last_no_halt_date
	FROM 
	(
	SELECT stock_code, end_date
	FROM market_table
	WHERE is_halt = 1
	) A LEFT JOIN
	(SELECT stock_code, end_date
	FROM market_table
	WHERE is_halt = 0 )B
	ON B.end_date + 365 >=  A.end_date > B.end_date AND A.stock_code = B.stock_code
	GROUP BY A.stock_code, A.end_date
	HAVING B.end_date = max(B.end_date)
	ORDER BY A.end_date, A.stock_code;
QUIT;

%map_date_to_index(busday_table=busday, raw_table=market_data_append, date_col_name=end_date, raw_table_edit=market_data_append2);
DATA market_data_append2;
	SET market_data_append2(rename = (date_index = end_date_index));
RUN;
%map_date_to_index(busday_table=busday, raw_table=market_data_append2, date_col_name=last_no_halt_date, raw_table_edit=market_data_append2);
DATA market_data_append2(drop = end_date_index date_index last_no_halt_date);
	SET market_data_append2;
	IF missing(last_no_halt_date) THEN halt_days = 1000000;
	ELSE halt_days = end_date_index - date_index;
RUN;
PROC SORT DATA = market_table;
	BY end_date stock_code;
RUN;
PROC SORT DATA = market_data_append2;
	BY end_date stock_code;
RUN;
DATA market_table;
	UPDATE market_table market_data_append2;
	BY end_date stock_code;
RUN;
DATA market_table;
	SET market_table;
	IF is_halt = 0 THEN halt_days = .;
RUN;
PROC SQL;
	DROP TABLE market_data_append, market_data_append2;
QUIT;

/*** 自由流通市值表 */
PROC SQL;
	CREATe TABLE fg_wind_freeshare AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.,
		freeshare, total_share, a_share, liqa_share
	FROM product.fg_wind_freeshare
	ORDER BY end_date, stock_code;
QUIT;

/** 行业信息表 */
PROC SQL;
	CREATe TABLE fg_wind_sector AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.,
		o_code AS indus_code, o_name AS indus_name
	FROM product.fg_wind_sector
	ORDER BY end_date, stock_code;
QUIT;
