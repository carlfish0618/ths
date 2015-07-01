/*** 指标之间的相关性检验 */


/************************************************** Step1: 总量上：同花顺的点击量指标是否有领先性或者可预测性? **/
/*** 方法：用总量的一阶差分考察Granger Causality ***/

/**方先针对总的数据采用Granger Casuality Test **/
/** 这里的问题之一是：att指标本身有可能是由同花顺的渗透率引致的。*/

/* Step1-1: 求每日的log_dif_att和log_dif_vol */
PROC SQL;
	CREATE TABLE daily_click AS
	SELECT A.date AS end_date, A.stock AS stock_code, A.click, 
	coalesce(B.vol,0) AS vol,
	C.users
	FROM product.raw_data_mdf A LEFT JOIN product.hqinfo B
	ON A.date = B.end_date AND A.stock = B.stock_code
	LEFT JOIN product.self_stock C
	ON A.date = C.end_date AND A.stock = C.stock_code
	ORDER BY A.date, A.stock;
QUIT;
/** 剔除上市未满90天的 */
%LET filter_day = 90;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.list_date,
		B.delist_date, B.is_delist
	FROM daily_click A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code 
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA daily_click(drop = list_date delist_date is_delist);
	SET tmp;
	IF missing(list_date) THEN delete;
	ELSE IF end_date - list_date <= &filter_day. THEN delete;
	ELSE IF is_delist = 1 AND end_date >= delist_date THEN delete;
RUN;

/** 剔除停牌超过60个交易日的股票 */
%LET halt_filter_day = 60;
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.halt_days, B.is_halt
	FROM daily_click A LEFT JOIN market_table B
	ON A.end_date= B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA daily_click(drop = is_halt halt_days);
	SET tmp;
	IF not missing(halt_days) AND halt_days >= &halt_filter_day. THEN delete;
	IF missing(is_halt) THEN delete;
RUN;

/** 统计每日的总量 */
PROC SQL;
	CREATE TABLE daily_analysis AS
	SELECT end_date, sum(click) AS click, sum(vol) AS vol, sum(users) AS users,
		 log(sum(click)) AS log_click,
		 log(sum(vol)) AS log_vol,
		 log(sum(users)) AS log_users
	FROM daily_click
	GROUP BY end_date
	ORDER BY end_date;
QUIT;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, 
	B.click AS pre_click, B.vol AS pre_vol, B.users AS pre_users
	FROM daily_analysis A LEFT JOIN daily_analysis B
	ON A.end_date > B.end_date
	GROUP BY A.end_date
	HAVING B.end_date = max(B.end_date)
	ORDER BY A.end_date;
QUIT;
DATA daily_analysis;
	SET tmp;
	pct_dif = click/pre_click-1;
	pct_dif_vol = vol/pre_vol-1;
	pct_dif_users = users/pre_users-1;
	log_dif = log(click)-log(pre_click);
	log_dif_vol = log(vol)-log(pre_vol);
	log_dif_users = log(users)-log(pre_users);
RUN;
DATA daily_analysis;
	SET daily_analysis;
	/* 第一条记录剔除 */
	IF _N_ = 1 THEN delete; 
RUN;


%test_stationarity(daily_analysis, log_click);
%test_stationarity(daily_analysis, log_dif);
%test_stationarity(daily_analysis, log_users);

%test_stationarity(daily_analysis, log_vol);
%test_stationarity(daily_analysis, log_dif_vol);
%test_stationarity(daily_analysis, log_dif_users);


/** 检验Granger test，只能针对平稳序列 */
/** 结果1：P=0.27,无法拒绝H0。即交易量对点击量没有预测性 */
%granger_test(daily_analysis, log_dif, log_dif_vol);
/** 交易量对加入自选股数量有预测性，但系数不显著 */
%granger_test(daily_analysis, log_dif_users, log_dif_vol);


/** 结果2：P=0.01,无法拒绝H0。即点击量对交易量具有预测性 */
%granger_test(daily_analysis, log_dif_vol, log_dif);
/** 加入自选股数量对交易量有预测性，但系数不显著 */
%granger_test(daily_analysis, log_dif_vol, log_dif_users);



/**** Step1-2: 稳健性检验-检验间隔N天的预测性 **/
/** 没有窗口的重叠性 */
%LET win_length = 5;
PROC SQL;
	CREATE TABLE daily_expand AS
	SELECT end_date, click, vol
	FROM daily_analysis
	GROUP BY end_date
	ORDER BY end_date;
QUIT;
DATA daily_expand(drop = r_group r_count);
	SET daily_expand;
	RETAIN r_group 1;
	RETAIN r_count 1;
	count = r_count;
	s_group = r_group;
	IF r_count >= &win_length. THEN DO;
		r_group + 1;
		r_count = 0;
	END;
	r_count + 1;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT s_group, max(end_date) AS end_date FORMAT yymmdd10., sum(click) AS click, sum(vol) AS vol,
		log(sum(click)) AS log_click,
		 log(sum(vol)) AS log_vol
	FROM daily_expand
	GROUP BY s_group
	ORDER BY s_group;
QUIT;
DATA daily_expand;
	SET tmp;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, 
	B.click AS pre_click, B.vol AS pre_vol
	FROM daily_expand A LEFT JOIN daily_expand B
	ON A.end_date > B.end_date
	GROUP BY A.end_date
	HAVING B.end_date = max(B.end_date)
	ORDER BY A.end_date;
QUIT;
DATA daily_expand;
	SET tmp;
	pct_dif = click/pre_click-1;
	pct_dif_vol = vol/pre_vol-1;
	log_dif = log(click)-log(pre_click);
	log_dif_vol = log(vol)-log(pre_vol);
RUN;
DATA daily_expand;
	SET daily_expand;
	IF _N_ = 1 THEN delete; /* 第一条记录剔除 */
RUN;

/*%test_stationarity(daily_expand, log_click);*/
/*%test_stationarity(daily_expand, log_dif);*/
/*%test_stationarity(daily_expand, log_vol);*/
/*%test_stationarity(daily_expand, log_dif_vol);*/

/** 检验Granger test，只能针对平稳序列 */
/** 结果1：无法拒绝H0。即交易量对点击量没有预测性 */
%granger_test(daily_expand, log_dif, log_dif_vol);

/** 结果2：当win_length>2时，无法拒绝H0。即当窗口拉长时，点击量对交易量没有预测性 */
%granger_test(daily_expand, log_dif_vol, log_dif);

/*** ★ 结论：从总量来看，点击量对交易量的"预测性"只提前了一天。基本属于同步指标 *********************/


