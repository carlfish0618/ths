/*** 分组检验 */

/**思路1：利用长短期点击量变化率的波动率 **/

%LET far_day = 40;
%LET near_day = 20;
%LET adjust_start_date = 1may2012;
%LET adjust_end_date = 27mar2015;


DATA busday2;
	SET busday;
	id = _N_;
RUN;

DATA daily_click_data;
	SET product.daily_click_data;
RUN;

/** Step1: 计算相邻点击率的增长率 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*,  B.id, C.date AS pre_date, D.click AS pre_click, D.is_halt AS pre_is_halt
	FROM daily_click_data A LEFT JOIN busday2 B
	ON A.end_date = B.date
	LEFT JOIN busday2 C
	ON B.id - C.id = 1
	LEFT JOIN daily_click_data D
	ON A.stock_code = D.stock_code AND D.end_date = C.date
	ORDER BY A.end_date, A.stock_code, C.date;
QUIT;
DATA daily_click_data;
	SET tmp;
	IF not missing(click) AND not missing(pre_click) AND click~=0 AND pre_click~=0 THEN DO;
		pre_log_dif_click = log(click)-log(pre_click);
	END;
	ELSE pre_log_dif_click = .;
	IF is_halt =0 AND pre_is_halt =0 THEN valid = 1;
	ELSE valid = 0;
RUN;

/** 检查数据：valid = 1但pre_log_dif_click=0的数据 */
/* 2012-9-20缺失数据 */
/** 缺少数据： 001696和001896两只股票 */
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT **/
/*	FROM daily_click_data*/
/*	WHERE valid = 1 AND missing(pre_log_dif_click) AND end_date NOT IN ("20sep2012"d, "21sep2012"d) */
/*	ORDER BY stock_code, end_date;*/
/*QUIT;*/
DATA daily_click_data;
	SET daily_click_data;
	IF stock_code IN ("001696", "001896") THEN delete;
RUN;
DATA subset;
	SET daily_click_data;
	If valid = 1;
RUN;
%plot_normal(pre_log_dif_click,subset);



/***************** Step2: 选取所要的考察的频率的数据 */
/** 确认调仓周期 */
/** 生成adjust_busdate，之后在分析IC时会用到该表 **/

/** type1: 月末调整*/
%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
	rename=end_date, output_table=adjust_busdate, type=1);

/** type2: 日调整 */
/*%get_daily_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., */
/*	rename=end_date, output_table=adjust_busdate);*/

/** type3: 周末调整 */
/*%get_weekday_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., */
/*	rename=end_date, output_table=adjust_busdate, type=2, trade_day =7);*/

PROC SQL;
	CREATE TABLE click_data AS
	SELECT stock_code, end_date
	FROM daily_click_data 
	WHERE end_date IN
	(SELECT end_Date FROM adjust_busdate)
	ORDER BY end_date, stock_code;
QUIT;

/** Step2-1:寻找相邻的日期 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, C.date AS date_b, B.id-C.id AS dif_id, B.id
	FROM click_data A LEFT JOIN busday2 B
	ON A.end_date = B.date
	LEFT JOIN busday2 C
	ON B.id >= C.id AND B.id < C.id + (&far_day.+&near_day.)
	ORDER BY A.end_date, A.stock_code, dif_id;
QUIT;



/** Step2-2: 对应点击率数据 */
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, 
	B.click AS click_b,
	B.pre_log_dif_click AS pre_log_dif_click_b,
	B.valid AS valid_b
	FROM tmp A LEFT JOIN daily_click_data B
	ON A.date_b = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code, A.dif_id desc;
QUIT; 
DATA click_data;
	SET tmp2;
RUN;




/** Step2-3: 计算近期和远期的点击率情况 */
PROC SQL;
	CREATE TABLE month_click_data AS
	SELECT A.*, B.far_valid, B.far_std_log_dif
	FROM
		(SELECT stock_code, end_date, 
			mean(id) AS id,
			count(1) AS near_valid,
			std(pre_log_dif_click_b) AS near_std_log_dif, /* log-difference */
			mean(pre_log_dif_click_b) AS near_mean_log_dif,
			mean(log(click_b)) AS near_mean_log,
			std(log(click_b)) AS near_std_log
		FROM click_data
		WHERE dif_id<&near_day. AND valid_b = 1
		GROUP BY stock_code, end_date
		)A LEFT JOIN
		(SELECT stock_code, end_date,  
			count(1) AS far_valid,
			std(pre_log_dif_click_b) AS far_std_log_dif,
			mean(pre_log_dif_click_b) AS far_mean_log_dif,
			mean(log(click_b)) AS far_mean_log,
			std(log(click_b)) AS far_std_log
		FROM click_data
		WHERE valid_b = 1
		GROUP BY stock_code, end_date) B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** Step2-4: 剔除不满足valid个数不足的 */
%LET pct_valid = 0.8;
DATA month_click_data;
	SET month_click_data;
	valid = 1;
	IF near_valid < floor(&pct_valid.*&near_day.) THEN valid = 0;
	IF far_valid < floor(&pct_valid.*(&near_day.+&far_day.)) THEN valid = 0;
RUN;

/** 统计：每天有效的股票数量 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, sum(valid) AS n_valid, 
		count(1) AS n_stock
	FROM month_click_data
	GROUP BY end_date;
QUIT;

DATA month_click_data;
	SET month_click_data;
	IF valid = 1;
	std_change = near_std_log_dif - far_std_log_dif;
	near_coef = near_mean_log/near_std_log;
RUN;


%plot_normal(near_std_log_dif,month_click_data);
%plot_normal(far_std_log_dif,month_click_data);
%plot_normal(std_change,month_click_data);
%plot_normal(near_mean_log,month_click_data);



/** Step2-5：计算IC */
%LET ic_length = 4; /* 月频率 */

PROC SQL;
	CREATE TABLE raw_table AS
	SELECT end_date, stock_code, close*factor AS price
	FROM hqinfo
	where end_date in 
	(SELECT end_date FROM adjust_busdate)
	ORDER BY end_date, stock_code;
QUIT;
%get_date_windows(raw_table=adjust_busdate, colname=end_date, output_table = adjust_busdate2, start_intval =-&ic_length., end_intval = &ic_length.);
%cal_intval_return(raw_table=raw_table, group_name=stock_code, price_name=price, date_table=adjust_busdate2, output_table=ot2, is_single = 1);

/** 计算IC */
/** (1) 相邻周期变化率 */
%single_factor_ic(factor_table=month_click_data, return_table=ot2, group_name=stock_code, fname=near_std_log_dif, type=3);
%single_factor_ic(factor_table=month_click_data, return_table=ot2, group_name=stock_code, fname=far_std_log_dif, type=3);
%single_factor_ic(factor_table=month_click_data, return_table=ot2, group_name=stock_code, fname=std_change, type=3);
%single_factor_ic(factor_table=month_click_data, return_table=ot2, group_name=stock_code, fname=std_change, type=3);


/********/
%single_factor_ic(factor_table=month_click_data, return_table=ot2, group_name=stock_code, fname=near_coef, type=3);
%single_factor_ic(factor_table=month_click_data, return_table=ot2, group_name=stock_code, fname=near_mean_log, type=3);


/** 20日点击量数据 */
%single_factor_ic(factor_table=month_click_data, return_table=ot2, group_name=stock_code, fname=near_mean_log, type=3);
%single_factor_score(raw_table=month_click_data, identity=stock_code, factor_name=near_mean_log,
		output_table=results, is_increase = 1, group_num = 20);
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=near_mean_log_score,
	ret_column =ret_f1, is_transpose = 1, type=2);

/** 稳健性检验：分组后用得分来进行IC */
%single_factor_ic(factor_table=results, return_table=ot2, group_name=stock_code, fname=near_mean_log_score, type=3);




