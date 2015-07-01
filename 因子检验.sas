
/******************* PART1: 计算abnormal attention*********************************/
/***定义abnormal attention: 最近5天的关注度/最近20日(不含这5天)的关注度 */
%LET far_day = 20;
%LET near_day = 5;
%LET adjust_start_date = 1may2012;
%LET adjust_end_date = 27mar2015;


DATA busday2;
	SET busday;
	id = _N_;
RUN;


/***************** Step1: 选取所要的考察的频率的数据 */
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
	FROM product.daily_click_data 
	WHERE end_date IN
	(SELECT end_Date FROM adjust_busdate)
	ORDER BY end_date, stock_code;
QUIT;


/** Step2:寻找相邻的日期 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, C.date AS date_b, B.id-C.id AS dif_id, B.id
	FROM click_data A LEFT JOIN busday2 B
	ON A.end_date = B.date
	LEFT JOIN busday2 C
	ON B.id >= C.id AND B.id < C.id + (&far_day.+&near_day.)
	ORDER BY A.end_date, A.stock_code, dif_id;
QUIT;

/** Step3: 对应各项数据 */
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, 
	coalesce(C.click,0) AS click_b,
	coalesce(C.vol,0) AS vol_b,
	coalesce(C.report,0) AS report_b,
	coalesce(C.atte,0) AS atte_b,
	is_halt AS is_halt_b,
	is_resumption AS is_resumption AS is_resumption_b
	FROM tmp A
	LEFT JOIN product.daily_click_data C
	ON A.date_b = C.end_date AND A.stock_code = C.stock_code
	ORDER BY A.end_date, A.stock_code, dif_id;
QUIT; 
DATA click_data;
	SET tmp2;
RUN;



/** Step4: 计算近期和远期的点击率情况 */
PROC SQL;
	CREATE TABLE month_click_data AS
	SELECT stock_code, end_date, mean(id) AS id,
		/** 点击率有效数量 */
		sum(click_b~=0 AND dif_id < &near_day.) AS near_count,
		sum(click_b~=0 AND &near_day. <= dif_id ) AS far_count,
		/** 停牌天数 */
		sum(is_halt_b*(dif_id<&near_day.)) AS near_halt,
		sum(is_halt_b*(dif_id>=&near_day.)) AS far_halt,
		sum(is_resumption_b*(dif_id<&near_day.)) AS near_resump,
		sum(is_resumption_b*(dif_id>=&near_day.)) AS far_resump,
		/** 点击率 */
		sum(click_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_click,
		sum(click_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_click,
		coalesce(sum(click_b*(dif_id<&near_day.))/sum(click_b~=0 AND dif_id < &near_day.),0) AS near_click_filter, /* 有可能为缺失*/
		coalesce(sum(click_b*(dif_id>=&near_day.))/sum(click_b~=0 AND dif_id >= &near_day.),0) AS far_click_filter,
		/** 成交量 */
		sum(vol_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_vol,
		sum(vol_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_vol,
		coalesce(sum(vol_b*(dif_id<&near_day.))/sum(vol_b~=0 AND dif_id < &near_day.),0) AS near_vol_filter, /* 有可能为缺失*/
		coalesce(sum(vol_b*(dif_id>=&near_day.))/sum(vol_b~=0 AND dif_id >= &near_day.),0) AS far_vol_filter,
		/** 券商报告*/
		sum(report_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_report,
		sum(report_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_report,
		sum(report_b*(dif_id<&far_day.)) AS cur_report,
		/** 关注度 */
		sum(atte_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_dfcf,
		sum(atte_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_dfcf,
		coalesce(sum(atte_b*(dif_id<&near_day.))/sum(atte_b~=0 AND dif_id < &near_day.),0) AS near_dfcf_filter, /* 有可能为缺失*/
		coalesce(sum(atte_b*(dif_id>=&near_day.))/sum(atte_b~=0 AND dif_id >= &near_day.),0) AS far_dfcf_filter
	FROM click_data
	GROUP BY stock_code, end_date
	ORDER BY end_date, stock_code;
QUIT;

/** Step5-1: 用于看动量和反转效应 */
/** 获取一个月长度的日期数据 */

/** 月频数据，对应过去1个月 */
/** 周频数据,对应过去4周 */
/** 日频数据，对应过去20个交易日 */
%LET intval = 4;  /* 可以是1,4或者20 */
%get_date_windows(raw_table=adjust_busdate, colname=end_date, output_table = adjust_busdate2, start_intval =-&intval., end_intval = 0);

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, 
	coalesce(B.close*B.factor, 0) AS price,
	coalesce(D.close*D.factor,0) AS price_b1
	FROM month_click_data A LEFT JOIN product.hqinfo B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	LEFT JOIN adjust_busdate2 C
	ON A.end_date = C.end_date
	LEFT JOIN product.hqinfo D
	ON C.date_b&intval. = D.end_date AND A.stock_code = D.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT; 

DATA month_click_data(drop = price_b1 price);
	SET tmp;
	IF not missing(price_b1) AND not missing(price) AND price_b1~=0 AND price ~=0 THEN DO; /* 价格为0，认为是异常 */
		ret = (price-price_b1)/price_b1*100;
	END;
	ELSE ret = .;
RUN;


/*** Step6: 计算abnormal attention **/
/** 要求near_day和far_day至少80%以上的数据 */
%LET valid_pct = 0.8;
DATA month_click_data(drop = i);
	SET month_click_data;
	IF near_count >= floor(&near_day.*&valid_pct.) AND far_count >= floor(&far_day.*&valid_pct.) THEN valid = 1;
	ELSE valid = 0;

	ARRAY raw_var(6) near_click near_vol  near_dfcf
					near_click_filter near_vol_filter near_dfcf_filter;
	ARRAY raw_var_b(6) far_click far_vol far_dfcf 
					far_click_filter far_vol_filter far_dfcf_filter;
	ARRAY log_var(6) log_dif_click log_dif_vol log_dif_dfcf
					log_dif_click_filter log_dif_vol_filter log_dif_dfcf_filter;
	ARRAY pct_var(6) pct_dif_click pct_dif_vol  pct_dif_dfcf
					pct_dif_click_filter pct_dif_vol_filter pct_dif_dfcf_filter;
	DO i = 1 TO 6;
		IF raw_var(i) ~= 0 AND raw_var_b(i) ~= 0 AND not missing(raw_var(i)) AND not missing(raw_var_b(i)) THEN DO;
			log_var(i) = log(raw_var(i)) - log(raw_var_b(i));
		END;
		ELSE DO;
			log_var(i) = .;
		END;
		IF raw_var_b(i) ~= 0 AND not missing(raw_var(i)) AND not missing(raw_var_b(i)) THEN DO;
			pct_var(i) = raw_var(i)/raw_var_b(i)-1;
		END;
		ELSE DO;
			pct_var(i) = .;
		END;
	END;
	/** 券商报告直接用差分 */
	abs_dif_report = near_report - far_report;
RUN; 

/** 计算相邻两个周期的变化*/
/** 如果是日频，周期就是日频 */
%get_date_windows(raw_table=adjust_busdate, colname=end_date, output_table = adjust_busdate2, start_intval =-1, end_intval = 0);

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.end_date, A.stock_code,
		A.click, A.vol,
		A.report, A.atte
	FROM product.daily_click_data A
	ORDER BY A.end_date, A.stock_code;
QUIT;


PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, 
	E.click AS click,
	E.vol AS vol,
	E.report AS report,
	E.atte AS atte,
	D.click AS click_b,
	D.vol AS vol_b,
	D.report AS report_b,
	D.atte AS atte_b
	FROM month_click_data A LEFT JOIN adjust_busdate2 C
	ON A.end_date = C.end_date
	LEFT JOIN tmp D
	ON C.date_b1 = D.end_date AND A.stock_code = D.stock_code
	LEFT JOIN tmp E
	ON A.end_date = E.end_date AND A.stock_code = E.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA month_click_data(drop = i click_b vol_b report_b atte_b);
	SET tmp2;
	ARRAY raw_var(3) click vol atte;
	ARRAY raw_var_b(3) click_b vol_b  atte_b;
	ARRAY log_var(3) pre_log_dif_click pre_log_dif_vol pre_log_dif_atte;
	ARRAY pct_var(3) pre_pct_dif_click pre_pct_dif_vol pre_pct_dif_atte;
	DO i = 1 TO 3;
		IF raw_var(i) ~= 0 AND raw_var_b(i) ~= 0 AND not missing(raw_var(i)) AND not missing(raw_var_b(i)) THEN DO;
			log_var(i) = log(raw_var(i)) - log(raw_var_b(i));
		END;
		ELSE DO;
			log_var(i) = .;
		END;
		IF raw_var_b(i) ~= 0 AND not missing(raw_var(i)) AND not missing(raw_var_b(i)) THEN DO;
			pct_var(i) = raw_var(i)/raw_var_b(i)-1;
		END;
		ELSE DO;
			pct_var(i) = .;
		END;
	END;
	IF not missing(report) AND not missing(report_b) THEN pre_abs_dif_report = report - report_b;
	ELSE pre_abs_dif_report = .;
RUN; 


/*** Step7: 分析数据特征 **/
/** 1: 有效性数据统计 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, count(distinct stock_code) AS n_stock,
		sum(valid) AS n_valid, sum(1-valid) AS n_invalid
	FROM month_click_data
	GROUP BY end_date;
QUIT;

/** 2. 统计近期停牌情况 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, 
		count(distinct stock_code) AS n_stock,
		sum(near_halt=0 AND far_halt=0 AND far_resump = 0 AND near_resump = 0) AS n_clear,
		sum(near_halt=1) AS near_halt1,
		sum(near_halt=2) AS near_halt2,
		sum(near_halt=3) AS near_halt3,
		sum(near_halt=4) AS near_halt4,
		sum(near_halt=5) AS near_halt5,
		sum(near_resump>0) AS near_resump,
		sum(far_halt>=1 AND far_halt<=5) AS far_halt5,
		sum(far_halt >5 AND far_halt <=10) AS far_halt10,
		sum(far_halt > 10 AND far_halt <=15) AS far_halt15,
		sum(far_halt > 15) AS far_halt20,
		sum(far_resump>0) AS far_resump
	FROM month_click_data
	GROUP BY end_date;
QUIT;



/******************************************* PART II: 分析因子有效性，仅针对clear_data ****************************/


/** Step0: 取clear_data进行分析 ***/
/*** 之后可以考虑进一步放松其他条件 **/
DATA month_click_data;
	SET month_click_data;
	IF near_halt=0 AND far_halt=0 AND far_resump = 0 AND near_resump = 0 
		AND near_count >= floor(&near_day.*&valid_pct.) AND far_count >= floor(&far_day.*&valid_pct.)THEN is_clear = 1;
	ELSE is_clear = 0;
RUN;

/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT end_date, count(1) AS n_stock,*/
/*	sum(is_clear) AS n_clear*/
/*	FROM month_click_data*/
/*	GROUP BY end_date;*/
/*QUIT;*/

/** Step1: 剔除无效股票后，分析log_dif和pct_dif的分布 */
DATA subdata;
	SET month_click_data;
	IF is_clear = 0 THEN delete;
RUN;

 
%plot_normal(log_dif_click,subdata);
%plot_normal(pct_dif_click,subdata);
%plot_normal(log_dif_vol,subdata);
%plot_normal(pct_dif_vol,subdata);
%plot_normal(log_dif_dfcf,subdata);
%plot_normal(pct_dif_dfcf,subdata);
%plot_normal(abs_dif_report,subdata);

%plot_normal(pre_log_click,subdata);
%plot_normal(cur_report,subdata);
%plot_normal(ret,subdata);


/** Step2：计算点击率因子IC值 ***/
/** 用到上文的adjust_busdate表格 */
/** 根据频率选择不同的ic_length。*/
/**
(1) 周频可以考虑未来8周。
(2) 月频考虑未来6个月
(3) 日频可以考虑未来20天
**/
%LET ic_length = 8;

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
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=pre_log_dif_click, type=3);
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=pre_log_dif_vol, type=3);
/* 稳健性测试 */
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=pre_pct_dif_click, type=2);
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=pre_pct_dif_vol, type=2);

/** (2)异常点击率 */
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=log_dif_click, type=3);
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=log_dif_vol, type=3);
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=pct_dif_click, type=3);
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=pct_dif_vol, type=3);

%single_factor_score(raw_table=subdata, identity=stock_code, factor_name=near_mean_log,
		output_table=results, is_increase = 1, group_num = 20);
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=near_mean_log_score,
	ret_column =ret_f1, is_transpose = 1, type=2);

%LET fname = log_dif_click;
%LET ret_column = ret_b1;
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
%single_factor_score(raw_table=subdata, identity=stock_code, factor_name=&fname.,
		output_table=results, is_increase = 1, group_num = 20);
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =&ret_column., is_transpose = 1, type=2);


/** !!结论：点击率和成交量的IC很相似。采用log-dif或者pct-dif略微有差异，但不明显*/
/** 查看二者的相关性(可以发现：高度相关，均值在0.86。最低值为0.78，最高值能达到0.93) */
/** 有两种解释：
(1) 点击行为本身包含了交易需求。所以和成交量放大是有显著关系的。解决方法：识别出那些非成交需求的点击。
(2) 成交量和点击量都是关注度的表现形式，二者相关性较大
***/
%cal_coef(subdata,log_dif_click, log_dif_vol);

/** 用IC相似的方法。为了统一，采用pct-difference（因为cal_intval_return中采用的就是pct-difference的方法) **/
/** vol-difference自身的相关性，以及click与不同期vol之间的相关性 */
PROC SQL;
	CREATE TABLE vol_table AS
	SELECT end_date, stock_code, vol
	FROM subdata
	ORDER BY end_date;
QUIT;
%cal_intval_return(raw_table=vol_table, group_name=stock_code, price_name=vol, date_table=adjust_busdate2, output_table=vol2, is_single = 1);
%single_factor_ic(factor_table=subdata, return_table=vol2, group_name=stock_code, fname=pre_pct_dif_vol, type=3);
%single_factor_ic(factor_table=subdata, return_table=vol2, group_name=stock_code, fname=pre_pct_dif_click, type=3);

PROC SQL;
	CREATE TABLE click_table AS
	SELECT end_date, stock_code, click
	FROM month_click_data
	ORDER BY end_date;
QUIT;
%cal_intval_return(raw_table=click_table, group_name=stock_code, price_name=click, date_table=adjust_busdate2, output_table=click2, is_single = 1);
%single_factor_ic(factor_table=subdata, return_table=click2, group_name=stock_code, fname=pre_pct_dif_click, type=3);
%single_factor_ic(factor_table=subdata, return_table=click2, group_name=stock_code, fname=pre_pct_dif_vol, type=3);



/*** Step3：因为大部分的交易都由散户主导，因此可以认为点击量和成交量都是散户关注度的间接指标。
寻找机构关注度的指标，目前暂时以券商研报数量作为机构关注的指标 **/
/** 机构关注度变化(abs_dif_report)与点击率之间相关性很弱，且前者的ic均值接近为0*/

/*%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=log_dif_report);*/
/*%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=pct_dif_report);*/
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=abs_dif_report);
%cal_coef(subdata,log_dif, abs_dif_report);



/*** Step4：分组对比 ***/
/** 4-1: 考虑用机构关注度变化对股票进行分组 **/
DATA subdata2;
	SET subdata;
	IF abs_dif_report > 0.2; /* 切换不同条件，对比分组结果*/
RUN;
%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=log_dif);
/** 结论： 分组后，各组之间差异性不大，效果并不好 **/

/** 4-2: 考虑用机构关注的绝对数量进行分组 */
/** cur_report分为0,1,2及以上 */
DATA subdata2;
	SET subdata;
	IF cur_report > 1; /* 切换不同条件，对比分组结果*/
RUN;
%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=log_dif);
/** 结论：尽管0组的IC最为显著，1组的最后微弱，但整体来看区分度没有非常明显 **/

/** 4-3：考虑动量效应（过去20天的绝对收益高低分组）*/
/** 检验相关性:0.4-0.5之间 */
%cal_coef(subdata,log_dif, ret);

/* 单纯的收益率因子更接近为反转效应 */
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=ret);
/** 用过去一个月的收益率进行分组 */
DATA subdata2;
	SET subdata;
	IF ret < -10; /* 切换不同条件，对比分组结果*/
RUN;
%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=log_dif);




/** Step5: 和东方财富数据对比 */
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=log_dif_dfcf);
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=pct_dif_dfcf_filter);

%cal_coef(subdata,log_dif, log_dif_dfcf);
%cal_coef(subdata,log_dif_filter, log_dif_dfcf_filter);






 /************************************* 切换思路 ************************************/
/** Step1: 将关注度区分为：交易需求和查询需求。
区分方法：交易需求中无法被成交量解释的部分，认为是查询需求 */
PROC REG DATA = subdata;
	MODEL log_dif = log_dif_vol;
	BY end_date;
	OUTPUT OUT = results(keep = end_date stock_code log_dif log_dif_vol r)
		rstudent = r;
	RUN;
QUIT;
%single_factor_ic(factor_table=results, return_table=ot2, group_name=stock_code, fname=r);
%cal_coef(subdata,log_dif, abs_dif_report);


