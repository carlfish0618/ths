/** ������-��-����ʹ�ϵ **/

/***����1����IC�ĽǶȣ����ǲ�ͬƵ��֮��Ĺ�ϵ **********/
/** ���Ǽ��ֹ�ϵ:
(1) abnormal attention �� abnormal volumn֮��Ĺ�ϵ
(2) interval return/attention/volumn����֮��Ĺ�ϵ
***/

/************************************ PART I: ����׼�� **************************************/

/***����abnormal attention: ���5��Ĺ�ע��/���20��(������5��)�Ĺ�ע�� */
%LET far_day = 60;
%LET near_day = 20;
%LET adjust_start_date = 5may2014;   /* ��ѡ�����ݵĿ�ʼ */
%LET adjust_end_date = 27mar2015;

DATA busday2;
	SET busday;
	id = _N_;
RUN;


/****Step1: ѡȡ��Ҫ�Ŀ����Ƶ�ʵ����� */
/** ȷ�ϵ������� */
/** ����adjust_busdate��֮���ڷ���ICʱ���õ��ñ� **/

/** type1: ��ĩ����*/
%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
	rename=end_date, output_table=adjust_busdate, type=1);

/** type2: �յ��� */
/*%get_daily_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., */
/*	rename=end_date, output_table=adjust_busdate);*/

/** type3: ��ĩ���� */
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


/** Step2:Ѱ�����ڵ����� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, C.date AS date_b, B.id-C.id AS dif_id, B.id
	FROM click_data A LEFT JOIN busday2 B
	ON A.end_date = B.date
	LEFT JOIN busday2 C
	ON B.id >= C.id AND B.id < C.id + (&far_day.+&near_day.)
	ORDER BY A.end_date, A.stock_code, dif_id;
QUIT;

/** Step3: ��Ӧ�������� */
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, 
	coalesce(C.click,0) AS click_b,
	coalesce(C.vol,0) AS vol_b,
	coalesce(C.report,0) AS report_b,
	coalesce(C.atte,0) AS atte_b,
	coalesce(C.users,0) AS users_b,
	coalesce(C.turnover,0) AS tover_b,
	coalesce(C.forward_news,0) AS fnews_b,
	coalesce(C.neg_news,0) AS neg_news_b,
	coalesce(C.pos_news,0) AS pos_news_b,
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
/**/
/*PROC SQL;*/
/*	CREATE TABLE tmp AS*/
/*	SELECT end_date, sum(users=0) AS nusers,*/
/*	count(stock_code) AS nstock*/
/*	FROM product.daily_click_data*/
/*	GROUP BY end_date*/
/*	ORDER BY end_date;*/
/*QUIT;*/

/** Step4: ������ں�Զ�ڵĵ������� */
PROC SQL;
	CREATE TABLE month_click_data AS
	SELECT stock_code, end_date, mean(id) AS id,
		/** �������Ч���� */
/*		sum(click_b~=0 AND users_b ~=0 AND dif_id < &near_day.) AS near_count,*/
/*		sum(click_b~=0 AND users_b ~= 0 AND &near_day. <= dif_id ) AS far_count,*/
		/* ��ѡ��������2014-12-3��2014-12-10֮�䶼��ȱʧ�����ʱ����ǡ�÷��仯��������click��ʱ����users������Чֵ */
		sum(click_b~=0 AND dif_id < &near_day.) AS near_count,  
		sum(click_b~=0 AND &near_day. <= dif_id ) AS far_count,
		/** ͣ������ */
		sum(is_halt_b*(dif_id<&near_day.)) AS near_halt,
		sum(is_halt_b*(dif_id>=&near_day.)) AS far_halt,
		sum(is_resumption_b*(dif_id<&near_day.)) AS near_resump,
		sum(is_resumption_b*(dif_id>=&near_day.)) AS far_resump,
		/** ����� */
		sum(click_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_click,
		sum(click_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_click,
		coalesce(sum(click_b*(dif_id<&near_day.))/sum(click_b~=0 AND dif_id < &near_day.),0) AS near_click_filter, /* �п���Ϊȱʧ*/
		coalesce(sum(click_b*(dif_id>=&near_day.))/sum(click_b~=0 AND dif_id >= &near_day.),0) AS far_click_filter,
		/** ������ѡ�� */
		sum(users_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_users,
		sum(users_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_users,
		coalesce(sum(users_b*(dif_id<&near_day.))/sum(users_b~=0 AND dif_id < &near_day.),0) AS near_users_filter, /* �п���Ϊȱʧ*/
		coalesce(sum(users_b*(dif_id>=&near_day.))/sum(users_b~=0 AND dif_id >= &near_day.),0) AS far_users_filter,
		/** �ɽ��� */
		sum(vol_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_vol,
		sum(vol_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_vol,
		coalesce(sum(vol_b*(dif_id<&near_day.))/sum(vol_b~=0 AND dif_id < &near_day.),0) AS near_vol_filter, /* �п���Ϊȱʧ*/
		coalesce(sum(vol_b*(dif_id>=&near_day.))/sum(vol_b~=0 AND dif_id >= &near_day.),0) AS far_vol_filter,
		/** ������ **/
		sum(tover_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_tover,
		sum(tover_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_tover,
		coalesce(sum(tover_b*(dif_id<&near_day.))/sum(tover_b~=0 AND dif_id < &near_day.),0) AS near_tover_filter, /* �п���Ϊȱʧ*/
		coalesce(sum(tover_b*(dif_id>=&near_day.))/sum(tover_b~=0 AND dif_id >= &near_day.),0) AS far_tover_filter,
		/** ȯ�̱���*/
		sum(report_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_report,
		sum(report_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_report,
		sum(report_b*(dif_id<&far_day.)) AS cur_report,
		/** ��ע�� */
		sum(atte_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_dfcf,
		sum(atte_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_dfcf,
		coalesce(sum(atte_b*(dif_id<&near_day.))/sum(atte_b~=0 AND dif_id < &near_day.),0) AS near_dfcf_filter, /* �п���Ϊȱʧ*/
		coalesce(sum(atte_b*(dif_id>=&near_day.))/sum(atte_b~=0 AND dif_id >= &near_day.),0) AS far_dfcf_filter,
		/** ����ת��*/
		sum(fnews_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_fnews,
		sum(fnews_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_fnews,
		/** ��������*/
		sum(neg_news_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_neg_news,
		sum(neg_news_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_neg_news,
		/** ��������*/
		sum(pos_news_b*(dif_id<&near_day.))/sum(dif_id < &near_day.) AS near_pos_news,
		sum(pos_news_b*(dif_id>=&near_day.))/sum(dif_id >= &near_day.) AS far_pos_news
	FROM click_data
	GROUP BY stock_code, end_date
	ORDER BY end_date, stock_code;
QUIT;


/*** Step5: ����abnormal attention **/
/** Ҫ��near_day��far_day����80%���ϵ����� */
DATA month_click_data(drop = i);
	SET month_click_data;
	ARRAY raw_var(8) near_click near_vol  near_dfcf near_users
					near_click_filter near_vol_filter near_dfcf_filter near_users_filter;
	ARRAY raw_var_b(8) far_click far_vol far_dfcf  far_users
					far_click_filter far_vol_filter far_dfcf_filter far_users_filter;
	ARRAY log_var(8) log_dif_click log_dif_vol log_dif_dfcf log_dif_users
					log_dif_click_filter log_dif_vol_filter log_dif_dfcf_filter log_dif_users_filter;
	ARRAY pct_var(8) pct_dif_click pct_dif_vol  pct_dif_dfcf pct_dif_users
					pct_dif_click_filter pct_dif_vol_filter pct_dif_dfcf_filter pct_dif_users_filter;
	DO i = 1 TO 8;
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
	/** ȯ�̱���ֱ���ò�� */
	abs_dif_report = near_report - far_report;
	/** ������ֱ���ò�� */
	abs_dif_tover = near_tover - far_tover;
	/** ������ص�ֱ���ò�� */
	abs_dif_fnews = near_fnews - far_fnews;
	abs_dif_pos_news = near_pos_news - far_pos_news;
	abs_dif_neg_news = near_neg_news - far_neg_news;
RUN; 


/** Step6: ���������������ڵı仯*/
/** ���������������ڵı仯*/
/** �������Ƶ�����ھ�����Ƶ */
%get_date_windows(raw_table=adjust_busdate, colname=end_date, output_table = adjust_busdate2, start_intval =-1, end_intval = 0);

/* �ʺ����� */
/*PROC SQL;*/
/*	CREATE TABLE tmp AS*/
/*	SELECT A.end_date, A.stock_code,*/
/*		A.click, A.vol,*/
/*		A.report, A.atte, A.users, A.turnover AS tover*/
/*	FROM product.daily_click_data A*/
/*	ORDER BY A.end_date, A.stock_code;*/
/*QUIT;*/

/* �ʺ���Ƶ����Ƶ */
/** ��Ƶ near = 20 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.end_date, A.stock_code,
		A.near_click_filter AS click, 
		A.near_vol_filter AS vol,
		A.near_dfcf_filter AS atte, 
		A.near_users_filter AS users,
		A.near_tover_filter AS tover,
		A.near_report AS report,
		A.near_fnews AS fnews,
		A.near_pos_news AS pos_news,
		A.near_neg_news AS neg_news
	FROM month_click_data A
	ORDER BY A.end_date, A.stock_code;
QUIT;



PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, 
	E.click AS click,
	E.vol AS vol,
	E.report AS report,
	E.atte AS atte,
	E.users AS users,
	E.tover AS tover,
	E.fnews AS fnews,
	E.neg_news AS neg_news,
	E.pos_news AS pos_news,
	D.click AS click_b,
	D.vol AS vol_b,
	D.report AS report_b,
	D.atte AS atte_b,
	D.users AS users_b,
	D.tover AS tover_b,
	D.fnews AS fnews_b,
	D.neg_news AS neg_news_b,
	D.pos_news AS pos_news_b
	FROM month_click_data A LEFT JOIN adjust_busdate2 C
	ON A.end_date = C.end_date
	LEFT JOIN tmp D
	ON C.date_b1 = D.end_date AND A.stock_code = D.stock_code
	LEFT JOIN tmp E
	ON A.end_date = E.end_date AND A.stock_code = E.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA month_click_data(drop = i click_b vol_b report_b atte_b users_b tover_b fnews_b pos_news_b neg_news_b);
	SET tmp2;
	ARRAY raw_var(4) click vol atte users;
	ARRAY raw_var_b(4) click_b vol_b  atte_b users_b;
	ARRAY log_var(4) pre_log_dif_click pre_log_dif_vol pre_log_dif_atte pre_log_dif_users;
	ARRAY pct_var(4) pre_pct_dif_click pre_pct_dif_vol pre_pct_dif_atte pre_pct_dif_users;
	DO i = 1 TO 4;
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
	IF not missing(tover) AND not missing(tover_b) AND tover ~=0 AND tover_b ~=0 
	THEN pre_abs_dif_tover = tover - tover_b;
	ELSE pre_abs_dif_tover = .;
	/** ���� */
	IF not missing(fnews) AND not missing(fnews_b) THEN pre_abs_dif_fnews = fnews - fnews_b;
	ELSE pre_abs_dif_fnews = .;
	IF not missing(neg_news) AND not missing(neg_news_b) THEN pre_abs_dif_neg_news = neg_news - neg_news_b;
	ELSE pre_abs_dif_neg_news = .;
	IF not missing(pos_news) AND not missing(pos_news_b) THEN pre_abs_dif_pos_news = pos_news - pos_news_b;
	ELSE pre_abs_dif_pos_news = .;
RUN; 

/** ���ӣ���ֵˮƽ */
%get_sector_info(stock_table=month_click_data, mapping_table=fg_wind_sector, output_stock_table=month_click_data);
%get_stock_size(stock_table=month_click_data, info_table=hqinfo, share_table=fg_wind_freeshare,output_table=month_click_data, 
							colname = size, index = 2);

/************************************ PART 2: ���ݷ��� **************************************/
/** Step0: ȡclear_data���з��� ***/
/** Ҫ��
(1) ������һ�������Ľ��ں�Զ������
(2) ��Զ�ںͽ���(������)û�г��ֹ�ͣ��/���Ƶ����
****/

%LET valid_pct = 0.8;
/*** ֮����Կ��ǽ�һ�������������� **/
DATA month_click_data;
	SET month_click_data;
	/** ���õ���-Զ����ʱ����Ҫ��ע��Ч������ */
/*	IF near_halt=0 AND far_halt=0 AND far_resump = 0 AND near_resump = 0 */
/*		AND near_count >= floor(&near_day.*&valid_pct.) AND far_count >= floor(&far_day.*&valid_pct.)THEN is_clear = 1;*/
/*	ELSE is_clear = 0;*/
	IF near_halt=0 AND near_resump = 0 
		AND near_count >= floor(&near_day.*&valid_pct.) THEN is_clear = 1;
	ELSE is_clear = 0;
RUN;


/** Step1: �޳���Ч��Ʊ�󣬷���log_dif��pct_dif�ķֲ� */
DATA subdata;
	SET month_click_data;
	IF is_clear = 0 THEN delete;
	view = pos_news - neg_news;  /* �淴������ */
	If fnews ~= 0 THEN DO;
		pos = pos_news/fnews;
		neg = neg_news/fnews;  /* �޳�ת������Ӱ�� */
		view2 = view/fnews;
	END;
	ELSE DO;
		pos = .;
		neg = .;
		view2 = .;
	END;
RUN;

 
%plot_normal(log_dif_click,subdata);
%plot_normal(log_dif_vol,subdata);
%plot_normal(log_dif_dfcf,subdata);
%plot_normal(abs_dif_report,subdata);

%plot_normal(pre_log_dif_click,subdata);
%plot_normal(pre_log_dif_vol, subdata);


/** Step2��������������ICֵ ***/
/** �õ����ĵ�adjust_busdate��� */
/** ����Ƶ��ѡ��ͬ��ic_length��*/
/**
(1) ��Ƶ���Կ���δ��8�ܡ�����Ƶ�ʵ�ʱ����Ҫ�ù�ȥ5���ƽ��ֵ��(near_***)
(2) ��Ƶ����δ��6���¡�����Ƶʱ���ù�ȥ20���ƽ��ֵ��(far_***)
(3) ��Ƶ���Կ���δ��20��
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

/******* ����1��������IC���� ***********/

%LET fname = pos;
%LET ret_column = ret_f1;
%single_factor_ic(factor_table=subdata, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
%single_factor_score(raw_table=subdata, identity=stock_code, factor_name=&fname.,
		output_table=results, is_increase = 1, group_num = 10);
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =&ret_column., is_transpose = 1, type=2);

/** ȡscore=1���飬����������Щ��Ʊ */
DATA tt;
	SET results;
	IF &fname._score = 1;
	If end_date = "12dec2014"d;
RUN;

/** ����1-1�� �����ӷ���-ֻ�������� */
%LET fname = pos_news;
%LET ret_column = ret_f1;
DATA results;
	SET subdata;
	IF not missing(&fname.) THEN DO;
		IF  &fname. < 0 THEN &fname._score = 1;
		ELSE IF &fname. = 0 THEN &fname._score = 2;
		ELSE IF &fname. > 0 THEN &fname._score = 3;
	END;
	ELSE &fname._score = .;
RUN;
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =&ret_column., is_transpose = 1, type=2);



/******* ����2����������֮�������� **************/

/** ����ʺͳɽ����������Ǵӱ仯�ʻ����Ǿ���ֵ��˵�������������������(0.8) */
%cal_coef(subdata,click, vol);
%cal_coef(subdata,click, tover); /** �ͻ�����֮���ϵ���� */

%cal_coef(subdata,pre_log_dif_click, pre_log_dif_vol);

/** ��ѡ�ɺͳɽ�����ˮƽֵ�ϵ�����Դ�����0.6-0.7; �仯���ϵ�����Դ���Ϊ0.5*/
%cal_coef(subdata,users, vol);
%cal_coef(subdata,pre_log_dif_users, pre_log_dif_vol);


/** ����ʣ���ѡ�ɺ���ֵ*/
/** ��14��11�¿�ʼ������0.5 */
%cal_coef(subdata,click, size);
%cal_coef(subdata,users, size);
%cal_coef(subdata,vol, size);


%cal_coef(subdata,click, pos_news);

/******* ����3������size���鿴�����ӵ�IC�Ƿ񻹳��� **************/

/** ����size������� */
/** size��Ϊ10��(ÿ����200ֻ��Ʊ)��Ȼ����ÿ�����ڷ�Ϊ4��(���50ֻ��Ʊ) */
%single_factor_score(raw_table=subdata, identity=stock_code, factor_name=size,
		output_table=results, is_increase = 1, group_num = 10);
DATA subdata_size;
	SET results;
	IF size_score = 10;
RUN;

%LET fname = click;
%LET ret_column = ret_f1;
%single_factor_ic(factor_table=subdata_size, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
%single_factor_score(raw_table=subdata_size, identity=stock_code, factor_name=&fname.,
		output_table=results, is_increase = 1, group_num = 4);
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =&ret_column., is_transpose = 1, type=2);


/******* ����4�����ûع飬����size���أ��鿴�����ӵ�IC�Ƿ񻹳��� **************/
DATA subdata_for_reg;
	SET subdata;
	log_click = log(click);
	log_size = log(size);
	log_vol = log(vol);
	log_users = log(users);
RUN;

PROC REG DATA = subdata_for_reg;
	MODEL log_click = log_size log_vol;
	BY end_date;
	OUTPUT OUT = reg_results(keep = end_date stock_code log_click log_size log_vol r)
		rstudent = r;
	RUN;
QUIT;
%cal_coef(results,log_click, r);
%cal_coef(results,log_vol, r);
%LET ret_column = ret_f1;
%single_factor_ic(factor_table=reg_results, return_table=ot2, group_name=stock_code, fname=r, type=3);
%single_factor_score(raw_table=reg_results, identity=stock_code, factor_name=r,
		output_table=results, is_increase = 1, group_num = 5);
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=r_score,
	ret_column =&ret_column., is_transpose = 1, type=2);

/** ȡscore=1���飬����������Щ��Ʊ */
DATA tt;
	SET r_results;
	IF r_score = 1;
	If end_date = "19dec2014"d;
RUN;


/* ֻ�Գɽ����ع� */
PROC REG DATA = subdata_for_reg;
	MODEL log_click = log_vol;
	BY end_date;
	OUTPUT OUT = reg_results(keep = end_date stock_code log_click log_vol r)
		rstudent = r;
	RUN;
QUIT;
%LET ret_column = ret_f1;
%single_factor_ic(factor_table=reg_results, return_table=ot2, group_name=stock_code, fname=r, type=3);
%single_factor_score(raw_table=reg_results, identity=stock_code, factor_name=r,
		output_table=results, is_increase = 1, group_num = 5);
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=r_score,
	ret_column =&ret_column., is_transpose = 1, type=2);


/* ֻ��size�ع� */
PROC REG DATA = subdata_for_reg;
	MODEL log_click = log_size;
	BY end_date;
	OUTPUT OUT = reg_results(keep = end_date stock_code log_click log_size r)
		rstudent = r;
	RUN;
QUIT;
%LET ret_column = ret_f1;
%single_factor_ic(factor_table=reg_results, return_table=ot2, group_name=stock_code, fname=r, type=3);
%single_factor_score(raw_table=reg_results, identity=stock_code, factor_name=r,
		output_table=results, is_increase = 1, group_num = 5);
%single_score_ret(score_table=results, return_table=ot2, identity=stock_code, score_name=r_score,
	ret_column =&ret_column., is_transpose = 1, type=2);



/** vol ��size���ع� */
PROC REG DATA = subdata_for_reg;
	MODEL log_vol = log_size;
	BY end_date;
	OUTPUT OUT = results3(keep = end_date stock_code log_vol log_size r3)
		rstudent = r3;
	RUN;
QUIT;
%LET ret_column = ret_f1;
%single_factor_ic(factor_table=results3, return_table=ot2, group_name=stock_code, fname=r3, type=3);
%single_factor_score(raw_table=results3, identity=stock_code, factor_name=r3,
		output_table=r_results, is_increase = 1, group_num = 5);
%single_score_ret(score_table=r_results, return_table=ot2, identity=stock_code, score_name=r3_score,
	ret_column =&ret_column., is_transpose = 1, type=2);



/** !!���ۣ�����ʺͳɽ�����IC�����ơ�����log-dif����pct-dif��΢�в��죬��������*/
/** �鿴���ߵ������(���Է��֣��߶���أ���ֵ��0.86�����ֵΪ0.78�����ֵ�ܴﵽ0.93) */
/** �����ֽ��ͣ�
(1) �����Ϊ��������˽����������Ժͳɽ����Ŵ�����������ϵ�ġ����������ʶ�����Щ�ǳɽ�����ĵ����
(2) �ɽ����͵�������ǹ�ע�ȵı�����ʽ����������Խϴ�
***/



/** ��IC���Ƶķ�����Ϊ��ͳһ������pct-difference����Ϊcal_intval_return�в��õľ���pct-difference�ķ���) **/
/** vol-difference���������ԣ��Լ�click�벻ͬ��vol֮�������� */

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

