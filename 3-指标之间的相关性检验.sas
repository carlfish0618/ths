/*** ָ��֮�������Լ��� */


/************************************************** Step1: �����ϣ�ͬ��˳�ĵ����ָ���Ƿ��������Ի��߿�Ԥ����? **/
/*** ��������������һ�ײ�ֿ���Granger Causality ***/

/**��������ܵ����ݲ���Granger Casuality Test **/
/** ���������֮һ�ǣ�attָ�걾���п�������ͬ��˳����͸�����µġ�*/

/* Step1-1: ��ÿ�յ�log_dif_att��log_dif_vol */
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
/** �޳�����δ��90��� */
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

/** �޳�ͣ�Ƴ���60�������յĹ�Ʊ */
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

/** ͳ��ÿ�յ����� */
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
	/* ��һ����¼�޳� */
	IF _N_ = 1 THEN delete; 
RUN;


%test_stationarity(daily_analysis, log_click);
%test_stationarity(daily_analysis, log_dif);
%test_stationarity(daily_analysis, log_users);

%test_stationarity(daily_analysis, log_vol);
%test_stationarity(daily_analysis, log_dif_vol);
%test_stationarity(daily_analysis, log_dif_users);


/** ����Granger test��ֻ�����ƽ������ */
/** ���1��P=0.27,�޷��ܾ�H0�����������Ե����û��Ԥ���� */
%granger_test(daily_analysis, log_dif, log_dif_vol);
/** �������Լ�����ѡ��������Ԥ���ԣ���ϵ�������� */
%granger_test(daily_analysis, log_dif_users, log_dif_vol);


/** ���2��P=0.01,�޷��ܾ�H0����������Խ���������Ԥ���� */
%granger_test(daily_analysis, log_dif_vol, log_dif);
/** ������ѡ�������Խ�������Ԥ���ԣ���ϵ�������� */
%granger_test(daily_analysis, log_dif_vol, log_dif_users);



/**** Step1-2: �Ƚ��Լ���-������N���Ԥ���� **/
/** û�д��ڵ��ص��� */
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
	IF _N_ = 1 THEN delete; /* ��һ����¼�޳� */
RUN;

/*%test_stationarity(daily_expand, log_click);*/
/*%test_stationarity(daily_expand, log_dif);*/
/*%test_stationarity(daily_expand, log_vol);*/
/*%test_stationarity(daily_expand, log_dif_vol);*/

/** ����Granger test��ֻ�����ƽ������ */
/** ���1���޷��ܾ�H0�����������Ե����û��Ԥ���� */
%granger_test(daily_expand, log_dif, log_dif_vol);

/** ���2����win_length>2ʱ���޷��ܾ�H0��������������ʱ��������Խ�����û��Ԥ���� */
%granger_test(daily_expand, log_dif_vol, log_dif);

/*** �� ���ۣ�������������������Խ�������"Ԥ����"ֻ��ǰ��һ�졣��������ͬ��ָ�� *********************/


