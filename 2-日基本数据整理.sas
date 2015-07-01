/** 2- �յ���ʡ��ɽ�����ͬ��˳��ע�ȡ�ȯ�̱�������� **/
/** һ�������У�֮��Ľ���洢�� product.daily_click_data�� */

/** ���:
(1) product.daily_click_data: stock_code/end_date/is_halt/is_resumption/click/vol/report/atte
***/

/** ���õ����ⲿ�����:
(1) product.raw_data_mdf: ͬ��˳���������
(2) quant.dfcf: �����Ƹ���ע������
(3) product.der_report_research: �������������о�����
(4) product.hqinfo: ��������(���ɽ�����
(5) product.stock_info_table: ����������Ϣ��
***/

%LET adjust_start_date = 9apr2012;
%LET adjust_end_date = 27mar2015;
%LET adjust_start_date2 = 15apr2012;  /** �����ȥһ��������ʱʹ�� */

/**** Step1����ѡ������һ�������Ĺ�Ʊ **/
%get_daily_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
	rename=end_date, output_table=adjust_busdate);

PROC SQL;
	CREATE TABLE click_data AS
	SELECT A.stock_code, B.end_date
	FROM
	(SELECT distinct stock_code FROM hqinfo) A,
	(SELECT end_date FROM adjust_busdate) B
	ORDER BY end_date, stock_code;
QUIT;

/** Ҫ��������3���� */
%LET filter_day = 90;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.list_date,
		B.delist_date, B.is_delist
	FROM click_data A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code 
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA click_data(drop = list_date delist_date is_delist);
	SET tmp;
	IF missing(list_date) THEN delete;
	ELSE IF end_date - list_date <= &filter_day. THEN delete;
	ELSE IF is_delist = 1 AND end_date >= delist_date THEN delete;
RUN;

/** �޳�ͣ�Ƴ���60�������յĹ�Ʊ */
/** �ṩ��־λ: is_halt, is_resumption */
%LET halt_filter_day = 60;
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, 
	coalesce(B.is_halt,.) AS is_halt,  /** û��is_halt��¼�ı�ʾ���ڳ���ͣ�ƣ����������ȱʧ*/
	coalesce(B.is_resumption,0) AS is_resumption, 
	coalesce(B.halt_days,0) AS halt_days
	FROM click_data A LEFT JOIN market_table B
	ON A.end_date= B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA click_data(drop = halt_days);
	SET tmp;
	IF halt_days >= &halt_filter_day. THEN delete;
	IF missing(is_halt) THEN delete;
RUN;



/** Step2: ��ȡ��������� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, 
	coalesce(B.click,0) AS click
	FROM click_data A LEFT JOIN product.raw_data_mdf B
	ON A.end_date = B.date AND A.stock_code = B.stock
	ORDER BY A.end_date, A.stock_code;
QUIT; 
DATA click_data;
	SET tmp;
RUN;



/** Step3: ��ȡ�������ͻ�������Ϣ */
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, 
		coalesce(B.vol, 0) AS vol,
		coalesce(B.value,0) AS trade_value
	FROM click_data A LEFT JOIN product.hqinfo B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT; 
DATA click_data;
	SET tmp2;
RUN;


%get_stock_size(stock_table=click_data, info_table=hqinfo, share_table=fg_wind_freeshare,output_table=click_data, colname=liqa_value,index = 4);
DATA click_data;
	SET click_data;
	turnover = trade_value/liqa_value*100;
	IF turnover >= 70 THEN turnover = .; /* �в��ִ����쳣������ */
RUN;



/** Step4: ��Ӧȯ�̱������� */
PROC SQL;
	CREATE TABLE report_stat AS
	SELECT code AS stock_code,
	datepart(create_date) AS end_date FORMAT yymmdd10.,
	count(1) AS t_reports, 
	sum(type_id IN (22,23,24,25)) AS t_reports_neat
	FROM product.der_report_research
	GROUP BY stock_code, end_date
	ORDER BY end_date, code;
QUIT;
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, 
	coalesce(B.t_reports_neat, 0) AS report
	FROM click_data A LEFT JOIN report_stat B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT; 

DATA click_data;
	SET tmp2;
RUN;



/** Step5: ��Ӧ�����Ƹ����� */

PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, 
	coalesce(B.atte, 0) AS atte
	FROM click_data A LEFT JOIN quant.dfcf B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT; 

DATA click_data;
	SET tmp2;
RUN;

/** Step6: ��Ӧͬ��˳��ѡ������ */
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.users
	FROM click_data A LEFT JOIN product.self_stock B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** ��Ϊ��ѡ�ɵ����ݱȽ��٣����Զ�����С����֮���users��ҪŪ��0 */
PROC SQL;
   UPDATE tmp
  SET users  = 0  WHERE missing(users) AND end_date IN (SELECT end_date FROM product.self_stock);
QUIT;
DATa click_data;
	SET tmp;
RUN;


/** Step7: ��Ӧͬ��˳�������� */
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.neg_news, B.pos_news, B.imp_news, B.common_news, B.forward_news
	FROM click_data A LEFT JOIN product.news B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** ��Ϊ�������ݱȽ��٣����Զ�����С����֮�����ҪŪ��0 */
PROC SQL;
   UPDATE tmp
  	SET neg_news  = 0  WHERE missing(neg_news) AND end_date IN (SELECT end_date FROM product.news);
  UPDATE tmp
  	SET pos_news  = 0  WHERE missing(pos_news) AND end_date IN (SELECT end_date FROM product.news);
  UPDATE tmp
  	SET imp_news  = 0  WHERE missing(imp_news) AND end_date IN (SELECT end_date FROM product.news);
  UPDATE tmp
  	SET common_news  = 0  WHERE missing(common_news) AND end_date IN (SELECT end_date FROM product.news);
  UPDATE tmp
  	SET forward_news  = 0  WHERE missing(forward_news) AND end_date IN (SELECT end_date FROM product.news);
QUIT;
DATa click_data;
	SET tmp;
RUN;

/** Χ�� */
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.negative AS neg_wb, B.positive As pos_wb, 
		B.forward AS forward_wb, B.comment AS comment_wb
	FROM click_data A LEFT JOIN product.weibo B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** ��Ϊ�������ݱȽ��٣����Զ�����С����֮�����ҪŪ��0 */
PROC SQL;
   UPDATE tmp
  	SET neg_wb  = 0  WHERE missing(neg_wb) AND end_date IN (SELECT end_date FROM product.weibo);
  UPDATE tmp
  	SET pos_wb  = 0  WHERE missing(pos_wb) AND end_date IN (SELECT end_date FROM product.weibo);
  UPDATE tmp
  	SET forward_wb  = 0  WHERE missing(forward_wb) AND end_date IN (SELECT end_date FROM product.weibo);
  UPDATE tmp
  	SET comment_wb  = 0  WHERE missing(comment_wb) AND end_date IN (SELECT end_date FROM product.weibo);
QUIT;
DATa click_data;
	SET tmp;
RUN;

/** ΢�� */
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.wx
	FROM click_data A LEFT JOIN product.weixin B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** ��Ϊ�������ݱȽ��٣����Զ�����С����֮�����ҪŪ��0 */
PROC SQL;
   UPDATE tmp
  	SET wx = 0  WHERE missing(wx) AND end_date IN (SELECT end_date FROM product.weixin);
QUIT;
DATa click_data;
	SET tmp;
RUN;


DATA product.daily_click_data;
	SET click_data;
RUN;





