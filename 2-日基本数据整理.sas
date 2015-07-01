/** 2- 日点击率、成交量、同花顺关注度、券商报告等数据 **/
/** 一次性运行，之后的结果存储在 product.daily_click_data中 */

/** 输出:
(1) product.daily_click_data: stock_code/end_date/is_halt/is_resumption/click/vol/report/atte
***/

/** 需用到的外部表包括:
(1) product.raw_data_mdf: 同花顺点击量数据
(2) quant.dfcf: 东方财富关注度数据
(3) product.der_report_research: 朝阳永续个股研究报告
(4) product.hqinfo: 行情数据(含成交量）
(5) product.stock_info_table: 个股上市信息等
***/

%LET adjust_start_date = 9apr2012;
%LET adjust_end_date = 27mar2015;
%LET adjust_start_date2 = 15apr2012;  /** 计算过去一个月收益时使用 */

/**** Step1：挑选出满足一定条件的股票 **/
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

/** 要求上市满3个月 */
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

/** 剔除停牌超过60个交易日的股票 */
/** 提供标志位: is_halt, is_resumption */
%LET halt_filter_day = 60;
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, 
	coalesce(B.is_halt,.) AS is_halt,  /** 没有is_halt记录的表示正在长期停牌，行情表数据缺失*/
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



/** Step2: 获取点击率数据 */
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



/** Step3: 获取交易量和换手率信息 */
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
	IF turnover >= 70 THEN turnover = .; /* 有部分存在异常的数据 */
RUN;



/** Step4: 对应券商报告数据 */
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



/** Step5: 对应东方财富数据 */

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

/** Step6: 对应同花顺自选股数据 */
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.users
	FROM click_data A LEFT JOIN product.self_stock B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** 因为自选股的数据比较少，所以对于最小日期之后的users需要弄成0 */
PROC SQL;
   UPDATE tmp
  SET users  = 0  WHERE missing(users) AND end_date IN (SELECT end_date FROM product.self_stock);
QUIT;
DATa click_data;
	SET tmp;
RUN;


/** Step7: 对应同花顺舆情数据 */
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.neg_news, B.pos_news, B.imp_news, B.common_news, B.forward_news
	FROM click_data A LEFT JOIN product.news B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** 因为舆情数据比较少，所以对于最小日期之后的需要弄成0 */
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

/** 围脖 */
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.negative AS neg_wb, B.positive As pos_wb, 
		B.forward AS forward_wb, B.comment AS comment_wb
	FROM click_data A LEFT JOIN product.weibo B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** 因为舆情数据比较少，所以对于最小日期之后的需要弄成0 */
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

/** 微信 */
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.wx
	FROM click_data A LEFT JOIN product.weixin B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** 因为舆情数据比较少，所以对于最小日期之后的需要弄成0 */
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





