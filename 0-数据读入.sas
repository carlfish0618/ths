/** ���ݶ��� */
/** ͬ��˳�������� */

%LET product_dir = D:\Research\GIT-BACKUP\ths;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 


%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

options validvarname=any; /* ֧�����ı����� */

%LET env_start_date = 1jan2012;

/******************** Step1: ������������ **************/
/* Step1-1: �����ļ��� */
/* use cmd: dir ~\*_d.txt /b/s > ~\file_list.txt */
DATA product.file_list;
	INFILE "&input_dir.\file_list.txt" TRUNCOVER;
	INPUT filepath $100.;
RUN;


/* Step1-2: ����ÿ������ */
DATA product.raw_data;
	SET product.file_list;
	INFILE dummy FILEVAR = filepath END = done DLM = '09'X TRUNCOVER;
	DO WHILE (not done);
		INPUT date yymmdd8. stock $ click pct $ dod $ ;
		FORMAT date yymmdd10.;
		OUTPUT;
	END;
RUN;

/** �ٷֱ����� */
DATA product.raw_data_mdf(drop = pct2 dod2);
	SET product.raw_data(rename = (pct = pct2 dod = dod2));
	pct = input(tranwrd(pct2, '%',''),8.);
	dod = input(tranwrd(dod2, '%',''),8.);
RUN;

/******************** Step2: ������ѡ������ **************/
%read_from_excel(excel_path=&input_dir.\self_stock.xlsx, output_table=self_stock, sheet_name = Sheet1$);
DATA product.self_stock(keep = end_date stock_code stock_name users);
	SET self_stock;
	end_date = input(put(end_date,$8.),yymmdd8.);
	FORMAT end_date yymmdd10.;
RUN;
/** ȱʧ���� */
/*PROC SQL;*/
/*	CREATE TABLE missing AS*/
/*	SELECT date*/
/*	FROM busday*/
/*	WHERE date NOT IN*/
/*	(SELECT end_date FROM product.self_stock)*/
/*	AND date >= (SELECT min(end_date) FROM product.self_stock)*/
/*	AND date <= (SELECT max(end_date) FROM product.self_stock)*/
/*	ORDER BY date;*/
/*QUIT;*/


/******************** Step3: ���붫���Ƹ����� **************/
%LET dfcf_dir = D:\Research\GIT-BACKUP\dfcf;


%LET dfcf_input_dir = &dfcf_dir.\input_data; 
%LET dfcf_output_dir = &dfcf_dir.\output_data;
LIBNAME quant "&dfcf_dir.\sasdata";


proc import out=quant.dfcf_2013_0 datafile="&dfcf_input_dir.\2013��ע��.csv" dbms=csv replace;
	getnames=yes;
	datarow=2;
run;
proc import out=quant.dfcf_2014_0 datafile="&dfcf_input_dir.\2014��ע��.csv" dbms=csv replace;
	getnames=yes;
	datarow=2;
run;
proc import out=quant.dfcf_2015_0 datafile="&dfcf_input_dir.\2015��ע��.csv" dbms=csv replace;
	getnames=yes;
	datarow=2;
run;
/** ͳһ��ʽ */
DATA quant.dfcf_2014_1(drop = i changedate2 var4--var9);
	SET quant.dfcf_2014_0(rename = (changedate = changedate2));
	changedate = input(changedate2, yymmdd10.);
	FORMAT changedate yymmdd10.;
	ARRAY var_list(6) var4--var9;
	ARRAY var_list2(6) nvar4 nvar5 nvar6 nvar7 nvar8 nvar9;
	DO i = 1 TO 6;
		var_list2(i) = input(var_list(i),8.);
	END;	
RUN;

DATA quant.dfcf_2015_1(drop = i changedate2 var4--var9);
	SET quant.dfcf_2015_0(rename = (changedate = changedate2));
	changedate = input(changedate2, yymmdd10.);
	FORMAT changedate yymmdd10.;
	ARRAY var_list(6) var4--var9;
	ARRAY var_list2(6) nvar4 nvar5 nvar6 nvar7 nvar8 nvar9;
	DO i = 1 TO 6;
		var_list2(i) = input(var_list(i),8.);
	END;	
RUN;

DATA quant.dfcf_2013_1(drop = i var4--var9);
	SET quant.dfcf_2013_0;
	ARRAY var_list(6) var4--var9;
	ARRAY var_list2(6) nvar4 nvar5 nvar6 nvar7 nvar8 nvar9;
	DO i = 1 TO 6;
		var_list2(i) = var_list(i);
	END;	
RUN;

DATA quant.dfcf;
	SET quant.dfcf_2013_1 quant.dfcf_2014_1 quant.dfcf_2015_1;
RUN;

proc sql;
	create table tmp as
		select substr(stockcode,1,6) as stock_code, stockname AS stock_name, changedate as end_date,
			nvar4 AS atte, nvar5 AS attechg, nvar6 AS attechgrate, nvar7 AS atterank, nvar8 AS attechgrank, nvar9 AS attechgraterank
		from quant.dfcf
		order by end_date, stock_code;
quit;
DATA quant.dfcf;
	SET tmp;
RUN;

/******************** Step4: ������������ **************/
%MACRO weibo_yq(input_data, output_data);
	DATA &output_data.(drop = ���� ���� ����΢���� ����΢���� �������� ��ת����);
		SET &input_data.;
		LENGTH stock_code $10.;
		stock_code = substr(����, 1, 6);
		end_date = input(����,yymmdd10.);
		neg_wb = input(����΢����, 8.);
		pos_wb = input(����΢����, 8.);
		forward_wb = input(��ת����, 8.);
		comment_wb = ��������;
		FORMAT end_date yymmdd10.;
	RUN;
	PROC SORT DATA = &output_data.;
		BY end_date stock_code;
	RUN;
%MEND weibo_yq;

%MACRO xw_yq(input_data, output_data);
	DATA &output_data.(drop = ���� ����	��������ƪ��	��������ƪ��	��Ҫ����ƪ��	��ͨ����ƪ��	ת����);
		SET &input_data.;
		LENGTH stock_code $10.;
		stock_code = substr(����, 1, 6);
		end_date = input(����,yymmdd10.);
		neg_news = input(��������ƪ��, 8.);
		pos_news = input(��������ƪ��, 8.);
		imp_news = input(��Ҫ����ƪ��, 8.);
		common_news = input(��ͨ����ƪ��, 8.);
		forward_news = ת����;
		FORMAT end_date yymmdd10.;
	RUN;
	PROC SORT DATA = &output_data.;
		BY end_date stock_code;
	RUN;
%MEND xw_yq;

%MACRO wx_yq(input_data, output_data);
	DATA &output_data.(drop = ���� ����	΢������������������);
		SET &input_data.;
		LENGTH stock_code $10.;
		stock_code = substr(����, 1, 6);
		end_date = input(����,yymmdd10.);
		wx = ΢������������������;
		FORMAT end_date yymmdd10.;
	RUN;
	PROC SORT DATA = &output_data.;
		BY end_date stock_code;
	RUN;
%MEND wx_yq;


%MACRO loop_wb(filepath, sheet_prefix, data_prefix, func, nobs=20);
	%DO i = 1 %TO &nobs.;
		%read_from_excel(excel_path=&input_dir.\&filepath. , output_table=&data_prefix.&i., sheet_name = &sheet_prefix.&i.$);
		%&func.(&data_prefix.&i., &data_prefix.&i.);
		%IF %SYSEVALF(&i.=1) %THEN %DO;
			DATA &data_prefix._all;
				SET &data_prefix.&i.;
			RUN;
		%END;
		%ELSE %DO;
			DATA &data_prefix._all;
				SET &data_prefix._all &data_prefix.&i.;
			RUN;
		%END;
		PROC SQL;
			DROP TABLE &data_prefix.&i.;
		QUIT;
	%END;
%MEND loop_wb;



%loop_wb(filepath=�������������Ŷ�������������-(΢��).xls, sheet_prefix=΢��, data_prefix=wb, func = weibo_yq, funcnobs=20);
%loop_wb(filepath=�������������Ŷ�������������-(����).xls, sheet_prefix=����, data_prefix=news, func=xw_yq, nobs=20);
%loop_wb(filepath=�������������Ŷ�������������-(΢��).xls, sheet_prefix=΢��, data_prefix=wx, func=wx_yq, nobs=20);

DATA product.weixin;
	SET wx_all;
RUN;
DATA product.weibo;
	SET wb_all;
RUN;
DATA product.news;
	SET news_all;
RUN;

