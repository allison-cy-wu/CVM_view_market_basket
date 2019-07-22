from utility_functions.date_period import date_period
from connect2Databricks.read2Databricks import redshift_cdw_read
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, ltrim, rtrim
# df.withColumn('col4',explode(split('col4',' '))).show()
def pull_lsg_omni_history(
        start_date:str,
        period:int,
):
    start_date, end_date = date_period(period, start_date)

    query  = 'SELECT ' \
             'VS.visit_session_key AS session_key, ' \
             'HIT.post_visid_combined, ' \
             'HIT.visit_return_count AS visit_number, ' \
             "TRIM(SUBSTRING(TRIM(DEMANDBASE), 0, POSITION('|' IN TRIM(DEMANDBASE)))) AS " \
             "account_no, "\
             'TRIM(prod_list) AS prod_list ' \
             'FROM datalake_omni.omni_hit_data HIT ' \
             'LEFT JOIN CDWDS.D_OMNI_VISIT_SESSION VS ON ' \
             '  VS.VISIT_RETURN_COUNT=HIT.VISIT_RETURN_COUNT AND VS.POST_VISID_COMBINED=HIT.POST_VISID_COMBINED ' \
             f'WHERE HIT.hit_time_gmt_dt_key<{start_date} AND HIT.hit_time_gmt_dt_key>={end_date} ' \
             "AND prod_list IS NOT NULL AND prod_list <> 'shipping-handling'"


    df = redshift_cdw_read(query, db_type = 'RS', database = 'CDWDS', env = 'TST').\
        filter(col('account_no') != 'undefined').\
        withColumn('prod_id',rtrim(ltrim(explode(split('prod_list',',')))))


    return df

"""
col_renaming = {
  'visit_num' : 'VISIT_RETURN_COUNT',
  'visit_page_num' : 'visit_page_seq',
  'post_page_event_var1' : 'url_text',
  'post_purchaseid' : 'omni_purch_id',
  'hit_time_gmt' : 'hit_time_gmt_unix_ts',
  'page_event' : 'page_evt_id',
  'hourly_visitor' : 'new_hourly_visitor_flag',
  'daily_visitor' : 'new_daily_visitor_flag',
  'monthly_visitor' : 'new_monthly_visitor_flag',
  'quarterly_visitor' : 'new_quarterly_visitor_flag',
  'weekly_visitor' : 'new_weekly_visitor_flag',
  'yearly_visitor' : 'new_yearly_visitor_flag',
  'post_evar10' : 'target_email',
  'geo_country' : 'geographic_country_code',
  'post_evar42' : 'days_band_since_last_visit',
  'pagename' : 'current_pagename',
  'post_evar14':'Form_ID ',
  'post_evar15':'Form_Name ',
  'post_evar21':'OnSite_Merchandising_Name',
  'post_evar23':'Omni_Visitor_ID ',
  'post_evar43':'NEW_RETURN_IND',
  'post_evar60':'opt_ver_nm',
  'post_evar45':'PURCHASE_ID',
  'ref_domain':'refer_domain_nm',
  'ref_type':'Omni_Ref_type_Id',
  'referrer':'refer_url_txt',
  'post_evar18':'current_Page_URL',
  'post_evar25':'Visitor_Login_Name',
  'post_prop8':'STOREFRONT_ROOT_NM ',
  'post_prop6':'STOREFRONT_NM ',
  'country':'Omni_Country_ID',
  'hit_source':'Hit_source Text',
  'post_evar4':'Content_div',
  'post_evar13':'Prod_div',
  'post_evar31':'search_term',
  'ip' : 'ip_addr',
  'evar64': 'container_id',
  'post_prop10' : 'EMID',
  'campagin': 'campaign',
  'post_campaign':'post_campaign',
  'post_evar8':'DemandBase',
  'post_evar9':'DemandBase_ACCT_TIER',
  'evar40':'AdobeCloudID',
  'post_evar50':'AMO_ID',
  'post_evar22' : 'Post_Social_ID',
  'evar22': 'Social_ID',
  'evar62' : 'PDP_View',
  'prop3' : 'VideoName',
  'browser' : 'Browser',
  'browser_height' : 'Browser_height',
  'browser_width' : 'Browser_width',
  'os':'OS'
}
"""




def cvm_pre_processing(df):