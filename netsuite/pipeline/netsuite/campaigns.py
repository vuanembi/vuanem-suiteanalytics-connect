from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "CAMPAIGNS",
    [
        {"name": "ACCOUNT_NUMBER", "type": "STRING"},
        {"name": "AUDIENCE_ID", "type": "INTEGER"},
        {"name": "BANK_ACCOUNT_NAME", "type": "STRING"},
        {"name": "BANK_NAME", "type": "STRING"},
        {"name": "CAMPAIGN_EXTID", "type": "STRING"},
        {"name": "CAMPAIGN_ID", "type": "INTEGER"},
        {"name": "CATEGORY_ID", "type": "INTEGER"},
        {"name": "CI_THIN", "type": "STRING"},
        {"name": "COST_0", "type": "STRING"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "END_DATE", "type": "TIMESTAMP"},
        {"name": "EXPECTEDREVENUE", "type": "STRING"},
        {"name": "FAMILY_ID", "type": "STRING"},
        {"name": "FROM_CAMPAIGN", "type": "STRING"},
        {"name": "FROM_LANDING", "type": "STRING"},
        {"name": "GI_D_KIN", "type": "TIMESTAMP"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "IS_SALES_CAMPAIGN", "type": "STRING"},
        {"name": "KEYWORD", "type": "STRING"},
        {"name": "KIN_THC_T_VN_SP_ID", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LEAD_SOURCE_ID", "type": "INTEGER"},
        {"name": "L_DO_CHO_S_IM", "type": "STRING"},
        {"name": "L_DO_KHNG_MUA_HNG_ID", "type": "INTEGER"},
        {"name": "MESSAGE", "type": "STRING"},
        {"name": "NEW_SEGMENT_ID", "type": "INTEGER"},
        {"name": "NGY_D_KIN", "type": "TIMESTAMP"},
        {"name": "NGY_HON_THNH_KHIU_NIBO_", "type": "TIMESTAMP"},
        {"name": "NPS_SCORE_ID", "type": "INTEGER"},
        {"name": "NUMBER_0", "type": "STRING"},
        {"name": "NV_BN_HNG_ID", "type": "INTEGER"},
        {"name": "NV_BO_HNH_M_ID", "type": "INTEGER"},
        {"name": "NV_GIAO_HNG_ID", "type": "INTEGER"},
        {"name": "NV_H_TR_GIAO_HNG_ID", "type": "INTEGER"},
        {"name": "OFFER_ID", "type": "INTEGER"},
        {"name": "ORGANIZER_ID", "type": "INTEGER"},
        {"name": "PAYMENT_METHOD_ID", "type": "INTEGER"},
        {"name": "RATING_KHCH_VO_CH_CHA_MUA__ID", "type": "INTEGER"},
        {"name": "RATING__KHCH_DIGITALHOTLINE_ID", "type": "INTEGER"},
        {"name": "RATING__SAU_KHI_GIAO_HNG_ID", "type": "INTEGER"},
        {"name": "RATING__SAU_KHI_LN_SO_ID", "type": "INTEGER"},
        {"name": "RATING__SN_PHM_A_DNG_PHON_ID", "type": "INTEGER"},
        {"name": "SAO_CHO_NVBH_ID", "type": "INTEGER"},
        {"name": "SAO_CHO_NVGH_ID", "type": "INTEGER"},
        {"name": "SAO_CHO_NV_BO_HNH_M_ID", "type": "INTEGER"},
        {"name": "SAO_CHO_NV_H_TR_ID", "type": "INTEGER"},
        {"name": "SEARCH_ENGINE_ID", "type": "INTEGER"},
        {"name": "SN_PHM_ID", "type": "INTEGER"},
        {"name": "SOURCES_ID", "type": "INTEGER"},
        {"name": "SO_LOCATION_ID", "type": "INTEGER"},
        {"name": "SO_REFERENCE_ID", "type": "INTEGER"},
        {"name": "START_DATE", "type": "TIMESTAMP"},
        {"name": "TIME_ZONE_0", "type": "STRING"},
        {"name": "TITLE", "type": "STRING"},
        {"name": "URL", "type": "STRING"},
        {"name": "VERTICAL_ID", "type": "INTEGER"},
        {"name": "YU_CU_SA_CHA", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            ACCOUNT_NUMBER,
            AUDIENCE_ID,
            BANK_ACCOUNT_NAME,
            BANK_NAME,
            CAMPAIGN_EXTID,
            CAMPAIGN_ID,
            CATEGORY_ID,
            CI_THIN,
            COST_0,
            DATE_CREATED,
            DATE_LAST_MODIFIED,
            END_DATE,
            EXPECTEDREVENUE,
            FAMILY_ID,
            FROM_CAMPAIGN,
            FROM_LANDING,
            GI_D_KIN,
            IS_INACTIVE,
            IS_SALES_CAMPAIGN,
            KEYWORD,
            KIN_THC_T_VN_SP_ID,
            LAST_MODIFIED_DATE,
            LEAD_SOURCE_ID,
            L_DO_CHO_S_IM,
            L_DO_KHNG_MUA_HNG_ID,
            MESSAGE,
            NEW_SEGMENT_ID,
            NGY_D_KIN,
            NGY_HON_THNH_KHIU_NIBO_,
            NPS_SCORE_ID,
            NUMBER_0,
            NV_BN_HNG_ID,
            NV_BO_HNH_M_ID,
            NV_GIAO_HNG_ID,
            NV_H_TR_GIAO_HNG_ID,
            OFFER_ID,
            ORGANIZER_ID,
            PAYMENT_METHOD_ID,
            RATING_KHCH_VO_CH_CHA_MUA__ID,
            RATING__KHCH_DIGITALHOTLINE_ID,
            RATING__SAU_KHI_GIAO_HNG_ID,
            RATING__SAU_KHI_LN_SO_ID,
            RATING__SN_PHM_A_DNG_PHON_ID,
            SAO_CHO_NVBH_ID,
            SAO_CHO_NVGH_ID,
            SAO_CHO_NV_BO_HNH_M_ID,
            SAO_CHO_NV_H_TR_ID,
            SEARCH_ENGINE_ID,
            SN_PHM_ID,
            SOURCES_ID,
            SO_LOCATION_ID,
            SO_REFERENCE_ID,
            START_DATE,
            TIME_ZONE_0,
            TITLE,
            URL,
            VERTICAL_ID,
            YU_CU_SA_CHA
        FROM
            "Vua Nem Joint Stock Company".Administrator.CAMPAIGNS
    """,
)
