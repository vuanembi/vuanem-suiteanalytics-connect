from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class CAMPAIGNS(NetSuite):
    query = """
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
    """
    schema = [
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
    ]
    columns = [
        Column("ACCOUNT_NUMBER", String),
        Column("AUDIENCE_ID", Integer),
        Column("BANK_ACCOUNT_NAME", String),
        Column("BANK_NAME", String),
        Column("CAMPAIGN_EXTID", String),
        Column("CAMPAIGN_ID", Integer, primary_key=True),
        Column("CATEGORY_ID", Integer),
        Column("CI_THIN", String),
        Column("COST_0", String),
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("END_DATE", DateTime(timezone=True)),
        Column("EXPECTEDREVENUE", String),
        Column("FAMILY_ID", String),
        Column("FROM_CAMPAIGN", String),
        Column("FROM_LANDING", String),
        Column("GI_D_KIN", DateTime(timezone=True)),
        Column("IS_INACTIVE", String),
        Column("IS_SALES_CAMPAIGN", String),
        Column("KEYWORD", String),
        Column("KIN_THC_T_VN_SP_ID", String),
        Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
        Column("LEAD_SOURCE_ID", Integer),
        Column("L_DO_CHO_S_IM", String),
        Column("L_DO_KHNG_MUA_HNG_ID", Integer),
        Column("MESSAGE", String),
        Column("NEW_SEGMENT_ID", Integer),
        Column("NGY_D_KIN", DateTime(timezone=True)),
        Column("NGY_HON_THNH_KHIU_NIBO_", DateTime(timezone=True)),
        Column("NPS_SCORE_ID", Integer),
        Column("NUMBER_0", String),
        Column("NV_BN_HNG_ID", Integer),
        Column("NV_BO_HNH_M_ID", Integer),
        Column("NV_GIAO_HNG_ID", Integer),
        Column("NV_H_TR_GIAO_HNG_ID", Integer),
        Column("OFFER_ID", Integer),
        Column("ORGANIZER_ID", Integer),
        Column("PAYMENT_METHOD_ID", Integer),
        Column("RATING_KHCH_VO_CH_CHA_MUA__ID", Integer),
        Column("RATING__KHCH_DIGITALHOTLINE_ID", Integer),
        Column("RATING__SAU_KHI_GIAO_HNG_ID", Integer),
        Column("RATING__SAU_KHI_LN_SO_ID", Integer),
        Column("RATING__SN_PHM_A_DNG_PHON_ID", Integer),
        Column("SAO_CHO_NVBH_ID", Integer),
        Column("SAO_CHO_NVGH_ID", Integer),
        Column("SAO_CHO_NV_BO_HNH_M_ID", Integer),
        Column("SAO_CHO_NV_H_TR_ID", Integer),
        Column("SEARCH_ENGINE_ID", Integer),
        Column("SN_PHM_ID", Integer),
        Column("SOURCES_ID", Integer),
        Column("SO_LOCATION_ID", Integer),
        Column("SO_REFERENCE_ID", Integer),
        Column("START_DATE", DateTime(timezone=True)),
        Column("TIME_ZONE_0", String),
        Column("TITLE", String),
        Column("URL", String),
        Column("VERTICAL_ID", Integer),
        Column("YU_CU_SA_CHA", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
