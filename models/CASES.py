from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class CASES(NetSuite):
    keys = {
        "p_key": ["CASE_ID"],
        "rank_key": ["CASE_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    query = """
        SELECT
            SUPPORT_INCIDENTS.ASSIGNED_ID,
            SUPPORT_INCIDENTS.CASE_ID,
            SUPPORT_INCIDENTS.CASE_NUMBER,
            SUPPORT_INCIDENTS.CI_THIN AS CAI_THIEN,
            SUPPORT_INCIDENTS.CREATE_DATE,
            SUPPORT_INCIDENTS.L_DO_CHO_S_IM AS LY_DO_CHO_SO_DIEM,
            SUPPORT_INCIDENTS.L_DO_KHNG_MUA_HNG_ID AS LY_DO_KHONG_MUA_HANG,
            SUPPORT_INCIDENTS.NAME,
            SUPPORT_INCIDENTS.NV_BN_HNG_ID,
            SUPPORT_INCIDENTS.NV_BO_HNH_M_ID AS BAOHANH_ID,
            SUPPORT_INCIDENTS.NV_GIAO_HNG_ID AS NVGH_ID,
            SUPPORT_INCIDENTS.NV_H_TR_GIAO_HNG_ID AS HOTRO_ID,
            SUPPORT_INCIDENTS.RATING_KHCH_VO_CH_CHA_MUA__ID,
            SUPPORT_INCIDENTS.RATING__KHCH_DIGITALHOTLINE_ID,
            SUPPORT_INCIDENTS.RATING__SN_PHM_A_DNG_PHON_ID,
            SUPPORT_INCIDENTS.RATING__SAU_KHI_GIAO_HNG_ID,
            SUPPORT_INCIDENTS.RATING__SAU_KHI_LN_SO_ID,
            SUPPORT_INCIDENTS.SAO_CHO_NVBH_ID,
            SUPPORT_INCIDENTS.SAO_CHO_NVGH_ID,
            SUPPORT_INCIDENTS.SAO_CHO_NV_BO_HNH_M_ID,
            SUPPORT_INCIDENTS.SAO_CHO_NV_H_TR_ID,
            SUPPORT_INCIDENTS.SO_LOCATION_ID,
            SUPPORT_INCIDENTS.SO_REFERENCE_ID,
            SUPPORT_INCIDENTS.NGY_HON_THNH_KHIU_NIBO_,
            SUPPORT_INCIDENTS.CASE_TYPE_ID,
            SUPPORT_INCIDENTS.NPS_SCORE_ID,
            SUPPORT_INCIDENTS.CASE_ORIGIN_ID,
            SUPPORT_INCIDENTS.DATE_LAST_MODIFIED
        FROM
            "Vua Nem Joint Stock Company".Administrator.SUPPORT_INCIDENTS
        WHERE
            SUPPORT_INCIDENTS.DATE_LAST_MODIFIED >= '{{ start }}'
            AND SUPPORT_INCIDENTS.DATE_LAST_MODIFIED <= '{{ end }}'
    """
    schema = [
        {"name": "ASSIGNED_ID", "type": "INTEGER"},
        {"name": "CASE_ID", "type": "INTEGER"},
        {"name": "CASE_NUMBER", "type": "INTEGER"},
        {"name": "CAI_THIEN", "type": "STRING"},
        {"name": "CREATE_DATE", "type": "TIMESTAMP"},
        {"name": "LY_DO_CHO_SO_DIEM", "type": "STRING"},
        {"name": "LY_DO_KHONG_MUA_HANG", "type": "INTEGER"},
        {"name": "NAME", "type": "STRING"},
        {"name": "NV_BN_HNG_ID", "type": "INTEGER"},
        {"name": "BAOHANH_ID", "type": "INTEGER"},
        {"name": "NVGH_ID", "type": "INTEGER"},
        {"name": "HOTRO_ID", "type": "INTEGER"},
        {"name": "RATING_KHCH_VO_CH_CHA_MUA__ID", "type": "INTEGER"},
        {"name": "RATING__KHCH_DIGITALHOTLINE_ID", "type": "INTEGER"},
        {"name": "RATING__SN_PHM_A_DNG_PHON_ID", "type": "INTEGER"},
        {"name": "RATING__SAU_KHI_GIAO_HNG_ID", "type": "INTEGER"},
        {"name": "RATING__SAU_KHI_LN_SO_ID", "type": "INTEGER"},
        {"name": "SAO_CHO_NVBH_ID", "type": "INTEGER"},
        {"name": "SAO_CHO_NVGH_ID", "type": "INTEGER"},
        {"name": "SAO_CHO_NV_BO_HNH_M_ID", "type": "INTEGER"},
        {"name": "SAO_CHO_NV_H_TR_ID", "type": "INTEGER"},
        {"name": "SO_LOCATION_ID", "type": "INTEGER"},
        {"name": "SO_REFERENCE_ID", "type": "INTEGER"},
        {"name": "NGY_HON_THNH_KHIU_NIBO_", "type": "TIMESTAMP"},
        {"name": "CASE_TYPE_ID", "type": "INTEGER"},
        {"name": "NPS_SCORE_ID", "type": "INTEGER"},
        {"name": "CASE_ORIGIN_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
    ]
    columns = [
        Column("ASSIGNED_ID", Integer),
        Column("CASE_ID", Integer),
        Column("CASE_NUMBER", Integer),
        Column("CAI_THIEN", String),
        Column("CREATE_DATE", DateTime(timezone=True)),
        Column("LY_DO_CHO_SO_DIEM", String),
        Column("LY_DO_KHONG_MUA_HANG", Integer),
        Column("NAME", String),
        Column("NV_BN_HNG_ID", Integer),
        Column("BAOHANH_ID", Integer),
        Column("NVGH_ID", Integer),
        Column("HOTRO_ID", Integer),
        Column("RATING_KHCH_VO_CH_CHA_MUA__ID", Integer),
        Column("RATING__KHCH_DIGITALHOTLINE_ID", Integer),
        Column("RATING__SN_PHM_A_DNG_PHON_ID", Integer),
        Column("RATING__SAU_KHI_GIAO_HNG_ID", Integer),
        Column("RATING__SAU_KHI_LN_SO_ID", Integer),
        Column("SAO_CHO_NVBH_ID", Integer),
        Column("SAO_CHO_NVGH_ID", Integer),
        Column("SAO_CHO_NV_BO_HNH_M_ID", Integer),
        Column("SAO_CHO_NV_H_TR_ID", Integer),
        Column("SO_LOCATION_ID", Integer),
        Column("SO_REFERENCE_ID", Integer),
        Column("NGY_HON_THNH_KHIU_NIBO_", DateTime(timezone=True)),
        Column("CASE_TYPE_ID", Integer),
        Column("NPS_SCORE_ID", Integer),
        Column("CASE_ORIGIN_ID", Integer),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
