from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "CASES",
    [
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
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            ASSIGNED_ID,
            CASE_ID,
            CASE_NUMBER,
            CI_THIN AS CAI_THIEN,
            CREATE_DATE,
            L_DO_CHO_S_IM AS LY_DO_CHO_SO_DIEM,
            L_DO_KHNG_MUA_HNG_ID AS LY_DO_KHONG_MUA_HANG,
            NAME,
            NV_BN_HNG_ID,
            NV_BO_HNH_M_ID AS BAOHANH_ID,
            NV_GIAO_HNG_ID AS NVGH_ID,
            NV_H_TR_GIAO_HNG_ID AS HOTRO_ID,
            RATING_KHCH_VO_CH_CHA_MUA__ID,
            RATING__KHCH_DIGITALHOTLINE_ID,
            RATING__SN_PHM_A_DNG_PHON_ID,
            RATING__SAU_KHI_GIAO_HNG_ID,
            RATING__SAU_KHI_LN_SO_ID,
            SAO_CHO_NVBH_ID,
            SAO_CHO_NVGH_ID,
            SAO_CHO_NV_BO_HNH_M_ID,
            SAO_CHO_NV_H_TR_ID,
            SO_LOCATION_ID,
            SO_REFERENCE_ID,
            NGY_HON_THNH_KHIU_NIBO_,
            CASE_TYPE_ID,
            NPS_SCORE_ID,
            CASE_ORIGIN_ID,
            DATE_LAST_MODIFIED
        FROM
            "Vua Nem Joint Stock Company".Administrator.SUPPORT_INCIDENTS
        WHERE
            DATE_LAST_MODIFIED >= '{tr[0]}'
            AND DATE_LAST_MODIFIED <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["CASE_ID"],
        cursor_key=["DATE_LAST_MODIFIED"],
    ),
    load_callback_fn=update,
)
