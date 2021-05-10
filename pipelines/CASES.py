from pipelines import NetSuiteIncrementalJob


class CASES(NetSuiteIncrementalJob):
    p_key = ["CASE_ID"]
    incremental_key = "DATE_LAST_MODIFIED"
    partition_key = ["CREATE_DATE"]

    query = """
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
        CASE_ORIGIN_ID
    FROM "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".SUPPORT_INCIDENTS
    WHERE
        SUPPORT_INCIDENTS.DATE_LAST_MODIFIED >= ?
    """

    schema = [
        {"name": "ASSIGNED_ID", "type": "INTEGER"},
        {"name": "CASE_ID", "type": "INTEGER"},
        {"name": "CASE_NUMBER", "type": "INTEGER"},
        {"name": "CASE_NUMBERCODE", "type": "STRING"},
        {"name": "CASE_ORIGIN_ID", "type": "INTEGER"},
        {"name": "CASE_PROFILE_ID", "type": "INTEGER"},
        {"name": "CASE_TYPE_ID", "type": "INTEGER"},
        {"name": "CAI_THIEN", "type": "STRING"},
        {"name": "COMPANY_ID", "type": "INTEGER"},
        {"name": "CREATE_DATE", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "KTTV", "type": "FLOAT"},
        {"name": "LY_DO_CHO_DIEM", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "NPS_SCORE_ID", "type": "INTEGER"},
        {"name": "OWNER_ID", "type": "INTEGER"},
        {"name": "PRIORITY", "type": "STRING"},
        {"name": "SO_LOCATION_ID", "type": "INTEGER"},
        {"name": "SO_REFERENCE_ID", "type": "INTEGER"},
        {"name": "STAGE", "type": "STRING"},
        {"name": "STATUS", "type": "STRING"},
        {"name": "TYPES_NAME", "type": "STRING"},
        {"name": "ORIGINS_NAME", "type": "STRING"},
    ]
