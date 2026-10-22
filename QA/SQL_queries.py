

RULE_QUERIES = {

    "RULE_1": {
        "kpi": "Completeness_HCP_PROFILE",
        "column": "SOURCE_ID",
        "task" : "Missing SOURCE_ID (NULL or blank) from both HCP Profile tables",
        "query": r""" select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL p
        where (source_id is NULL or TRIM(source_id)='')
        union
        select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL p
        where (source_id is NULL or TRIM(source_id)='');
        """

        },
    "RULE_2": {
        "kpi": "Completeness_HCP_PROFILE",
        "column": "SOURCE_CD",
        "task" : "Missing SOURCE_CD (NULL or blank) from both HCP Profile tables",
        "query" : r"""
        select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL p 
        where (source_cd is NULL or TRIM(source_cd)='')
        union
        select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL p 
        where (source_cd is NULL or TRIM(source_cd)='');"""
    },
    
    "RULE_3": {
        "kpi": "Completeness_HCP_PROFILE",
        "column": "FIRST_NAME",
        "task" : " Missing FIRST_NAME (NULL or blank) from both HCP Profile tables",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL p 
where (first_name is NULL or TRIM(first_name)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL p 
where (first_name is NULL or TRIM(first_name)='');
"""
    },

    "RULE_4": {
        "kpi": "Completeness_HCP_PROFILE",
        "column": "LAST_NAME",
        "task" : "Missing LAST_NAME (NULL or blank) from both HCP Profile tables",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL p 
where (last_name is NULL or TRIM(last_name)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL p 
where (last_name is NULL or TRIM(last_name)='');
"""
    },

    "RULE_5": {
        "kpi": "Completeness_HCP_ADDRESS",
        "column": "SOURCE_ID",
        "task" : "Missing SOURCE_ID (NULL or blank) from both HCP Address tables",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL
where (source_id is NULL or TRIM(source_id)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL
where (source_id is NULL or TRIM(source_id)='');
"""
    },

    "RULE_6": {
        "kpi": "Completeness_HCP_ADDRESS",
        "column": "SOURCE_CD",
        "task" : "missing SOURCE_CD (NULL or blank) from both HCP Address table",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL
where (source_cd is NULL or TRIM(source_cd)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL
where (source_cd is NULL or TRIM(source_cd)='');
"""
    },

    "RULE_7": {
        "kpi": "Completeness_HCP_ADDRESS",
        "column": "ADDRESS_ID",
        "task" : "missing ADDRESS_ID (NULL or blank) from both HCP Address tables",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL
where (address_id is NULL or TRIM(address_id)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL
where (address_id is NULL or TRIM(address_id)='');
"""
    },

    "RULE_8": {
        "kpi": "Completeness_HCP_ADDRESS",
        "column": "CITY_STATE_ZIP",
        "task" : "missing address information (CITY, STATE, and/or ZIP) from HCP Address tables",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL
WHERE (City IS NULL or TRIM(City)='') and (State IS NULL or TRIM(State)='')
union 
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL
WHERE (zip IS NULL or TRIM(zip)='') and (State IS NULL or TRIM(State)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL
WHERE (City IS NULL or TRIM(City)='') and (State IS NULL or TRIM(State)='') and (zip IS NULL or TRIM(zip)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL
WHERE (zip IS NULL or TRIM(zip)='') and (State IS NULL or TRIM(State)='');
"""
    },

    "RULE_9": {
        "kpi": "Completeness_HCP_LICENSE",
        "column": "SOURCE_ID",
        "task": "Missing SOURCE_ID (NULL or blank) from both HCP License tables",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL
where (source_id is NULL or TRIM(source_id)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL
where (source_id is NULL or TRIM(source_id)='');
"""
    },

    "RULE_10": {
        "kpi": "Completeness_HCP_LICENSE",
        "column": "SOURCE_CD",
        "task": "Missing SOURCE_CD (NULL or blank) from both HCP License tables",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL
where (source_cd is NULL or TRIM(source_cd)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL
where (source_cd is NULL or TRIM(source_cd)='');
"""
    },

    "RULE_11": {
        "kpi": "Completeness_HCP_LICENSE",
        "column": "LICENSE_ID",
        "task": "Missing LICENSE_ID (NULL or blank) from both HCP License tables",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL
where (PRMRY_KEY is NULL or TRIM(PRMRY_KEY)='')
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL
where (PRMRY_KEY is NULL or TRIM(PRMRY_KEY)='');
"""
    },

    "RULE_12": {
        "kpi": "Completeness_HCO_PROFILE",
        "column": "SOURCE_ID",
        "task": "Missing SOURCE_ID (NULL or blank) from HCO Profile table",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL
where (source_id is NULL or TRIM(source_id)='');
"""
    },

    "RULE_13": {
        "kpi": "Completeness_HCO_PROFILE",
        "column": "SOURCE_CD",
        "task": "Missing SOURCE_CD (NULL or blank) from HCO Profile table",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL
where (source_cd is NULL or TRIM(source_cd)='');
"""
    },

    "RULE_14": {
        "kpi": "Completeness_HCO_PROFILE",
        "column": "ORGNAME",
        "task": "Missing ORGNAME (NULL or blank) from HCO Profile table",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL
where (orgname is NULL or TRIM(orgname)='');
"""
    },

    "RULE_15": {
        "kpi": "Completeness_HCO_ADDRESS",
        "column": "CITY_STATE_ZIP",
        "task": "Missing address information (CITY, STATE, and/or ZIP) from HCO Address table",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL
WHERE (City IS NULL or TRIM(City)='') and (State IS NULL or TRIM(State)='')
union 
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL
WHERE (zip IS NULL or TRIM(zip)='') and (State IS NULL or TRIM(State)='');
"""
    },

    "RULE_16": {
        "kpi": "Completeness_HCO_LICENSE",
        "column": "SOURCE_ID",
        "task": "Missing SOURCE_ID (NULL or blank) from HCO License table",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL
where (source_id is NULL or TRIM(source_id)='');
"""
    },

    "RULE_17": {
        "kpi": "Completeness_HCO_LICENSE",
        "column": "SOURCE_CD",
        "task": "Missing SOURCE_CD (NULL or blank) from HCO License table",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL
where (source_cd is NULL or TRIM(source_cd)='');
"""
    },

    "RULE_18": {
        "kpi": "Completeness_HCO_LICENSE",
        "column": "LICENSE_ID",
        "task": "Missing LICENSE_ID (NULL or blank) from HCO License table",
        "query": r"""
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL
where (PRMRY_KEY is NULL or TRIM(PRMRY_KEY)='');
"""
    },

    "RULE_19": {
        "kpi": "Uniqueness_HCP_PROFILE",
        "column": "SOURCE_ID",
        "task": "Duplicate SOURCE_ID and SOURCE_CD combinations in HCP Profile tables",
        "query": r"""
select source_id, source_cd, count(1) 
from {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL p 
group by source_id, source_cd 
having count(1) > 1 
union
select source_id, source_cd, count(1) 
from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL p 
group by source_id, source_cd 
having count(1) > 1;
"""
    },

    "RULE_20": {
        "kpi": "Uniqueness_HCP_ADDRESS",
        "column": "SOURCE_ID",
        "task": "Duplicate SOURCE_ID, SOURCE_CD, and ADDRESS_ID combinations in HCP Address tables",
        "query": r"""
select source_id, source_cd, address_id, count(1) 
from {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL p 
group by source_id, source_cd, address_id
having count(1) > 1 
union
select source_id, source_cd, address_id, count(1) 
from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL p 
group by source_id, source_cd, address_id
having count(1) > 1;
"""
    },

    "RULE_21": {
        "kpi": "Uniqueness_HCP_LICENSE",
        "column": "SOURCE_ID",
        "task": "Duplicate SOURCE_ID, SOURCE_CD, and LICENSE_ID combinations in HCP License tables",
        "query": r"""
select source_id, source_cd, prmry_key, count(1) 
from {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL p 
group by source_id, source_cd, prmry_key
having count(1) > 1 
union
select source_id, source_cd, prmry_key, count(1) 
from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL p 
group by source_id, source_cd, prmry_key
having count(1) > 1;
"""
    },

    "RULE_22": {
        "kpi": "Uniqueness_HCO_PROFILE",
        "column": "SOURCE_ID",
        "task": "Duplicate SOURCE_ID and SOURCE_CD combinations in HCO Profile table",
        "query": r"""
select source_id, source_cd, count(1) 
from {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL p 
group by source_id, source_cd 
having count(1) > 1;
"""
    },

    "RULE_23": {
        "kpi": "Uniqueness_HCO_ADDRESS",
        "column": "SOURCE_ID",
        "task": "Duplicate SOURCE_ID, SOURCE_CD, and ADDRESS_ID combinations in HCO Address table",
        "query": r"""
select source_id, source_cd, address_id, count(1) 
from {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL p 
group by source_id, source_cd, address_id
having count(1) > 1;
"""
    },

    "RULE_24": {
        "kpi": "Uniqueness_HCO_LICENSE",
        "column": "SOURCE_ID",
        "task": "Duplicate SOURCE_ID, SOURCE_CD, and LICENSE_ID combinations in HCO License table",
        "query": r"""
select source_id, source_cd, prmry_key, count(1) 
from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL p 
group by source_id, source_cd, prmry_key
having count(1) > 1;
"""
    },

    "RULE_25": {
        "kpi": "Conformity_HCP_PROFILE",
        "column": "NPI",
        "task": "Invalid NPI format in HCP Profile tables",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL a
WHERE (NPI is not null and trim(NPI) <> '')
and (length(NPI) <> 10 or NOT REGEXP_LIKE(NPI, '^[0-9]{10}$'))
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL a
WHERE (NPI is not null and trim(NPI) <> '')
and (length(NPI) <> 10 or NOT REGEXP_LIKE(NPI, '^[0-9]{10}$'));
"""
    },

    "RULE_26": {
        "kpi": "Conformity_HCP_PROFILE",
        "column": "EMAIL",
        "task": "Invalid EMAIL format in HCP Profile tables",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL a
WHERE (EMAIL is not null and trim(EMAIL) <> '')
AND NOT REGEXP_LIKE(
        TRIM(EMAIL),
        '^([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|(https?:\/\/)?(www\.)?[A-Z0-9.-]+\.[A-Z]{2,}(\/.*)?)$',
        'i'
      )
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL a
WHERE (EMAIL is not null and trim(EMAIL) <> '')
AND NOT REGEXP_LIKE(
        TRIM(EMAIL),
        '^([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|(https?:\/\/)?(www\.)?[A-Z0-9.-]+\.[A-Z]{2,}(\/.*)?)$',
        'i'
      );
"""
    },

    "RULE_27": {
        "kpi": "Conformity_HCP_PROFILE",
        "column": "PHONE_NUMBER",
        "task": "Invalid PHONE_NUMBER format in HCP Profile tables",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL a
WHERE (PHONE_NUMBER is not null and trim(PHONE_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(PHONE_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      )
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL a
WHERE (PHONE_NUMBER is not null and trim(PHONE_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(PHONE_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_28": {
        "kpi": "Conformity_HCP_PROFILE",
        "column": "FAX_NUMBER",
        "task": "Invalid FAX_NUMBER format in HCP Profile tables",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL a
WHERE (FAX_NUMBER is not null and trim(FAX_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(FAX_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      )
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL a
WHERE (FAX_NUMBER is not null and trim(FAX_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(FAX_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_29": {
        "kpi": "Conformity_HCP_ADDRESS",
        "column": "STATE",
        "task": "Invalid STATE code in HCP Address tables",
        "query": r"""
WITH ref AS (
    SELECT state_cd
    FROM phcdw_dev.stg.lkp_state_cd 
)
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL a
WHERE (STATE is not null and trim(STATE) <> '')
AND NOT EXISTS (
    SELECT 1 FROM ref r WHERE upper(r.state_cd) = upper(a.STATE)
)
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL a
WHERE (STATE is not null and trim(STATE) <> '')
AND NOT EXISTS (
    SELECT 1 FROM ref r WHERE upper(r.state_cd) = upper(a.STATE)
);
"""
    },

    "RULE_30": {
        "kpi": "Conformity_HCP_ADDRESS",
        "column": "COUNTRY_CODE",
        "task": "Invalid COUNTRY_CODE value in HCP Address tables",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL a
WHERE (COUNTRY_CODE is not null and trim(COUNTRY_CODE) <> '')
AND REPLACE(REPLACE(UPPER(TRIM(country_code)), '.', ''), ' ', '') 
      NOT IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', 'AMERICA')
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL a
WHERE (COUNTRY_CODE is not null and trim(COUNTRY_CODE) <> '')
AND REPLACE(REPLACE(UPPER(TRIM(country_code)), '.', ''), ' ', '') 
      NOT IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', 'AMERICA');
"""
    },

    "RULE_31": {
        "kpi": "Conformity_HCP_ADDRESS",
        "column": "PHONE_NUMBER1",
        "task": "Invalid PHONE_NUMBER1 format in HCP Address tables",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL a
WHERE (PHONE_NUMBER1 is not null and trim(PHONE_NUMBER1) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(PHONE_NUMBER1, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      )
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL a
WHERE (PHONE_NUMBER1 is not null and trim(PHONE_NUMBER1) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(PHONE_NUMBER1, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_32": {
        "kpi": "Conformity_HCP_ADDRESS",
        "column": "PHONE_NUMBER2",
        "task": "Invalid FAX_NUMBER format in HCP Address tables",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL a
WHERE (PHONE_NUMBER2 is not null and trim(PHONE_NUMBER2) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(PHONE_NUMBER2, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      )
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL a
WHERE (PHONE_NUMBER2 is not null and trim(PHONE_NUMBER2) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(PHONE_NUMBER2, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_33": {
        "kpi": "Conformity_HCP_ADDRESS",
        "column": "FAX_NUMBER",
        "task": "Invalid FAX_NUMBER format in HCP Address tables",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL a
WHERE (FAX_NUMBER is not null and trim(FAX_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(FAX_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      )
UNION
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL a
WHERE (FAX_NUMBER is not null and trim(FAX_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(FAX_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_34": {
        "kpi": "Conformity_HCP_LICENSE",
        "column": "IDENTIFIER_VALUE",
        "task": "Invalid license identifier format in HCP License tables",
        "query": r"""
select * from 
(
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'NPI'
and (length(IDENTIFIER_VALUE) <> 10 or NOT REGEXP_LIKE(IDENTIFIER_VALUE, '^[0-9]{10}$'))
union 
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'DEA'
and (length(IDENTIFIER_VALUE) <> 9 or 
NOT REGEXP_LIKE(
        REPLACE(UPPER(TRIM(IDENTIFIER_VALUE)), '-', ''),
        '^[A-Z]{2}[0-9]{7}$'
  )
)
union
select * from {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE in ('SLN','ST_LIC','LIC_NUM')
and NOT REGEXP_LIKE(
        REPLACE(REPLACE(UPPER(TRIM(IDENTIFIER_VALUE)), '-', ''), ' ', ''),
        '^[A-Z0-9]{5,15}$'
)
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'NPI'
and (length(IDENTIFIER_VALUE) <> 10 or NOT REGEXP_LIKE(IDENTIFIER_VALUE, '^[0-9]{10}$'))
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'DEA'
and (length(IDENTIFIER_VALUE) <> 9 or 
NOT REGEXP_LIKE(
        REPLACE(UPPER(TRIM(IDENTIFIER_VALUE)), '-', ''),
        '^[A-Z]{2}[0-9]{7}$'
  )
)
union
select * from {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE in ('SLN','ST_LIC','LIC_NUM')
and NOT REGEXP_LIKE(
        REPLACE(REPLACE(UPPER(TRIM(IDENTIFIER_VALUE)), '-', ''), ' ', ''),
        '^[A-Z0-9]{5,15}$'
)
)
order by source_cd, identifier_type;
"""
    },

    "RULE_35": {
        "kpi": "Conformity_HCO_PROFILE",
        "column": "ORG_EMAIL_ADDR",
        "task": "Invalid ORG_EMAIL_ADDR format in HCO Profile table",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL a
WHERE (ORG_EMAIL_ADDR is not null and trim(ORG_EMAIL_ADDR) <> '')
AND NOT REGEXP_LIKE(
        TRIM(ORG_EMAIL_ADDR),
        '^([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|(https?:\/\/)?(www\.)?[A-Z0-9.-]+\.[A-Z]{2,}(\/.*)?)$',
        'i'
      );
"""
    },

    "RULE_36": {
        "kpi": "Conformity_HCO_PROFILE",
        "column": "ORG_PHONE_NUMBER",
        "task": "Invalid ORG_PHONE_NUMBER format in HCO Profile table",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL a
WHERE (ORG_PHONE_NUMBER is not null and trim(ORG_PHONE_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(ORG_PHONE_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_37": {
        "kpi": "Conformity_HCO_PROFILE",
        "column": "ORG_FAX_NUMBER",
        "task": "Invalid ORG_FAX_NUMBER format in HCO Profile table",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL a
WHERE (ORG_FAX_NUMBER is not null and trim(ORG_FAX_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(ORG_FAX_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_38": {
        "kpi": "Conformity_HCO_ADDRESS",
        "column": "STATE",
        "task": "Invalid STATE code in HCO Address table",
        "query": r"""
WITH ref AS (
    SELECT state_cd
    FROM phcdw_dev.stg.lkp_state_cd
)
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL a
WHERE (STATE is not null and trim(STATE) <> '')
AND NOT EXISTS (
    SELECT 1 FROM ref r WHERE upper(r.state_cd) = upper(a.STATE)
);
"""
    },

    "RULE_39": {
        "kpi": "Conformity_HCO_ADDRESS",
        "column": "COUNTRY_CODE",
        "task": "Invalid COUNTRY_CODE value in HCO Address table",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL a
WHERE (COUNTRY_CODE is not null and trim(COUNTRY_CODE) <> '')
AND REPLACE(REPLACE(UPPER(TRIM(country_code)), '.', ''), ' ', '') 
      NOT IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', 'AMERICA');
"""
    },

    "RULE_40": {
        "kpi": "Conformity_HCO_ADDRESS",
        "column": "PHONE_NUMBER1",
        "task": "Invalid PHONE_NUMBER1 format in HCO Address table",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL a
WHERE (PHONE_NUMBER1 is not null and trim(PHONE_NUMBER1) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(PHONE_NUMBER1, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_41": {
        "kpi": "Conformity_HCO_ADDRESS",
        "column": "PHONE_NUMBER2",
        "task": "Invalid PHONE_NUMBER2 format in HCO Address table",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL a
WHERE (PHONE_NUMBER2 is not null and trim(PHONE_NUMBER2) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(PHONE_NUMBER2, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_42": {
        "kpi": "Conformity_HCO_ADDRESS",
        "column": "FAX_NUMBER",
        "task": "Invalid FAX_NUMBER format in HCO Address table",
        "query": r"""
SELECT *
FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL a
WHERE (FAX_NUMBER is not null and trim(FAX_NUMBER) <> '')
AND NOT REGEXP_LIKE(
        REGEXP_REPLACE(FAX_NUMBER, '[^0-9]', ''),
        '^(1)?[2-9][0-9]{2}[2-9][0-9]{2}[0-9]{4}$'
      );
"""
    },

    "RULE_43": {
        "kpi": "Conformity_HCO_LICENSE",
        "column": "IDENTIFIER_VALUE",
        "task": "Invalid license identifier format in HCO License table",
        "query": r"""
select * from 
(
select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL a
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'NPI'
and (length(IDENTIFIER_VALUE) <> 10 or NOT REGEXP_LIKE(IDENTIFIER_VALUE, '^[0-9]{10}$'))

union 

select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL a
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'DEA'
and (length(IDENTIFIER_VALUE) <> 9 or 
NOT REGEXP_LIKE(
        REPLACE(UPPER(TRIM(IDENTIFIER_VALUE)), '-', ''),
        '^[A-Z]{2}[0-9]{7}$'
  )
)

union 

select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL a
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'HIN'
and (length(IDENTIFIER_VALUE) <> 9 or 
NOT REGEXP_LIKE(
        REPLACE(UPPER(TRIM(IDENTIFIER_VALUE)), '-', ''),
        '^[A-Z0-9]{9}$'
)
)

union 

select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL a
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'HIN'
and (length(IDENTIFIER_VALUE) <> 7 or 
NOT REGEXP_LIKE(
        REPLACE(UPPER(TRIM(IDENTIFIER_VALUE)), '-', ''),
        '^[A-Z0-9]{7}$'
)
)

union 

select * from {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL a
where (IDENTIFIER_TYPE is not null and trim(IDENTIFIER_TYPE) <> '')
and IDENTIFIER_TYPE = 'ST_LIC'
and NOT REGEXP_LIKE(
        REPLACE(REPLACE(UPPER(TRIM(IDENTIFIER_VALUE)), '-', ''), ' ', ''),
        '^[A-Z0-9]{5,15}$'
)
)
order by source_cd, identifier_type;
"""
    },

    "RULE_44": {
        "kpi": "Consistency_HCP_LICENSE",
        "column": "IDENTIFIER_TYPE, IDENTIFIER_VALUE",
        "task": "Duplicate IDENTIFIER_VALUE mapped to multiple IDENTIFIER_TYPE values",
        "query": r"""
        SELECT SOURCE_ID, IDENTIFIER_VALUE, SOURCE_CD
        FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL
        WHERE IDENTIFIER_VALUE IS NOT NULL
        GROUP BY SOURCE_ID, IDENTIFIER_VALUE
        HAVING COUNT(DISTINCT IDENTIFIER_TYPE) > 1

        UNION
        SELECT SOURCE_ID, IDENTIFIER_VALUE, SOURCE_CD
        FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL
        WHERE IDENTIFIER_VALUE IS NOT NULL
        GROUP BY SOURCE_ID, IDENTIFIER_VALUE
        HAVING COUNT(DISTINCT IDENTIFIER_TYPE) > 1;
    """
    },
    "RULE_45": {
    "kpi": "Consistency_HCP_ADDRESS",
    "column": "STATE, ZIP, CITY",
    "task": "Same ZIP associated with multiple cities",
    "query": r"""
    SELECT SOURCE_ID,SOURCE_CD,ZIP
    FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL
    WHERE ZIP IS NOT NULL
    GROUP BY ZIP
    HAVING COUNT(DISTINCT CITY) > 1

    UNION

    SELECT SOURCE_ID,SOURCE_CD,ZIP
    FROM cph_db_dev.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL
    WHERE ZIP IS NOT NULL
    GROUP BY ZIP
    HAVING COUNT(DISTINCT CITY) > 1;
    """
    },

    "RULE_46": {
    "kpi": "Consistency_HCP_ADDRESS",
    "column": "STATE, ZIP",
    "task": "Same ZIP associated with multiple states",
    "query": r"""
    SELECT ZIP, SOURCE_ID,SOURCE_CD
    FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL
    WHERE ZIP IS NOT NULL
    GROUP BY ZIP
    HAVING COUNT(DISTINCT STATE) > 1

    UNION

    SELECT ZIP, SOURCE_ID,SOURCE_CD
    FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_ADDR_DT_FULL
    WHERE ZIP IS NOT NULL
    GROUP BY ZIP
    HAVING COUNT(DISTINCT STATE) > 1;
    """
    },

    "RULE_47": {
    "kpi": "Consistency_HCP_PROFILE",
    "column": "SOURCE_ID",
    "task": "Source ID exists in License but missing in Address",
    "query": r"""
    SELECT DISTINCT l.*
    FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL l
    WHERE EXISTS (
        SELECT 1
        FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL p
        WHERE p.SOURCE_ID = l.SOURCE_ID
    )
    AND NOT EXISTS (
        SELECT 1
        FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_ADDR_DATA_FULL a
        WHERE a.SOURCE_ID = l.SOURCE_ID
    );
    """
    },

    "RULE_49": {
    "kpi": "Consistency_HCO_LICENSE",
    "column": "IDENTIFIER_TYPE, IDENTIFIER_VALUE",
    "task": "Same IDENTIFIER_VALUE linked to multiple IDENTIFIER_TYPE values",
    "query": r"""
    # SELECT SOURCE_ID, IDENTIFIER_VALUE
    # FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL
    # WHERE IDENTIFIER_VALUE IS NOT NULL
    # GROUP BY SOURCE_ID, IDENTIFIER_VALUE
    # HAVING COUNT(DISTINCT IDENTIFIER_TYPE) > 1;
    # """
     },

    "RULE_50": {
    "kpi": "Consistency_HCP_LICENSE",
    "column": "IDENTIFIER_TYPE, IDENTIFIER_VALUE", 
    "task": "Same IDENTIFIER_TYPE mapped to multiple IDENTIFIER_VALUE records for a Source ID",
    "query": r"""
    SELECT SOURCE_ID, IDENTIFIER_TYPE,SOURCE_CD
    FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_LIC_DATA_FULL
    WHERE IDENTIFIER_TYPE IS NOT NULL
    GROUP BY SOURCE_ID, IDENTIFIER_TYPE
    HAVING COUNT(DISTINCT IDENTIFIER_VALUE) > 1

    UNION

    SELECT SOURCE_ID, IDENTIFIER_TYPE, SOURCE_CD
    FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_LIC_DT_FULL
    WHERE IDENTIFIER_TYPE IS NOT NULL
    GROUP BY SOURCE_ID, IDENTIFIER_TYPE
    HAVING COUNT(DISTINCT IDENTIFIER_VALUE) > 1;
    """
    },

    "RULE_51": {
        "kpi": "Consistency_HCP_PROFILE", 
        "column": "SOURCE_ID",
        "task": "Multiple Profile records for same SOURCE_ID",
        "query": r"""
        SELECT SOURCE_ID, SOURCE_CD
        FROM {__DB_NAME__}.MODEL_V2.MDS_HCP_PRFL_DATA_FULL
        GROUP BY SOURCE_ID
        HAVING COUNT(*) > 1

        UNION

        SELECT SOURCE_ID, SOURCE_CD
        FROM {__DB_NAME__}.MODEL_V2.MDM_NON_MDS_PRFL_DATA_FULL
        GROUP BY SOURCE_ID
        HAVING COUNT(*) > 1;
        """
    },
    
    "RULE_52": {
        "kpi": "Consistency_HCO_PROFILE",
        "column": "SOURCE_ID",
        "task": "Profile record exists without corresponding Address and License records",
        "query": r"""
        SELECT *
        FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL p
        WHERE NOT EXISTS (
            SELECT 1
            FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL a
            WHERE p.SOURCE_ID = a.SOURCE_ID
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL l
            WHERE p.SOURCE_ID = l.SOURCE_ID
        );
        """
},

    
    "RULE_53": {
        "kpi": "Consistency_HCO_ADDRESS",
        "column": "SOURCE_ID",
        "task": "Address record exists without corresponding Profile record",
        "query": r"""
        SELECT *
        FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_ADDR_DATA_FULL a
        WHERE NOT EXISTS (
            SELECT 1
            FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL p
            WHERE a.SOURCE_ID = p.SOURCE_ID);
            
        """
        },

    "RULE_54": {
        "kpi": "Consistency_HCO_LICENSE-1",
        "column": "SOURCE_ID",
        "task": "License record exists without corresponding Profile record",
        "query": r"""
        SELECT SOURCE_ID, SOURCE_CD
        FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL l
        WHERE NOT EXISTS (
            SELECT 1
            FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_PRFL_DATA_FULL p
            WHERE l.SOURCE_ID = p.SOURCE_ID);
        """},

    "RULE_55": {
        "kpi": "Consistency_HCO_LICENSE - 2",
        "column": "IDENTIFIER_TYPE, IDENTIFIER_VALUE",
        "task": "IDENTIFIER_TYPE is populated but IDENTIFIER_VALUE is missing",
        "query": r"""
        SELECT SOURCE_ID, IDENTIFIER_TYPE, SOURCE_CD
        FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL
        WHERE (IDENTIFIER_TYPE IS NOT NULL AND TRIM(IDENTIFIER_TYPE) <> '')
        AND (IDENTIFIER_VALUE IS NULL OR TRIM(IDENTIFIER_VALUE) = '');
        """
    },

    "RULE_56": {
        "kpi": "Consistency_HCO_LICENSE - 3",
        "column": "IDENTIFIER_VALUE, IDENTIFIER_TYPE",
        "task": "IDENTIFIER_VALUE is populated but IDENTIFIER_TYPE is missing",
        "query": r"""
        SELECT SOURCE_ID, IDENTIFIER_VALUE
        FROM {__DB_NAME__}.MODEL_V2.MDM_HCO_LIC_DATA_FULL
        WHERE (IDENTIFIER_VALUE IS NOT NULL AND TRIM(IDENTIFIER_VALUE) <> '')
        AND (IDENTIFIER_TYPE IS NULL OR TRIM(IDENTIFIER_TYPE) = '');
        """},
}


