\i '/Users/Naylanocera/soen363/vars.sql'

\copy st_patients      FROM :'csvdir'/PATIENTS.csv        WITH (FORMAT csv, HEADER true);
\copy st_admissions    FROM :'csvdir'/ADMISSIONS.csv      WITH (FORMAT csv, HEADER true);
\copy st_icustays      FROM :'csvdir'/ICUSTAYS.csv        WITH (FORMAT csv, HEADER true);
\copy st_d_icd_diag    FROM :'csvdir'/D_ICD_DIAGNOSES.csv WITH (FORMAT csv, HEADER true);
\copy st_diagnoses_icd FROM :'csvdir'/DIAGNOSES_ICD.csv   WITH (FORMAT csv, HEADER true);
\copy st_noteevents    FROM :'csvdir'/NOTEEVENTS.csv      WITH (FORMAT csv, HEADER true);