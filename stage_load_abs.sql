-- stage_load_abs.sql — ABSOLUTE PATHS

\copy st_patients      FROM '/Users/Naylanocera/soen363/csvs/files/PATIENTS.csv'        WITH (FORMAT csv, HEADER true);
\copy st_admissions    FROM '/Users/Naylanocera/soen363/csvs/files/ADMISSIONS.csv'      WITH (FORMAT csv, HEADER true);
\copy st_icustays      FROM '/Users/Naylanocera/soen363/csvs/files/ICUSTAYS.csv'        WITH (FORMAT csv, HEADER true);
\copy st_d_icd_diag    FROM '/Users/Naylanocera/soen363/csvs/files/D_ICD_DIAGNOSES.csv' WITH (FORMAT csv, HEADER true);
\copy st_diagnoses_icd FROM '/Users/Naylanocera/soen363/csvs/files/DIAGNOSES_ICD.csv'   WITH (FORMAT csv, HEADER true);
\copy st_noteevents    FROM '/Users/Naylanocera/soen363/csvs/files/NOTEEVENTS.csv'      WITH (FORMAT csv, HEADER true);