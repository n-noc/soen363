-- load.sql
\i schema.sql
\i stage_schema.sql
\i stage_load_abs.sql
\i transform.sql


-- BEGIN;

-- \copy Patients(patient_id, date_of_birth, dob_privacy, gender, life_status) FROM 'csvs/files/PATIENTS.csv' WITH (FORMAT csv, HEADER true);
-- \copy Admission(admission_id, patient_id, admit_time, discharge_time, visit_type, insurance_plan, marital_status, arrival_source) FROM 'csvs/files/ADMISSIONS.csv' WITH (FORMAT csv, HEADER true);
-- \copy ICU_Stays(icu_stay_id, admission_id, icu_in_time, icu_out_time) FROM 'csvs/files/ICUSTAYS.csv' WITH (FORMAT csv, HEADER true);
-- \copy D_Diagnosis_ICD(icd_code, icd_version, long_title, valid_from, valid_to) FROM 'csvs/files/D_ICD_DIAGNOSES.csv' WITH (FORMAT csv, HEADER true);
-- \copy Diagnosis_ICD(diagnosis_id, admission_id, icd_code, icd_version, assigned_time, present_on_admission, sequence) FROM 'csvs/files/DIAGNOSES_ICD.csv' WITH (FORMAT csv, HEADER true);
-- \copy Note_Events(note_id, admission_id, icu_stay_id, author, note_type, note_time, has_error, note_text) FROM 'csvs/files/NOTEEVENTS.csv' WITH (FORMAT csv, HEADER true);

-- COMMIT;