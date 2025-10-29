-- stage_schema.sql
DROP TABLE IF EXISTS st_patients      CASCADE;
DROP TABLE IF EXISTS st_admissions    CASCADE;
DROP TABLE IF EXISTS st_icustays      CASCADE;
DROP TABLE IF EXISTS st_d_icd_diag    CASCADE;
DROP TABLE IF EXISTS st_diagnoses_icd CASCADE;
DROP TABLE IF EXISTS st_noteevents    CASCADE;

CREATE TABLE st_patients (
  row_id INT, subject_id INT, gender TEXT, dob TIMESTAMP,
  dod TIMESTAMP, dod_hosp TIMESTAMP, dod_ssn TIMESTAMP, expire_flag INT
);

CREATE TABLE st_admissions (
  row_id INT, subject_id INT, hadm_id INT, admittime TIMESTAMP, dischtime TIMESTAMP,
  deathtime TIMESTAMP, admission_type TEXT, admission_location TEXT, discharge_location TEXT,
  insurance TEXT, language TEXT, religion TEXT, marital_status TEXT, ethnicity TEXT,
  edregtime TIMESTAMP, edouttime TIMESTAMP, diagnosis TEXT,
  hospital_expire_flag INT, has_chartevents_data INT
);

CREATE TABLE st_icustays (
  row_id INT, subject_id INT, hadm_id INT, icustay_id INT, dbsource TEXT,
  first_careunit TEXT, last_careunit TEXT, first_wardid INT, last_wardid INT,
  intime TIMESTAMP, outtime TIMESTAMP, los NUMERIC
);


CREATE TABLE st_d_icd_diag (
  row_id INT,
  icd9_code TEXT,
  short_title TEXT,
  long_title TEXT
);

CREATE TABLE st_diagnoses_icd (
  row_id INT, subject_id INT, hadm_id INT, seq_num INT, icd9_code TEXT
);

CREATE TABLE st_noteevents (
  row_id INT, subject_id INT, hadm_id INT, chartdate DATE, charttime TIMESTAMP,
  storetime TIMESTAMP, category TEXT, description TEXT, cgid INT, iserror TEXT, text TEXT
);