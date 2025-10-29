BEGIN;

-- 1) Patients
INSERT INTO patients (patient_id, date_of_birth, dob_privacy, gender, life_status)
SELECT p.subject_id,
       CAST(p.dob AS DATE),
       FALSE,
       p.gender,
       CASE WHEN p.dod IS NOT NULL THEN 'deceased' ELSE 'alive' END
FROM st_patients p
ON CONFLICT (patient_id) DO NOTHING;

-- 2) Admissions
INSERT INTO admission (admission_id, patient_id, admit_time, discharge_time,
                       visit_type, insurance_plan, marital_status, arrival_source)
SELECT a.hadm_id, a.subject_id, a.admittime, a.dischtime,
       a.admission_type, a.insurance, a.marital_status, a.admission_location
FROM st_admissions a
ON CONFLICT (admission_id) DO NOTHING;

-- ICU stays
INSERT INTO icu_stays (icu_stay_id, admission_id, icu_in_time, icu_out_time)
SELECT i.icustay_id, i.hadm_id, i.intime, i.outtime
FROM st_icustays i
ON CONFLICT (icu_stay_id) DO NOTHING;

-- ICD-9 dictionary (from D_ICD_DIAGNOSES) with normalization
INSERT INTO d_diagnosis_icd (icd_code, icd_version, long_title, valid_from, valid_to)
SELECT DISTINCT
  regexp_replace(d.icd9_code, '[^0-9A-Za-z]', '', 'g') AS icd_code,
  '9',
  d.long_title,
  NULL::DATE,
  NULL::DATE
FROM st_d_icd_diag d
WHERE d.icd9_code IS NOT NULL AND d.icd9_code <> ''
ON CONFLICT (icd_code, icd_version) DO NOTHING;

-- Backfill any missing codes found in diagnoses but absent in dictionary
INSERT INTO d_diagnosis_icd (icd_code, icd_version, long_title, valid_from, valid_to)
SELECT DISTINCT
  dx.code AS icd_code,
  '9'     AS icd_version,
  'Unknown (from diagnoses demo)'::text AS long_title,
  NULL::DATE,
  NULL::DATE
FROM (
  SELECT regexp_replace(icd9_code, '[^0-9A-Za-z]', '', 'g') AS code
  FROM st_diagnoses_icd
  WHERE icd9_code IS NOT NULL AND icd9_code <> ''
) dx
LEFT JOIN d_diagnosis_icd dict
  ON dict.icd_code = dx.code AND dict.icd_version = '9'
WHERE dict.icd_code IS NULL;

-- 3) Diagnoses (normalized)
INSERT INTO diagnosis_icd (
  diagnosis_id, admission_id, icd_code, icd_version,
  assigned_time, present_on_admission, sequence
)
SELECT
  dx.row_id,
  dx.hadm_id,
  regexp_replace(dx.icd9_code, '[^0-9A-Za-z]', '', 'g') AS icd_code,
  '9',
  NULL::TIMESTAMP,
  NULL::BOOLEAN,
  dx.seq_num
FROM st_diagnoses_icd dx
WHERE dx.icd9_code IS NOT NULL AND dx.icd9_code <> '';

-- 4) Note events
INSERT INTO note_events (note_id, admission_id, icu_stay_id, author, note_type, note_time, has_error, note_text)
SELECT n.row_id, n.hadm_id, NULL::INT,
       NULLIF(n.description,''),
       n.category,
       COALESCE(n.charttime, n.storetime),
       CASE WHEN COALESCE(n.iserror,'') IN ('1','t','T','true','TRUE','y','Y') THEN TRUE ELSE FALSE END,
       n.text
FROM st_noteevents n
WHERE n.hadm_id IS NOT NULL;

COMMIT;