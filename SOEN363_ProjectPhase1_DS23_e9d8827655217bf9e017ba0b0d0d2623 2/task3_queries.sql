-- QUERY 1: List all patients (patient IDs) along with their date of birth and gender.
SELECT p.patient_id, p.date_of_birth, p.gender 
FROM Patients p;

-- QUERY 2: Find all admissions for a given patient (use a sample patient ID) showing all the information.
SELECT * 
FROM Admission 
WHERE patient_id = 123;

-- QUERY 3: Retrieve the number of admissions for each patient.
SELECT patient_id, COUNT(*) AS admission_count 
FROM Admission 
GROUP BY patient_id;

-- QUERY 4: Show all the patients who were discharged to "home".
SELECT DISTINCT * 
FROM Patients p 
JOIN Admission a ON p.patient_id = a.patient_id 
WHERE a.discharge_time IS NOT NULL;

-- QUERY 5: Show the list of all the patients who have private insurance.
SELECT DISTINCT p.* 
FROM Patients p 
JOIN Admission a ON p.patient_id = a.patient_id 
WHERE a.insurance_plan = 'Private';

-- QUERY 6: Show the list of all the patients who transferred from one location to another location in the hospital.
SELECT DISTINCT p.patient_id, a.admission_id, a.arrival_source AS from_location, a.discharge_location AS to_location 
FROM Admission a 
JOIN Patients p ON p.patient_id = a.patient_id 
WHERE a.arrival_source IS NOT NULL 
  AND a.discharge_location IS NOT NULL 
  AND LOWER(TRIM(a.arrival_source)) <> LOWER(TRIM(a.discharge_location)) 
ORDER BY p.patient_id, a.admission_id;

-- QUERY 7: Find patients who have more than one ICU stay in a single hospital admission.
SELECT a.patient_id, i.admission_id, COUNT(*) AS icu_stay_count 
FROM ICU_Stays i 
JOIN Admission a ON a.admission_id = i.admission_id 
GROUP BY a.patient_id, i.admission_id 
HAVING COUNT(*) > 1 
ORDER BY icu_stay_count DESC, a.patient_id, i.admission_id;

-- QUERY 8: List all the patients that were in the ICU and the first care unit, and the last care unit are "MICU".
SELECT DISTINCT p.patient_id, i.icu_stay_id, i.first_careunit, i.last_careunit 
FROM ICU_Stays i 
JOIN Admission a ON a.admission_id = i.admission_id 
JOIN Patients p ON p.patient_id = a.patient_id 
WHERE i.first_careunit = 'MICU' 
  AND i.last_careunit = 'MICU' 
ORDER BY p.patient_id, i.icu_stay_id;

-- QUERY 9: Retrieve all notes written for a specific admission ID, including the author and whether an error was flagged.
SELECT author, CASE WHEN has_error THEN 1 ELSE 0 END AS has_error, note_text 
FROM Note_Events 
WHERE admission_id = 163353 
ORDER BY note_id;

-- QUERY 10: Show the first 10 discharge summaries recorded in the database.
SELECT note_id, admission_id, author, note_text 
FROM Note_Events 
WHERE note_type = 'Discharge summary' 
LIMIT 10;

-- QUERY 11: Count the number of notes written per admission.
SELECT admission_id, COUNT(*) AS note_count 
FROM Note_Events 
GROUP BY admission_id;

-- QUERY 12: List all diagnoses (ICD codes) assigned to a given patient, including the textual description from the ICD dictionary.
SELECT d.diagnosis_id, d.admission_id, d.icd_code, d.icd_version, d.sequence, dict.long_title AS diagnosis_description 
FROM Diagnosis_ICD d 
JOIN D_Diagnosis_ICD dict ON d.icd_code = dict.icd_code AND d.icd_version = dict.icd_version 
JOIN Admission a ON d.admission_id = a.admission_id 
WHERE a.patient_id = 10006 
ORDER BY d.sequence;

-- QUERY 13: Find the top 5 most common diagnoses in the hospital database.
SELECT d.icd_code, d.icd_version, dict.long_title AS diagnosis_description, COUNT(*) AS diagnosis_count 
FROM Diagnosis_ICD d 
JOIN D_Diagnosis_ICD dict ON d.icd_code = dict.icd_code AND d.icd_version = dict.icd_version 
GROUP BY d.icd_code, d.icd_version, dict.long_title 
ORDER BY diagnosis_count DESC 
LIMIT 5;

-- QUERY 14: Retrieve all admissions that include an ICU stay and at least one diagnosis of "hypertension" (ICD-9 code: 4019).
SELECT DISTINCT a.admission_id, a.patient_id, a.admit_time, a.discharge_time, d.icd_code, dict.long_title AS diagnosis_description 
FROM Admission a 
JOIN ICU_Stays i ON a.admission_id = i.admission_id 
JOIN Diagnosis_ICD d ON a.admission_id = d.admission_id 
JOIN D_Diagnosis_ICD dict ON d.icd_code = dict.icd_code AND d.icd_version = dict.icd_version 
WHERE d.icd_code = '4019' 
  AND d.icd_version = '9' 
ORDER BY a.patient_id;

-- QUERY 15: Show all patients who have never been admitted to the ICU.
SELECT DISTINCT  p.*
FROM Patients p
JOIN Admission a ON p.patient_id = a.patient_id
WHERE a.admission_id NOT IN (
    SELECT admission_id 
    FROM ICU_Stays
);


-- QUERY 16: List of patients who had at least one radiology report during admission.
SELECT p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time, COUNT(n.note_id) AS radiology_report_count 
FROM Patients p 
JOIN Admission a ON a.patient_id = p.patient_id 
JOIN Note_Events n ON n.admission_id = a.admission_id 
WHERE LOWER(n.note_type) = 'radiology' 
GROUP BY p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time 
ORDER BY p.patient_id, a.admission_id;

-- QUERY 17: List of patients who had at least one radiology report from the chest on admission.
SELECT DISTINCT
    p.*
FROM Patients p
JOIN Admission a   ON a.patient_id = p.patient_id
JOIN Note_Events n ON n.admission_id = a.admission_id
WHERE n.note_type = 'Radiology' AND n.note_text ILIKE '%CHEST%'
AND n.note_time >= a.admit_time
AND (a.discharge_time IS NULL OR n.note_time <= a.discharge_time);



-- QUERY 18: List of patients with discharge summary report during the hospitalization.
SELECT p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time, a.discharge_time, n.note_type, n.author 
FROM Patients p 
JOIN Admission a ON a.patient_id = p.patient_id 
JOIN Note_Events n ON n.admission_id = a.admission_id 
WHERE LOWER(n.note_type) = 'discharge summary' 
ORDER BY p.patient_id, a.admission_id 
LIMIT 50;

-- QUERY 19: List of patients with radiology report or ECG report during the hospitalization.
SELECT p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time, n.note_type, COUNT(n.note_id) AS report_count 
FROM Patients p 
JOIN Admission a ON a.patient_id = p.patient_id 
JOIN Note_Events n ON n.admission_id = a.admission_id 
WHERE LOWER(n.note_type) IN ('radiology', 'ecg') 
GROUP BY p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time, n.note_type 
ORDER BY p.patient_id, a.admission_id, n.note_type;

-- QUERY 20: Generate a summary report showing for a patient, the total number of admissions, the number of ICU stays, and the number of diagnoses recorded.
SELECT p.patient_id, p.date_of_birth, p.gender, COUNT(DISTINCT a.admission_id) AS total_admissions, COUNT(DISTINCT i.icu_stay_id) AS total_icu_stays, COUNT(DISTINCT d.diagnosis_id) AS total_diagnoses 
FROM Patients p 
LEFT JOIN Admission a ON a.patient_id = p.patient_id 
LEFT JOIN ICU_Stays i ON i.admission_id = a.admission_id 
LEFT JOIN Diagnosis_ICD d ON d.admission_id = a.admission_id 
GROUP BY p.patient_id, p.date_of_birth, p.gender 
ORDER BY p.patient_id;