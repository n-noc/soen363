@echo off
REM Batch script to run each query and save to separate text files

SET PSQL="C:\Program Files\PostgreSQL\17\bin\psql.exe"
SET DB=hospital_database
SET USER=postgres
set PGPASSWORD=Poppy2003

echo Running Query 1...
%PSQL% -U %USER% -d %DB% -c "SELECT p.patient_id, p.date_of_birth, p.gender FROM Patients p;" -o query1_output.txt

echo Running Query 2...
%PSQL% -U %USER% -d %DB% -c "SELECT * FROM Admission WHERE patient_id = 123;" -o query2_output.txt

echo Running Query 3...
%PSQL% -U %USER% -d %DB% -c "SELECT patient_id, COUNT(*) AS admission_count FROM Admission GROUP BY patient_id;" -o query3_output.txt

echo Running Query 4...
%PSQL% -U %USER% -d %DB% -c "SELECT DISTINCT * FROM Patients p JOIN Admission a ON p.patient_id = a.patient_id WHERE a.discharge_time IS NOT NULL;" -o query4_output.txt

echo Running Query 5...
%PSQL% -U %USER% -d %DB% -c "SELECT DISTINCT p.* FROM Patients p JOIN Admission a ON p.patient_id = a.patient_id WHERE a.insurance_plan = 'Private';" -o query5_output.txt

echo Running Query 6...
%PSQL% -U %USER% -d %DB% -c "SELECT DISTINCT p.patient_id, a.admission_id, a.arrival_source AS from_location, a.discharge_location AS to_location FROM Admission a JOIN Patients p ON p.patient_id = a.patient_id WHERE a.arrival_source IS NOT NULL AND a.discharge_location IS NOT NULL AND LOWER(TRIM(a.arrival_source)) <> LOWER(TRIM(a.discharge_location)) ORDER BY p.patient_id, a.admission_id;" -o query6_output.txt

echo Running Query 7...
%PSQL% -U %USER% -d %DB% -c "SELECT a.patient_id, i.admission_id, COUNT(*) AS icu_stay_count FROM ICU_Stays i JOIN Admission a ON a.admission_id = i.admission_id GROUP BY a.patient_id, i.admission_id HAVING COUNT(*) > 1 ORDER BY icu_stay_count DESC, a.patient_id, i.admission_id;" -o query7_output.txt

echo Running Query 8...
%PSQL% -U %USER% -d %DB% -c "SELECT DISTINCT p.patient_id, i.icu_stay_id, i.first_careunit, i.last_careunit FROM ICU_Stays i JOIN Admission a ON a.admission_id = i.admission_id JOIN Patients p ON p.patient_id = a.patient_id WHERE i.first_careunit = 'MICU' AND i.last_careunit = 'MICU' ORDER BY p.patient_id, i.icu_stay_id;" -o query8_output.txt

echo Running Query 9...
%PSQL% -U %USER% -d %DB% -c "SELECT author, CASE WHEN has_error THEN 1 ELSE 0 END AS has_error, note_text FROM Note_Events WHERE admission_id = 163353 ORDER BY note_id;" -o query9_output.txt

echo Running Query 10...
%PSQL% -U %USER% -d %DB% -c "SELECT note_id, admission_id, author, note_text FROM Note_Events WHERE note_type = 'Discharge summary' LIMIT 10;" -o query10_output.txt

echo Running Query 11...
%PSQL% -U %USER% -d %DB% -c "SELECT admission_id, COUNT(*) AS note_count FROM Note_Events GROUP BY admission_id;" -o query11_output.txt

echo Running Query 12...
%PSQL% -U %USER% -d %DB% -c "SELECT d.diagnosis_id, d.admission_id, d.icd_code, d.icd_version, d.sequence, dict.long_title AS diagnosis_description FROM Diagnosis_ICD d JOIN D_Diagnosis_ICD dict ON d.icd_code = dict.icd_code AND d.icd_version = dict.icd_version JOIN Admission a ON d.admission_id = a.admission_id WHERE a.patient_id = 10006 ORDER BY d.sequence;" -o query12_output.txt

echo Running Query 13...
%PSQL% -U %USER% -d %DB% -c "SELECT d.icd_code, d.icd_version, dict.long_title AS diagnosis_description, COUNT(*) AS diagnosis_count FROM Diagnosis_ICD d JOIN D_Diagnosis_ICD dict ON d.icd_code = dict.icd_code AND d.icd_version = dict.icd_version GROUP BY d.icd_code, d.icd_version, dict.long_title ORDER BY diagnosis_count DESC LIMIT 5;" -o query13_output.txt

echo Running Query 14...
%PSQL% -U %USER% -d %DB% -c "SELECT DISTINCT a.admission_id, a.patient_id, a.admit_time, a.discharge_time, d.icd_code, dict.long_title AS diagnosis_description FROM Admission a JOIN ICU_Stays i ON a.admission_id = i.admission_id JOIN Diagnosis_ICD d ON a.admission_id = d.admission_id JOIN D_Diagnosis_ICD dict ON d.icd_code = dict.icd_code AND d.icd_version = dict.icd_version WHERE d.icd_code = '4019' AND d.icd_version = '9' ORDER BY a.patient_id;" -o query14_output.txt

echo Running Query 15...
%PSQL% -U %USER% -d %DB% -c "SELECT DISTINCT p.* FROM Patients p JOIN Admission a ON p.patient_id = a.patient_id WHERE a.admission_id NOT IN (SELECT admission_id FROM ICU_Stays);" -o query15_output.txt

echo Running Query 16...
%PSQL% -U %USER% -d %DB% -c "SELECT p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time, COUNT(n.note_id) AS radiology_report_count FROM Patients p JOIN Admission a ON a.patient_id = p.patient_id JOIN Note_Events n ON n.admission_id = a.admission_id WHERE LOWER(n.note_type) = 'radiology' GROUP BY p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time ORDER BY p.patient_id, a.admission_id;" -o query16_output.txt

echo Running Query 17...
%PSQL% -U %USER% -d %DB% -c "SELECT DISTINCT p.* FROM Patients p JOIN Admission a ON a.patient_id = p.patient_id JOIN Note_Events n ON n.admission_id = a.admission_id WHERE n.note_type = 'Radiology' AND n.note_text ILIKE '%%CHEST%%' AND n.note_time >= a.admit_time AND (a.discharge_time IS NULL OR n.note_time <= a.discharge_time);" -o query17_output.txt

echo Running Query 18...
%PSQL% -U %USER% -d %DB% -c "SELECT p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time, a.discharge_time, n.note_type, n.author FROM Patients p JOIN Admission a ON a.patient_id = p.patient_id JOIN Note_Events n ON n.admission_id = a.admission_id WHERE LOWER(n.note_type) = 'discharge summary' ORDER BY p.patient_id, a.admission_id LIMIT 50;" -o query18_output.txt

echo Running Query 19...
%PSQL% -U %USER% -d %DB% -c "SELECT p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time, n.note_type, COUNT(n.note_id) AS report_count FROM Patients p JOIN Admission a ON a.patient_id = p.patient_id JOIN Note_Events n ON n.admission_id = a.admission_id WHERE LOWER(n.note_type) IN ('radiology', 'ecg') GROUP BY p.patient_id, p.date_of_birth, p.gender, a.admission_id, a.admit_time, n.note_type ORDER BY p.patient_id, a.admission_id, n.note_type;" -o query19_output.txt

echo Running Query 20...
%PSQL% -U %USER% -d %DB% -c "SELECT p.patient_id, p.date_of_birth, p.gender, COUNT(DISTINCT a.admission_id) AS total_admissions, COUNT(DISTINCT i.icu_stay_id) AS total_icu_stays, COUNT(DISTINCT d.diagnosis_id) AS total_diagnoses FROM Patients p LEFT JOIN Admission a ON a.patient_id = p.patient_id LEFT JOIN ICU_Stays i ON i.admission_id = a.admission_id LEFT JOIN Diagnosis_ICD d ON d.admission_id = a.admission_id GROUP BY p.patient_id, p.date_of_birth, p.gender ORDER BY p.patient_id;" -o query20_output.txt

echo.
echo All queries completed!
echo Output files: query1_output.txt through query20_output.txt
pause