-- Row counts
SELECT 'Patients' AS table, COUNT(*) FROM Patients
UNION ALL SELECT 'Admission', COUNT(*) FROM Admission
UNION ALL SELECT 'ICU_Stays', COUNT(*) FROM ICU_Stays
UNION ALL SELECT 'Note_Events', COUNT(*) FROM Note_Events
UNION ALL SELECT 'D_Diagnosis_ICD', COUNT(*) FROM D_Diagnosis_ICD
UNION ALL SELECT 'Diagnosis_ICD', COUNT(*) FROM Diagnosis_ICD;

-- Orphan checks
-- Admissions must have a patient
SELECT COUNT(*) AS orphan_admissions
FROM Admission a
LEFT JOIN Patients p ON p.patient_id = a.patient_id
WHERE p.patient_id IS NULL;

-- ICU stays must have an admission
SELECT COUNT(*) AS orphan_icu_stays
FROM ICU_Stays i
LEFT JOIN Admission a ON a.admission_id = i.admission_id
WHERE a.admission_id IS NULL;

-- Notes must have an admission
SELECT COUNT(*) AS orphan_notes_admission
FROM Note_Events n
LEFT JOIN Admission a ON a.admission_id = n.admission_id
WHERE a.admission_id IS NULL;