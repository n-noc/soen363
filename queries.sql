
-- #6
SELECT DISTINCT
  p.patient_id,
  s.hadm_id       AS admission_id,
  s.admission_location AS from_location,
  s.discharge_location AS to_location
FROM st_admissions s
JOIN admission a ON a.admission_id = s.hadm_id
JOIN patients  p ON p.patient_id  = a.patient_id
WHERE s.admission_location IS NOT NULL
  AND s.discharge_location IS NOT NULL
  AND btrim(lower(s.admission_location)) <> btrim(lower(s.discharge_location))
ORDER BY p.patient_id, s.hadm_id;


-- #7
SELECT
  p.patient_id,
  i.admission_id,
  COUNT(*) AS icu_stay_count
FROM icu_stays i
JOIN admission a ON a.admission_id = i.admission_id
JOIN patients  p ON p.patient_id = a.patient_id
GROUP BY p.patient_id, i.admission_id
HAVING COUNT(*) > 1
ORDER BY icu_stay_count DESC, p.patient_id, i.admission_id;

-- #8
SELECT DISTINCT
  p.patient_id,
  s.icustay_id,
  s.first_careunit,
  s.last_careunit
FROM st_icustays s
JOIN icu_stays i ON i.icu_stay_id = s.icustay_id
JOIN admission a ON a.admission_id = i.admission_id
JOIN patients  p ON p.patient_id = a.patient_id
WHERE s.first_careunit = 'MICU'
  AND s.last_careunit  = 'MICU'
ORDER BY p.patient_id, s.icustay_id;

-- #9
SELECT
  note_id,
  admission_id,
  author,
  note_type,
  note_time,
  has_error,
  note_text
FROM note_events
WHERE admission_id = 142345
ORDER BY note_time NULLS LAST;


-- #10
SELECT
  note_id,
  admission_id,
  author,
  note_time,
  has_error,
  SUBSTRING(note_text FOR 200) AS snippet
FROM note_events
WHERE lower(note_type) = 'discharge summary'
ORDER BY note_time
LIMIT 10;