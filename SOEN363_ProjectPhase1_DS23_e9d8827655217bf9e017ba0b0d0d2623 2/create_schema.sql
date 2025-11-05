

-- TABLE: Patients

CREATE TABLE Patients (
    patient_id INT PRIMARY KEY,
    date_of_birth DATE NOT NULL,
    dob_privacy BOOLEAN DEFAULT FALSE,
    gender VARCHAR(16),
    life_status VARCHAR(32)
);


-- TABLE: Admission


CREATE TABLE Admission (
    admission_id INT PRIMARY KEY,
    patient_id INT NOT NULL,
    admit_time TIMESTAMP NOT NULL,
    discharge_time TIMESTAMP,
    visit_type VARCHAR(50),
    insurance_plan VARCHAR(50),
    marital_status VARCHAR(50),
    arrival_source VARCHAR(100),
    
    -- Foreign Key Constraints
    FOREIGN KEY (patient_id) REFERENCES Patients(patient_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);


-- TABLE: ICU_Stays


CREATE TABLE ICU_Stays (
    icu_stay_id INT PRIMARY KEY,
    admission_id INT NOT NULL,
    icu_in_time TIMESTAMP,
    icu_out_time TIMESTAMP,
    first_careunit VARCHAR(50),
    last_careunit VARCHAR(50),
    first_wardid INT,
    last_wardid INT,
    
    -- Foreign Key Constraints
    FOREIGN KEY (admission_id) REFERENCES Admission(admission_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);


-- TABLE: Note_Events

CREATE TABLE Note_Events (
    note_id INT PRIMARY KEY,
    admission_id INT NOT NULL,
    icu_stay_id INT NOT NULL,
    author VARCHAR(100),
    note_type VARCHAR(50),
    note_time TIMESTAMP,
    has_error BOOLEAN DEFAULT FALSE,
    note_text TEXT,
    
    -- Foreign Key Constraints
    FOREIGN KEY (admission_id) REFERENCES Admission(admission_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (icu_stay_id) REFERENCES ICU_Stays(icu_stay_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);


-- TABLE: D_Diagnosis_ICD
CREATE TABLE D_Diagnosis_ICD (
    icd_code VARCHAR(10) NOT NULL,
    icd_version VARCHAR(10) NOT NULL,
    long_title VARCHAR(255),
    valid_from DATE,
    valid_to DATE,
    
    PRIMARY KEY (icd_code, icd_version)
);


-- TABLE: Diagnosis_ICD


CREATE TABLE Diagnosis_ICD (
    diagnosis_id INT PRIMARY KEY,
    admission_id INT NOT NULL,
    icd_code VARCHAR(10) NOT NULL,
    icd_version VARCHAR(10) NOT NULL,
    assigned_time TIMESTAMP,
    present_on_admission BOOLEAN,
    sequence INT,
    
    -- Foreign Key Constraints
    FOREIGN KEY (admission_id) REFERENCES Admission(admission_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (icd_code, icd_version) REFERENCES D_Diagnosis_ICD(icd_code, icd_version)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);





-- Patient lookups
CREATE INDEX idx_patients_dob ON Patients(date_of_birth);
CREATE INDEX idx_patients_gender ON Patients(gender);
CREATE INDEX idx_patients_life_status ON Patients(life_status);

-- Admission lookups
CREATE INDEX idx_admission_patient ON Admission(patient_id);
CREATE INDEX idx_admission_admit_time ON Admission(admit_time);
CREATE INDEX idx_admission_visit_type ON Admission(visit_type);
CREATE INDEX idx_admission_insurance ON Admission(insurance_plan);

-- ICU Stay lookups
CREATE INDEX idx_icu_admission ON ICU_Stays(admission_id);
CREATE INDEX idx_icu_in_time ON ICU_Stays(icu_in_time);

-- Note Event lookups
CREATE INDEX idx_notes_admission ON Note_Events(admission_id);
CREATE INDEX idx_notes_icu_stay ON Note_Events(icu_stay_id);
CREATE INDEX idx_notes_type ON Note_Events(note_type);
CREATE INDEX idx_notes_time ON Note_Events(note_time);
CREATE INDEX idx_notes_author ON Note_Events(author);

-- Diagnosis lookups
CREATE INDEX idx_diagnosis_admission ON Diagnosis_ICD(admission_id);
CREATE INDEX idx_diagnosis_icd_code ON Diagnosis_ICD(icd_code, icd_version);
CREATE INDEX idx_diagnosis_sequence ON Diagnosis_ICD(sequence);





-- Verify tables were created
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
ORDER BY table_name;