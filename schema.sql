-- schema.sql (drop-and-create)
DROP TABLE IF EXISTS diagnosis_icd CASCADE;
DROP TABLE IF EXISTS d_diagnosis_icd CASCADE;
DROP TABLE IF EXISTS note_events CASCADE;
DROP TABLE IF EXISTS icu_stays CASCADE;
DROP TABLE IF EXISTS admission CASCADE;
DROP TABLE IF EXISTS patients CASCADE;

CREATE TABLE patients (
    patient_id     INT PRIMARY KEY,
    date_of_birth  DATE NOT NULL,
    dob_privacy    BOOLEAN DEFAULT FALSE,
    gender         VARCHAR(16),
    life_status    VARCHAR(32)
);

CREATE TABLE admission (
    admission_id    INT PRIMARY KEY,
    patient_id      INT NOT NULL,
    admit_time      TIMESTAMP NOT NULL,
    discharge_time  TIMESTAMP,
    visit_type      VARCHAR(50),
    insurance_plan  VARCHAR(50),
    marital_status  VARCHAR(50),
    arrival_source  VARCHAR(100),
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);

CREATE TABLE icu_stays (
    icu_stay_id  INT PRIMARY KEY,
    admission_id INT NOT NULL,
    icu_in_time  TIMESTAMP,
    icu_out_time TIMESTAMP,
    FOREIGN KEY (admission_id) REFERENCES admission(admission_id)
);

CREATE TABLE note_events (
    note_id       INT PRIMARY KEY,
    admission_id  INT NOT NULL,
    icu_stay_id   INT,                -- nullable: many notes aren't ICU-linked
    author        VARCHAR(100),
    note_type     VARCHAR(50),
    note_time     TIMESTAMP,
    has_error     BOOLEAN DEFAULT FALSE,
    note_text     TEXT,
    FOREIGN KEY (admission_id) REFERENCES admission(admission_id),
    FOREIGN KEY (icu_stay_id)  REFERENCES icu_stays(icu_stay_id)
);

CREATE TABLE d_diagnosis_icd (
    icd_code     VARCHAR(10) NOT NULL,
    icd_version  VARCHAR(10) NOT NULL,
    long_title   VARCHAR(255),
    valid_from   DATE,
    valid_to     DATE,
    PRIMARY KEY (icd_code, icd_version)
);

CREATE TABLE diagnosis_icd (
    diagnosis_id          INT PRIMARY KEY,
    admission_id          INT NOT NULL,
    icd_code              VARCHAR(10) NOT NULL,
    icd_version           VARCHAR(10) NOT NULL,
    assigned_time         TIMESTAMP,
    present_on_admission  BOOLEAN,
    sequence              INT,
    FOREIGN KEY (admission_id) REFERENCES admission(admission_id),
    FOREIGN KEY (icd_code, icd_version) REFERENCES d_diagnosis_icd(icd_code, icd_version)
);