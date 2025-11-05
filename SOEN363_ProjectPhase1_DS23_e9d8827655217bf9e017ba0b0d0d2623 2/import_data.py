
import psycopg2
import pandas as pd
import os
import sys
from datetime import datetime


DB_CONFIG = {
    'host': 'localhost',
    'database': 'hospital_database',   
    'password': 'Poppy2003',
    'port': 5432
}

DATA_DIR = './csv_files/' 
TABLE_FILES = {
    'Patients':        'PATIENTS_sorted.csv',
    'D_Diagnosis_ICD': 'D_ICD_DIAGNOSES.csv',
    'Admission':       'ADMISSIONS_sorted.csv',
    'ICU_Stays':       'ICUSTAYS_sorted.csv',
    'Diagnosis_ICD':   'DIAGNOSES_ICD_sorted.csv',
    'Note_Events':     'NOTEEVENTS_sorted.csv'
}

REQUIRE_ICU_FOR_NOTES = False


# HELPERS

def connect_db():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        print(f" Connected to database: {DB_CONFIG['database']}")
        return conn
    except Exception as e:
        print(f" Error connecting to database: {e}")
        sys.exit(1)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN with None for SQL NULL"""
    return df.where(pd.notnull(df), None)

def norm_icd(code):
    """Normalize ICD-9 code to match dictionary/assignments consistently.
       e.g., ' 410.71 ' -> '41071'; None -> None
    """
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return None
    s = str(code).strip().upper()
    
    s = s.replace('.', '')
    return s

def to_date_only(value):
    """Coerce timestamp/string to YYYY-MM-DD (text), or None"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value)
    return s.split(' ')[0]  


# LOADERS

def import_patients(conn, filepath):
    print("\n--- Importing Patients ---")
    cursor = conn.cursor()
    try:
        df_chunks = pd.read_csv(filepath, chunksize=1000, low_memory=False)
        total_rows = 0

        for chunk in df_chunks:
            chunk = clean_data(chunk)
            for _, row in chunk.iterrows():
              
                expire = row.get('EXPIRE_FLAG', 0)
                try:
                    expire = int(expire)
                except (TypeError, ValueError):
                    expire = 0

            
                dob = to_date_only(row.get('DOB'))

                gender = row.get('GENDER')
                if gender is None:
                    gender = 'UNKNOWN'

                cursor.execute("""
                    INSERT INTO Patients (
                        patient_id, date_of_birth, dob_privacy, gender, life_status
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (patient_id) DO NOTHING
                """, (
                    row['SUBJECT_ID'],
                    dob,
                    False,
                    gender,
                    'Expired' if expire == 1 else 'Alive'
                ))
                total_rows += 1

            conn.commit()
            if total_rows % 1000 == 0:
                print(f"  Processed {total_rows} patients...")

        print(f" Imported {total_rows} patients successfully")
    except Exception as e:
        conn.rollback()
        print(f" Error importing patients: {e}")
        raise
    finally:
        cursor.close()

def import_d_diagnosis_icd(conn, filepath):
    print("\n--- Importing D_Diagnosis_ICD ---")
    cursor = conn.cursor()
    try:
        df = pd.read_csv(filepath, low_memory=False)
        df = clean_data(df)
        total_rows = 0

        for _, row in df.iterrows():
            code = norm_icd(row.get('ICD9_CODE'))
           
            if not code:
                continue
            cursor.execute("""
                INSERT INTO D_Diagnosis_ICD (
                    icd_code, icd_version, long_title, valid_from, valid_to
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (icd_code, icd_version) DO NOTHING
            """, (
                code,
                '9',
                row.get('LONG_TITLE'),
                None,
                None
            ))
            total_rows += 1

        conn.commit()
        print(f" Imported {total_rows} ICD diagnosis codes successfully")
    except Exception as e:
        conn.rollback()
        print(f" Error importing ICD diagnoses: {e}")
        raise
    finally:
        cursor.close()

def import_admission(conn, filepath):
    print("\n--- Importing Admission ---")
    cursor = conn.cursor()
    try:
        df_chunks = pd.read_csv(filepath, chunksize=1000, low_memory=False)
        total_rows = 0

        for chunk in df_chunks:
            chunk = clean_data(chunk)
            for _, row in chunk.iterrows():
                cursor.execute("""
                    INSERT INTO Admission (
                        admission_id, patient_id, admit_time, discharge_time,
                        visit_type, insurance_plan, marital_status, arrival_source
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (admission_id) DO NOTHING
                """, (
                    row['HADM_ID'],
                    row['SUBJECT_ID'],
                    row['ADMITTIME'],
                    row.get('DISCHTIME'),
                    row.get('ADMISSION_TYPE'),
                    row.get('INSURANCE'),
                    row.get('MARITAL_STATUS'),
                    row.get('ADMISSION_LOCATION'),
                    row.get('DISCHARGE_LOCATION')
                ))
                total_rows += 1

            conn.commit()
            if total_rows % 1000 == 0:
                print(f"  Processed {total_rows} admissions...")

        print(f" Imported {total_rows} admissions successfully")
    except Exception as e:
        conn.rollback()
        print(f" Error importing admissions: {e}")
        raise
    finally:
        cursor.close()

def import_icu_stays(conn, filepath):
    print("\n--- Importing ICU_Stays ---")
    cursor = conn.cursor()
    try:
        df_chunks = pd.read_csv(filepath, chunksize=1000, low_memory=False)
        total_rows = 0
        skipped_rows = 0

        for chunk in df_chunks:
            chunk = clean_data(chunk)
            for _, row in chunk.iterrows():
  
                cursor.execute("SAVEPOINT sp_icu")
                try:
                    cursor.execute("""
                        INSERT INTO ICU_Stays (
                            icu_stay_id, admission_id, icu_in_time, icu_out_time,
                            first_careunit, last_careunit, first_wardid, last_wardid
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (icu_stay_id) DO NOTHING
                    """, (
                        row['ICUSTAY_ID'],
                        row['HADM_ID'],
                        row['INTIME'],
                        row.get('OUTTIME'),
                        row.get('FIRST_CAREUNIT'),
                        row.get('LAST_CAREUNIT'),
                        row.get('FIRST_WARDID'),
                        row.get('LAST_WARDID')
                    ))
                    total_rows += 1
                except psycopg2.Error:
                    skipped_rows += 1
                    cursor.execute("ROLLBACK TO SAVEPOINT sp_icu")

            conn.commit()
            if total_rows % 1000 == 0:
                print(f"  Processed {total_rows} ICU stays (skipped {skipped_rows})...")

        print(f" Imported {total_rows} ICU stays successfully")
        if skipped_rows > 0:
            print(f"⚠ Skipped {skipped_rows} ICU stays with missing admissions")
    except Exception as e:
        conn.rollback()
        print(f" Error importing ICU stays: {e}")
        raise
    finally:
        cursor.close()

def import_diagnosis_icd(conn, filepath):
    print("\n--- Importing Diagnosis_ICD ---")
    cursor = conn.cursor()
    try:
        df_chunks = pd.read_csv(filepath, chunksize=1000, low_memory=False)
        total_rows = 0
        fixed_dict = 0

        for chunk in df_chunks:
            chunk = clean_data(chunk)

            for _, row in chunk.iterrows():
                code = norm_icd(row.get('ICD9_CODE'))
                if not code:
                    continue

                cursor.execute("SAVEPOINT sp_diag")
                try:
                    cursor.execute("""
                        INSERT INTO Diagnosis_ICD (
                            diagnosis_id, admission_id, icd_code, icd_version,
                            assigned_time, present_on_admission, sequence
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (diagnosis_id) DO NOTHING
                    """, (
                        row['ROW_ID'],
                        row['HADM_ID'],
                        code,
                        '9',
                        None,
                        None,
                        row.get('SEQ_NUM')
                    ))
                    total_rows += 1

                except psycopg2.errors.ForeignKeyViolation:
                   
                    cursor.execute("ROLLBACK TO SAVEPOINT sp_diag")
                    cursor.execute("""
                        INSERT INTO D_Diagnosis_ICD (icd_code, icd_version, long_title, valid_from, valid_to)
                        VALUES (%s, %s, %s, NULL, NULL)
                        ON CONFLICT (icd_code, icd_version) DO NOTHING
                    """, (code, '9', None))
                    fixed_dict += 1

                    cursor.execute("""
                        INSERT INTO Diagnosis_ICD (
                            diagnosis_id, admission_id, icd_code, icd_version,
                            assigned_time, present_on_admission, sequence
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (diagnosis_id) DO NOTHING
                    """, (
                        row['ROW_ID'],
                        row['HADM_ID'],
                        code,
                        '9',
                        None,
                        None,
                        row.get('SEQ_NUM')
                    ))
                    total_rows += 1

                except psycopg2.Error:
                    
                    cursor.execute("ROLLBACK TO SAVEPOINT sp_diag")

            conn.commit()
            if total_rows % 1000 == 0:
                print(f"  Processed {total_rows} diagnoses... (added {fixed_dict} missing dict codes)")

        print(f" Imported {total_rows} diagnoses successfully (added {fixed_dict} missing dict codes)")
    except Exception as e:
        conn.rollback()
        print(f" Error importing diagnoses: {e}")
        raise
    finally:
        cursor.close()

def import_note_events(conn, filepath):
    """Import Note_Events table with ICU inference by time window if ICUSTAY_ID missing."""
    print("\n--- Importing Note_Events ---")
    cursor = conn.cursor()
    try:
        df_chunks = pd.read_csv(filepath, chunksize=500, low_memory=False)
        total_rows = 0
        skipped_rows = 0

        for chunk in df_chunks:
            chunk = clean_data(chunk)

            for _, row in chunk.iterrows():
                icu_id = row.get('ICUSTAY_ID')

     
                if pd.isna(icu_id):
                    note_time = row.get('CHARTTIME') or row.get('STORETIME')
                    if note_time:
                        cursor.execute("""
                            SELECT icu_stay_id
                            FROM ICU_Stays
                            WHERE admission_id = %s
                              AND %s::timestamp BETWEEN icu_in_time AND icu_out_time
                            ORDER BY icu_stay_id
                            LIMIT 1
                        """, (row['HADM_ID'], note_time))
                        r = cursor.fetchone()
                        icu_id = r[0] if r else None

                cursor.execute("SAVEPOINT sp_note")
                try:
                    cursor.execute("""
                        INSERT INTO Note_Events (
                            note_id, admission_id, icu_stay_id, author,
                            note_type, note_time, has_error, note_text
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (note_id) DO NOTHING
                    """, (
                        row['ROW_ID'],
                        row['HADM_ID'],
                        None if pd.isna(icu_id) else int(icu_id),
                        (row.get('DESCRIPTION') if pd.notna(row.get('DESCRIPTION')) else str(row.get('CGID', 'Unknown'))),
                        row.get('CATEGORY'),
                        row.get('CHARTTIME') or row.get('STORETIME'),
                        True if str(row.get('ISERROR', '0')).strip() in ('1','TRUE','true') else False,
                        row.get('TEXT')
                    ))
                    total_rows += 1
                except psycopg2.Error:
                    cursor.execute("ROLLBACK TO SAVEPOINT sp_note")
                    skipped_rows += 1

            conn.commit()
            if total_rows % 500 == 0:
                print(f"  Processed {total_rows} notes (skipped {skipped_rows})...")

        print(f" Imported {total_rows} notes successfully")
        if skipped_rows > 0:
            print(f"⚠ Skipped {skipped_rows} notes (no matching ICU window or row errors)")
    except Exception as e:
        conn.rollback()
        print(f" Error importing notes: {e}")
        raise
    finally:
        cursor.close()


def verify_import(conn):
    print("\n" + "="*60)
    print("DATABASE IMPORT VERIFICATION")
    print("="*60)
    cursor = conn.cursor()
    tables = ['Patients', 'D_Diagnosis_ICD', 'Admission',
              'ICU_Stays', 'Diagnosis_ICD', 'Note_Events']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:20} : {count:,} rows")
    cursor.close()
    print("="*60)

def main():
    print("="*60)
    print(" DATABASE IMPORT SCRIPT (CUSTOM SCHEMA)")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    conn = connect_db()

    import_order = [
        ('Patients',        import_patients),
        ('D_Diagnosis_ICD', import_d_diagnosis_icd),
        ('Admission',       import_admission),
        ('ICU_Stays',       import_icu_stays),
        ('Diagnosis_ICD',   import_diagnosis_icd),
        ('Note_Events',     import_note_events)
    ]

    for table_name, import_func in import_order:
        filepath = os.path.join(DATA_DIR, TABLE_FILES[table_name])
        if not os.path.exists(filepath):
            print(f"⚠ Warning: File not found: {filepath}")
            continue
        import_func(conn, filepath)

    verify_import(conn)
    conn.close()
    print(f"\n Database connection closed")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

if __name__ == "__main__":
    main()