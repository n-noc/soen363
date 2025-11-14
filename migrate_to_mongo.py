import math
from collections import defaultdict

import psycopg2
import psycopg2.extras
from pymongo import MongoClient


# ---------- CONFIG ----------

PG_CONFIG = {
    "dbname": "soen363_project",      # change to your DB name
    "user": "postgres",       # change
    "password": "CHANGE_ME",  # change
    "host": "localhost",
    "port": 5432,
}

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB_NAME = "soen363_phase2"

# ---------- CONNECTIONS ----------


def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)


def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB_NAME]


# ---------- HELPERS TO GROUP ROWS ----------


def group_by(rows, key):
    """Group list of dict rows by a key -> {key_value: [rows...]}"""
    grouped = defaultdict(list)
    for r in rows:
        grouped.r[r[key]].append(r)
    return grouped


# ---------- MAIN MIGRATION ----------


def migrate():
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    mongo_db = get_mongo_db()
    patients_col = mongo_db["patients"]
    diag_dict_col = mongo_db["diagnosis_dictionary"]

    # Clean Mongo collections if you want a fresh run
    patients_col.drop()
    diag_dict_col.drop()

    print("Fetching relational data...")

    # --- Fetch all patients ---
    pg_cur.execute("SELECT * FROM Patients")
    patients = pg_cur.fetchall()

    # --- Fetch all admissions ---
    pg_cur.execute("SELECT * FROM Admission")
    admissions = pg_cur.fetchall()
    admissions_by_patient = defaultdict(list)
    for a in admissions:
        admissions_by_patient[a["patient_id"]].append(a)

    # --- Fetch all ICU stays ---
    pg_cur.execute("SELECT * FROM ICU_Stays")
    icu_stays = pg_cur.fetchall()
    icu_by_admission = defaultdict(list)
    for i in icu_stays:
        icu_by_admission[i["admission_id"]].append(i)

    # --- Fetch all notes ---
    pg_cur.execute("SELECT * FROM Note_Events")
    notes = pg_cur.fetchall()
    notes_by_admission = defaultdict(list)
    for n in notes:
        notes_by_admission[n["admission_id"]].append(n)

    # --- Fetch all diagnoses ---
    pg_cur.execute("SELECT * FROM Diagnosis_ICD")
    diag = pg_cur.fetchall()
    diag_by_admission = defaultdict(list)
    for d in diag:
        diag_by_admission[d["admission_id"]].append(d)

    # --- Fetch diagnosis dictionary ---
    pg_cur.execute("SELECT * FROM D_Diagnosis_ICD")
    diag_dict_rows = pg_cur.fetchall()

    print(f"Patients: {len(patients)}")
    print(f"Admissions: {len(admissions)}")
    print(f"ICU_Stays: {len(icu_stays)}")
    print(f"Notes: {len(notes)}")
    print(f"Diagnoses: {len(diag)}")
    print(f"Diagnosis dictionary entries: {len(diag_dict_rows)}")

    # ---------- Insert diagnosis dictionary to Mongo ----------

    diag_dict_docs = []
    for row in diag_dict_rows:
        doc = {
            "icdCode": row["icd_code"],
            "icdVersion": row["icd_version"],
            "longTitle": row["long_title"],
            "validFrom": row["valid_from"],
            "validTo": row["valid_to"],
        }
        diag_dict_docs.append(doc)

    if diag_dict_docs:
        diag_dict_col.insert_many(diag_dict_docs)
        print("Inserted diagnosis dictionary into Mongo.")

    # ---------- Build patient documents with embedded data ----------

    BATCH_SIZE = 1000
    patient_docs_batch = []
    total_inserted = 0

    for p in patients:
        pid = p["patient_id"]

        patient_doc = {
            "patientId": pid,
            "dateOfBirth": p["date_of_birth"],
            "dobPrivacy": p["dob_privacy"],
            "gender": p["gender"],
            "lifeStatus": p["life_status"],
            "admissions": []
        }

        for adm in admissions_by_patient.get(pid, []):
            adm_id = adm["admission_id"]

            adm_doc = {
                "admissionId": adm_id,
                "admitTime": adm["admit_time"],
                "dischargeTime": adm["discharge_time"],
                "visitType": adm["visit_type"],
                "insurancePlan": adm["insurance_plan"],
                "maritalStatus": adm["marital_status"],
                "arrivalSource": adm["arrival_source"],
                "icuStays": [],
                "notes": [],
                "diagnoses": []
            }

            # ICU stays for this admission
            for icu in icu_by_admission.get(adm_id, []):
                icu_doc = {
                    "icuStayId": icu["icu_stay_id"],
                    "icuInTime": icu["icu_in_time"],
                    "icuOutTime": icu["icu_out_time"],
                    "firstCareUnit": icu["first_careunit"],
                    "lastCareUnit": icu["last_careunit"],
                    "firstWardId": icu["first_wardid"],
                    "lastWardId": icu["last_wardid"],
                }
                adm_doc["icuStays"].append(icu_doc)

            # Notes for this admission
            for n in notes_by_admission.get(adm_id, []):
                note_doc = {
                    "noteId": n["note_id"],
                    "icuStayId": n["icu_stay_id"],
                    "author": n["author"],
                    "noteType": n["note_type"],
                    "noteTime": n["note_time"],
                    "hasError": n["has_error"],
                    "noteText": n["note_text"],
                }
                adm_doc["notes"].append(note_doc)

            # Diagnoses for this admission
            for d in diag_by_admission.get(adm_id, []):
                diag_doc = {
                    "diagnosisId": d["diagnosis_id"],
                    "icdCode": d["icd_code"],
                    "icdVersion": d["icd_version"],
                    "assignedTime": d["assigned_time"],
                    "presentOnAdmission": d["present_on_admission"],
                    "sequence": d["sequence"],
                }
                adm_doc["diagnoses"].append(diag_doc)

            patient_doc["admissions"].append(adm_doc)

        patient_docs_batch.append(patient_doc)

        # Insert in batches to avoid huge single insert
        if len(patient_docs_batch) >= BATCH_SIZE:
            patients_col.insert_many(patient_docs_batch)
            total_inserted += len(patient_docs_batch)
            print(f"Inserted {total_inserted} patients...")
            patient_docs_batch = []

    # Insert remaining docs
    if patient_docs_batch:
        patients_col.insert_many(patient_docs_batch)
        total_inserted += len(patient_docs_batch)

    print(f"Total patients inserted into MongoDB: {total_inserted}")

    # ---------- Create indexes in MongoDB ----------

    print("Creating MongoDB indexes...")

    patients_col.create_index("patientId")
    patients_col.create_index("admissions.admissionId")
    patients_col.create_index("admissions.admitTime")
    patients_col.create_index("admissions.diagnoses.icdCode")
    patients_col.create_index("admissions.notes.noteTime")

    diag_dict_col.create_index([("icdCode", 1), ("icdVersion", 1)], unique=True)

    print("Migration completed successfully.")

    pg_cur.close()
    pg_conn.close()


if __name__ == "__main__":
    migrate()
