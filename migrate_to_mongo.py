import math
from collections import defaultdict

import psycopg2
import psycopg2.extras
from pymongo import MongoClient


# Config

PG_CONFIG = {
    "dbname": "soen363",
    "host": "localhost",
    "port": 5432,
}

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB_NAME = "soen363_phase2"


# Connections

def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)


def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB_NAME]


# Helper group_by function

def group_by(rows, key):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r[key]].append(r)
    return grouped


# Migration from SQL to MongoDB

def migrate():
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    mongo_db = get_mongo_db()
    patients_col = mongo_db["patients"]
    diag_dict_col = mongo_db["diagnosis_dictionary"]

    # Clean Mongo collections
    patients_col.drop()
    diag_dict_col.drop()

    print("Fetching relational data...")

    # Fetch all tables
    pg_cur.execute("SELECT * FROM Patients")
    patients = pg_cur.fetchall()

    pg_cur.execute("SELECT * FROM Admission")
    admissions = pg_cur.fetchall()
    admissions_by_patient = group_by(admissions, "patient_id")

    pg_cur.execute("SELECT * FROM ICU_Stays")
    icu_stays = pg_cur.fetchall()
    icu_by_admission = group_by(icu_stays, "admission_id")

    pg_cur.execute("SELECT * FROM Note_Events")
    notes = pg_cur.fetchall()
    notes_by_admission = group_by(notes, "admission_id")

    pg_cur.execute("SELECT * FROM Diagnosis_ICD")
    diagnoses = pg_cur.fetchall()
    diag_by_admission = group_by(diagnoses, "admission_id")

    pg_cur.execute("SELECT * FROM D_Diagnosis_ICD")
    diag_dict_rows = pg_cur.fetchall()

    print(f"Patients: {len(patients)}")
    print(f"Admissions: {len(admissions)}")
    print(f"ICU_Stays: {len(icu_stays)}")
    print(f"Notes: {len(notes)}")
    print(f"Diagnoses: {len(diagnoses)}")
    print(f"Diagnosis dictionary entries: {len(diag_dict_rows)}")

    # Insert diagnosis dictionary
    diag_dict_docs = []
    for row in diag_dict_rows:
        diag_dict_docs.append({
            "icdCode": row["icd_code"],
            "icdVersion": row["icd_version"],
            "longTitle": row["long_title"],
            "validFrom": row["valid_from"].isoformat() if row["valid_from"] else None,
            "validTo": row["valid_to"].isoformat() if row["valid_to"] else None,
        })

    if diag_dict_docs:
        diag_dict_col.insert_many(diag_dict_docs)
        print("Inserted diagnosis dictionary into Mongo.")

    # Build and insert patients 

    BATCH_SIZE = 1000
    patient_docs_batch = []
    total_inserted = 0

    for p in patients:
        pid = p["patient_id"]

        # fix date_of_birth (datetime.date → ISO String)
        dob = p.get("date_of_birth")
        dob = dob.isoformat() if dob else None

        patient_doc = {
            "patientId": pid,
            "dateOfBirth": dob,
            "dobPrivacy": p["dob_privacy"],
            "gender": p["gender"],
            "lifeStatus": p["life_status"],
            "admissions": []
        }

        # Admissions for this patient
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

            # ICU stays
            for icu in icu_by_admission.get(adm_id, []):
                icu_doc = {
                    "icuStayId": icu.get("icu_stay_id"),
                    "icuInTime": icu.get("icu_in_time"),
                    "icuOutTime": icu.get("icu_out_time"),
                    "firstCareUnit": icu.get("first_careunit"),
                    "lastCareUnit": icu.get("last_careunit"),
                    "firstWardId": icu.get("first_wardid"),
                    "lastWardId": icu.get("last_wardid"),
                }
                adm_doc["icuStays"].append(icu_doc)

            # Notes
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

            # Diagnoses
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

        # batch insert
        if len(patient_docs_batch) >= BATCH_SIZE:
            patients_col.insert_many(patient_docs_batch)
            total_inserted += len(patient_docs_batch)
            print(f"Inserted {total_inserted} patients...")
            patient_docs_batch = []

    # Insert remaining
    if patient_docs_batch:
        patients_col.insert_many(patient_docs_batch)
        total_inserted += len(patient_docs_batch)

    print(f"Total patients inserted into MongoDB: {total_inserted}")

    # Indexes
    print("Creating MongoDB indexes...")

    patients_col.create_index("patientId")
    patients_col.create_index("admissions.admissionId")
    patients_col.create_index("admissions.admitTime")
    patients_col.create_index("admissions.diagnoses.icdCode")
    patients_col.create_index("admissions.notes.noteTime")

    diag_dict_col.create_index(
        [("icdCode", 1), ("icdVersion", 1)], unique=True
    )

    print("Migration completed successfully.")

    pg_cur.close()
    pg_conn.close()


if __name__ == "__main__":
    migrate()
