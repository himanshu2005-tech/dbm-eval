from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import os, shutil, time, psutil, hashlib
from backend.execute.scidb import upload_to_scidb
from backend.execute.mapreduce import upload_to_mapreduce

app = FastAPI()

# ---- CORS ----
origins = ["http://localhost:3000"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Upload directory ----
UPLOAD_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "uploads"))
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def file_metrics(path):
    """Collect extra metrics for uploaded file"""
    stat = os.stat(path)
    md5 = hashlib.md5(open(path, "rb").read()).hexdigest()
    return {
        "file_size_bytes": stat.st_size,
        "file_md5": md5,
    }

@app.post("/upload-and-process")
async def upload_and_process(file: UploadFile = File(...)):
    response = {}

    # Save uploaded file
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    metrics = file_metrics(file_path)

    # ---- SciDB upload ----
    try:
        scidb_result = upload_to_scidb(file_path)
        response["scidb"] = {**scidb_result, **metrics}
    except Exception as e:
        response["scidb"] = {"error": str(e)}

    # ---- MapReduce upload ----
    try:
        mapreduce_result = upload_to_mapreduce(file_path)
        response["mapreduce"] = {**mapreduce_result, **metrics}
    except Exception as e:
        response["mapreduce"] = {"error": str(e)}

    # ---- Compare ----
    try:
        if "execution_time_seconds" in response["scidb"] and "execution_time_seconds" in response["mapreduce"]:
            sc_time = response["scidb"]["execution_time_seconds"]
            mr_time = response["mapreduce"]["execution_time_seconds"]
            faster = "scidb" if sc_time < mr_time else "mapreduce"
            response["comparison"] = {
                "faster_system": faster,
                "scidb_time": sc_time,
                "mapreduce_time": mr_time,
                "time_difference": abs(sc_time - mr_time)
            }
        else:
            response["comparison"] = {"error": "Missing execution time in one or both systems"}
    except Exception as e:
        response["comparison"] = {"error": str(e)}

    return response
