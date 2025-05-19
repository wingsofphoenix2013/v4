# main.py
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "v4 engine stub running"}