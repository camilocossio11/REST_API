import os
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, request

CREATE_DEPARTMENTS_TABLE = '''CREATE TABLE IF NOT EXISTS departments (
                                id SERIAL PRIMARY KEY,
                                department VARCHAR(255) NOT NULL
                            );'''
CREATE_JOBS_TABLE = '''CREATE TABLE IF NOT EXISTS jobs (
                        id SERIAL PRIMARY KEY,
                        job VARCHAR(255) NOT NULL
                    );'''
CREATE_EMPLOYEES_TABLE = '''CREATE TABLE IF NOT EXISTS employees (
                                id SERIAL PRIMARY KEY,
                                name VARCHAR(255),
                                date TIMESTAMP,
                                department_id INTEGER,
                                job_id INTEGER,
                                FOREIGN KEY(department_id) REFERENCES departments(id) ON DELETE CASCADE,
                                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                            );'''

load_dotenv()

app = Flask(__name__)
url = os.getenv('DATABASE_URL')
connection = psycopg2.connect(url)

@app.post('/api/create_tables')
def create_tables():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_DEPARTMENTS_TABLE)
            cursor.execute(CREATE_JOBS_TABLE)
            cursor.execute(CREATE_EMPLOYEES_TABLE)
    return {'message':'Tables "departments", "employees" and "jobs" created'}, 201

def load_from_csv(tabla, file):
    with connection:
        with connection.cursor() as cursor:
            cursor.copy_from(file, tabla, sep=',')
    return f'Datos cargados exitosamente en la tabla {tabla}'

@app.post('/api/load_data')
def load_data():
    if 'departments' in request.files:
        departments = request.files['departments']
        return load_from_csv(tabla='departments', file=departments)
    elif 'employees' in request.files:
        employees = request.files['employees']
        return load_from_csv(tabla='employees', file=employees)
    elif 'jobs' in request.files:
        jobs = request.files['jobs']
        return load_from_csv(tabla='jobs', file=jobs)
    else:
        return 'No se encontró ningún archivo en la solicitud.'

