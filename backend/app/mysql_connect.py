import pymysql

def get_mysql_connection():
    """
    Establish and return a connection to the MySQL server.
    """
    # CREDENTIALS
    user = "root"
    password = input("Enter MySQL password: ") # USER PASSWORD INPUT
    host = "localhost"  # or "127.0.0.1"

    try:
        #CONNECT 
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Now connected to MySQL server!")
        return connection
    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL server: {e}")
        return None
#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def create_sample_database(conn):
    #CREATE HOSPITAL_DB (SAMPLE DATABASE)
    try:
        with conn.cursor() as cursor:
            print("Creating sample database: hospital_db")
            cursor.execute("CREATE DATABASE IF NOT EXISTS hospital_db;")
            cursor.execute("USE hospital_db;")

            #CREATE TABLES
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS province_names (
                    province_id CHAR(2) PRIMARY KEY,
                    province_name TEXT
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS doctors (
                    doctor_id INT PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    specialty TEXT
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS patients (
                    patient_id INT PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    gender CHAR(1),
                    birth_date DATE,
                    city TEXT,
                    province_id CHAR(2),
                    allergies TEXT,
                    height INT,
                    weight INT,
                    FOREIGN KEY (province_id) REFERENCES province_names(province_id)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS admissions (
                    patient_id INT,
                    admission_date DATE,
                    discharge_date DATE,
                    diagnosis TEXT,
                    attending_doctor_id INT,
                    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
                    FOREIGN KEY (attending_doctor_id) REFERENCES doctors(doctor_id)
                );
            """)

            #INSERTS
            cursor.execute("""
                INSERT INTO province_names (province_id, province_name) VALUES 
                ('CA', 'California'), ('NY', 'New York'), ('TX', 'Texas'),
                ('FL', 'Florida'), ('IL', 'Illinois')
                ON DUPLICATE KEY UPDATE province_name = VALUES(province_name);
            """)
            cursor.execute("""
                INSERT INTO doctors (doctor_id, first_name, last_name, specialty) VALUES
                (0001, 'Stacey', 'Mcintosh', 'Cardiology'),
                (0002, 'Carol', 'Wood', 'Neurology'),
                (0003, 'Anthony', 'Hansen', 'Orthopedics'),
                (0004, 'David', 'Collins', 'Pediatrics'),
                (0005, 'Alexis', 'Henry', 'Dermatology'),
                (0006, 'Gabriel', 'Davis', 'Psychiatry'),
                (0007, 'Sandra', 'Bowman', 'Oncology'),
                (0008, 'Courtney', 'Higgins', 'Radiology'),
                (0009, 'Jason', 'Gray', 'Endocrinology'),
                (0010, 'Rebecca', 'Walters', 'Gastroenterology')
                ON DUPLICATE KEY UPDATE first_name = VALUES(first_name);
            """)

            cursor.execute("""
                INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, province_id, allergies, height, weight) VALUES
                (1, 'Shannon', 'Sanders', 'F', '1965-02-07', 'Emilystad', 'FL', 'Pollen', 167, 88),
                (2, 'Beth', 'Pierce', 'M', '1950-04-30', 'Port Lisastad', 'NY', 'None', 159, 98),
                (3, 'Carol', 'Flynn', 'M', '1990-10-12', 'New Joseph', 'FL', 'Peanuts', 195, 78),
                (4, 'Sara', 'Duran', 'F', '1975-08-15', 'Kellytown', 'IL', 'Dust', 174, 60),
                (5, 'Barbara', 'Christensen', 'F', '2003-01-10', 'Espinozaburgh', 'NY', 'None', 194, 97),
                (6, 'Linda', 'Cook', 'M', '1980-08-11', 'Mercerborough', 'FL', 'None', 155, 74),
                (7, 'Debbie', 'Fox', 'F', '1986-12-23', 'Lisaburgh', 'TX', 'Dust', 191, 53),
                (8, 'Jennifer', 'Johnson', 'M', '1994-02-09', 'Elizabethhaven', 'TX', 'Dust', 199, 61),
                (9, 'Juan', 'Hampton', 'M', '1957-12-13', 'Port Peter', 'CA', 'Peanuts', 168, 94),
                (10, 'Brenda', 'Hoffman', 'F', '1985-09-07', 'Port Kenneth', 'NY', 'Pollen', 178, 88),
                (11, 'Danny', 'Baker', 'M', '1973-07-17', 'East William', 'CA', 'Pollen', 186, 97),
                (12, 'Stephanie', 'Santos', 'F', '1976-11-05', 'Sarahstad', 'TX', 'None', 194, 95),
                (13, 'Kevin', 'Harris', 'F', '1973-02-04', 'Lopezshire', 'IL', 'Peanuts', 168, 94),
                (14, 'Yolanda', 'Thomas', 'F', '1987-09-29', 'South Nicholas', 'CA', 'None', 160, 58),
                (15, 'Jesus', 'Hernandez', 'M', '1975-12-19', 'Carterfort', 'IL', 'Pollen', 165, 54),
                (16, 'William', 'Nash', 'F', '1958-01-18', 'Shawnberg', 'CA', 'Pollen', 153, 73),
                (17, 'Miguel', 'Ellis', 'M', '1989-02-22', 'Wagnerport', 'CA', 'Dust', 186, 65),
                (18, 'Nathan', 'Hunt', 'M', '2002-06-25', 'Port Deannaview', 'TX', 'Dust', 164, 67),
                (19, 'Raymond', 'Ingram', 'F', '1973-05-09', 'West Daniel', 'NY', 'Pollen', 154, 59),
                (20, 'Joshua', 'Lucero', 'F', '1977-01-25', 'Lake Erik', 'IL', 'None', 199, 83),
                (21, 'Scott', 'Perez', 'F', '1970-10-20', 'New Andrew', 'TX', 'None', 150, 82),
                (22, 'Cynthia', 'Brown', 'M', '1959-01-17', 'Karenberg', 'TX', 'None', 170, 73),
                (23, 'Jorge', 'Smith', 'M', '1975-10-13', 'New Aaron', 'TX', 'Peanuts', 200, 54),
                (24, 'Jacqueline', 'Turner', 'F', '1982-09-27', 'Lake Travis', 'CA', 'Pollen', 157, 89),
                (25, 'Jason', 'Patel', 'M', '1963-07-25', 'Amberburgh', 'NY', 'None', 168, 57),
                (26, 'Victor', 'Gilbert', 'F', '1960-08-09', 'Port Jacob', 'CA', 'Peanuts', 153, 63),
                (27, 'Julie', 'Ramirez', 'M', '1944-10-07', 'Zacharymouth', 'IL', 'Pollen', 156, 60),
                (28, 'Jessica', 'Gray', 'F', '1964-10-29', 'New Michaelside', 'NY', 'None', 157, 71),
                (29, 'Manuel', 'Nelson', 'M', '1962-12-11', 'North Leslie', 'CA', 'None', 189, 50),
                (30, 'Kevin', 'Bailey', 'F', '1981-09-30', 'Lake Stacyton', 'TX', 'None', 200, 98),
                (31, 'David', 'Sutton', 'F', '1986-03-29', 'North Emmaville', 'TX', 'Dust', 157, 66),
                (32, 'Jason', 'Rodriguez', 'F', '1969-10-12', 'Lake Josephton', 'NY', 'Pollen', 187, 57),
                (33, 'Cynthia', 'Briggs', 'F', '1962-01-13', 'East George', 'IL', 'Peanuts', 173, 69),
                (34, 'Theodore', 'Pace', 'M', '1982-02-13', 'Port Dawnberg', 'FL', 'Pollen', 170, 100),
                (35, 'William', 'Anderson', 'M', '1994-02-18', 'Blakeborough', 'IL', 'Peanuts', 153, 59),
                (36, 'Emily', 'Duran', 'F', '1965-06-23', 'Douglasfurt', 'TX', 'None', 172, 60),
                (37, 'Nichole', 'Nelson', 'M', '1952-03-26', 'Brianhaven', 'NY', 'Peanuts', 172, 59),
                (38, 'Matthew', 'Neal', 'F', '1987-08-21', 'Ingramborough', 'NY', 'Peanuts', 191, 98),
                (39, 'Kelly', 'Peterson', 'M', '1994-11-25', 'Walkermouth', 'TX', 'Dust', 186, 83),
                (40, 'Anna', 'Gill', 'M', '1972-09-12', 'Josephland', 'TX', 'Pollen', 193, 86),
                (41, 'Anthony', 'Mahoney', 'F', '1968-11-09', 'New Lisa', 'IL', 'Dust', 185, 55),
                (42, 'Donald', 'Anderson', 'M', '1946-11-19', 'Port Kristina', 'CA', 'None', 164, 51),
                (43, 'Justin', 'Ferguson', 'F', '1976-02-09', 'Julieton', 'FL', 'Pollen', 174, 60),
                (44, 'Raymond', 'Thomas', 'M', '1962-09-05', 'Mooreville', 'IL', 'None', 182, 67),
                (45, 'Lauren', 'Miller', 'F', '1988-02-21', 'Mayburgh', 'IL', 'Dust', 153, 82),
                (46, 'Melissa', 'Sims', 'F', '2004-07-17', 'Lake Karenchester', 'FL', 'None', 194, 91),
                (47, 'Connie', 'Wade', 'M', '1947-08-15', 'Williamsview', 'CA', 'Peanuts', 177, 70),
                (48, 'Danielle', 'Jackson', 'M', '1951-12-29', 'South Donnabury', 'NY', 'None', 151, 53),
                (49, 'Lisa', 'Hale', 'F', '1959-08-28', 'Warrenstad', 'IL', 'Peanuts', 165, 91),
                (50, 'Nathaniel', 'Ferguson', 'M', '1988-07-27', 'Johnsmouth', 'CA', 'Pollen', 188, 72)
                ON DUPLICATE KEY UPDATE first_name = VALUES(first_name);
                 """)

            cursor.execute("""
                INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id) VALUES
                (10, '2022-05-27', '2022-05-30', 'Diabetes', 10),
                (8, '2024-01-08', '2024-01-11', 'Diabetes', 9),
                (3, '2022-12-01', '2022-12-06', 'Diabetes', 7),
                (18, '2021-08-16', '2021-08-24', 'Asthma', 4),
                (19, '2022-01-27', '2022-02-06', 'Asthma', 4),
                (34, '2020-11-20', '2020-11-27', 'Diabetes', 1),
                (20, '2021-11-20', '2021-11-22', 'Asthma', 5),
                (11, '2024-08-21', '2024-08-28', 'Diabetes', 6),
                (31, '2023-03-19', '2023-03-27', 'Diabetes', 10),
                (32, '2024-02-07', '2024-02-15', 'Fracture', 6),
                (36, '2022-01-16', '2022-01-21', 'Asthma', 8),
                (6, '2024-03-06', '2024-03-11', 'Asthma', 1),
                (40, '2022-12-04', '2022-12-13', 'Asthma', 4),
                (47, '2023-05-22', '2023-05-26', 'Diabetes', 8),
                (14, '2020-04-27', '2020-05-07', 'Fracture', 1),
                (11, '2022-08-18', '2022-08-19', 'Fracture', 5),
                (40, '2023-11-18', '2023-11-19', 'Fracture', 6),
                (10, '2021-09-29', '2021-10-03', 'Fracture', 2),
                (7, '2024-05-03', '2024-05-08', 'Flu', 3),
                (20, '2024-10-05', '2024-10-14', 'Diabetes', 6),
                (10, '2022-04-01', '2022-04-10', 'Flu', 2),
                (5, '2022-01-07', '2022-01-11', 'Asthma', 1),
                (40, '2024-01-18', '2024-01-27', 'Flu', 5),
                (40, '2022-05-27', '2022-05-31', 'Asthma', 6),
                (48, '2020-05-23', '2020-05-24', 'Flu', 8),
                (21, '2022-02-12', '2022-02-21', 'Fracture', 6),
                (38, '2023-03-31', '2023-04-05', 'Flu', 4),
                (37, '2021-11-12', '2021-11-20', 'Asthma', 7),
                (49, '2020-05-08', '2020-05-13', 'Fracture', 5),
                (8, '2023-03-19', '2023-03-29', 'Fracture', 4),
                (29, '2020-03-19', '2020-03-22', 'Flu', 5),
                (38, '2023-04-19', '2023-04-22', 'Diabetes', 1),
                (25, '2023-05-27', '2023-05-28', 'Diabetes', 6),
                (23, '2022-07-04', '2022-07-08', 'Flu', 10),
                (37, '2022-08-08', '2022-08-14', 'Fracture', 6),
                (10, '2020-06-12', '2020-06-15', 'Asthma', 1),
                (20, '2022-10-11', '2022-10-20', 'Diabetes', 8),
                (31, '2020-05-08', '2020-05-13', 'Asthma', 7),
                (20, '2023-02-15', '2023-02-21', 'Flu', 6),
                (50, '2024-10-04', '2024-10-14', 'Fracture', 5),
                (49, '2021-03-19', '2021-03-27', 'Asthma', 3),
                (27, '2024-06-09', '2024-06-13', 'Fracture', 5),
                (47, '2021-08-09', '2021-08-19', 'Asthma', 9),
                (28, '2023-06-12', '2023-06-17', 'Fracture', 9),
                (42, '2020-07-31', '2020-08-07', 'Asthma', 7),
                (18, '2021-05-22', '2021-05-29', 'Fracture', 8),
                (42, '2022-11-28', '2022-12-03', 'Diabetes', 4),
                (14, '2024-07-19', '2024-07-29', 'Diabetes', 8),
                (38, '2023-03-14', '2023-03-15', 'Fracture', 4),
                (3, '2022-12-29', '2023-01-08', 'Fracture', 10)
                ON DUPLICATE KEY UPDATE diagnosis = VALUES(diagnosis);
            """)
            conn.commit()
            print("Sample database 'hospital_db' created and populated successfully!")
    except pymysql.MySQLError as e:
        print(f"Error creating sample database: {e}")
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def verify_connection(conn):
    if conn:
        try:
            with conn.cursor() as cursor:
                # Execute a simple query to get the current database
                cursor.execute("SELECT DATABASE();")
                result = cursor.fetchone()
                print(f"Connected to database: {result['DATABASE()']}")  # Will be None if no database is selected
        except pymysql.MySQLError as e:
            print(f"Error verifying connection: {e}")
        finally:
            conn.close()
    else:
        print("No active connection to verify.")

#USAGE
if __name__ == "__main__":
    conn = get_mysql_connection()
    if conn:
        use_sample = input("Would you like to use a sample database? (y/yes or n/no): ").strip().lower()
        if use_sample in ["y", "yes"]:
            create_sample_database(conn)
        verify_connection(conn)
