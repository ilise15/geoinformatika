import psycopg2
import pandas as pd

conn = None
cur = None

try:
    conn = psycopg2.connect(
        dbname="univerzitet",
        user="postgres",
        password="og",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # ako tabele vec postoje, prvo dropovati upis zbog foreign key-a
    try:
        cur.execute("DROP TABLE IF EXISTS upis;")
        cur.execute("DROP TABLE IF EXISTS student;")
        cur.execute("DROP TABLE IF EXISTS kurs;")
        conn.commit()
    except Exception as e:
        print(f"Error pri brisanju tabela: {e}")
        exit(1)

    try:
        # ============================= TABELA STUDENT =============================
        create_script = ''' CREATE TABLE IF NOT EXISTS student (
                            id SERIAL PRIMARY KEY,
                            ime varchar(40) NOT NULL,
                            prezime varchar(40) NOT NULL,
                            indeks varchar(20) NOT NULL UNIQUE );'''
        cur.execute(create_script)
        conn.commit()

        # ============================= TABELA KURS =============================
        create_script = ''' CREATE TABLE IF NOT EXISTS kurs (
                            kurs_id SERIAL PRIMARY KEY,
                            naziv varchar(40) NOT NULL,
                            espb int NOT NULL );'''
        cur.execute(create_script)
        conn.commit()

        # ============================= TABELA UPIS =============================
        create_script = ''' CREATE TABLE IF NOT EXISTS upis (
                            student_id int REFERENCES student(id),
                            kurs_id int REFERENCES kurs(kurs_id),
                            ocena int NOT NULL );'''
        cur.execute(create_script)
        conn.commit()
    except Exception as e:
        print(f"Error pri kreiranju tabela: {e}")
        exit(1)

    try:
        # ============================= UNOS STUDENATA U TABELU STUDENT =============================
        insert_script = ''' INSERT INTO student (ime, prezime, indeks) VALUES (%s, %s, %s); '''
        insert_values = [
            ('Pera', 'Peric', 'RA1/2025'),
            ('Mika', 'Mikic', 'RA2/2025'),
            ('Zika', 'Zikic', 'RA3/2025'),
            ('Laza', 'Lazic', 'RA4/2025'),
            ('Sima', 'Simic', 'RA5/2025')
        ]
        cur.executemany(insert_script, insert_values)
        conn.commit()

        # ============================= UNOS KURSEVA U TABELU KURS =============================
        insert_script = ''' INSERT INTO kurs (naziv, espb) VALUES (%s, %s); '''
        insert_values = [
            ('Matematička analiza 1', 8),
            ('Fizika', 6),
            ('Algebra', 8),
            ('Engleski za inženjere', 4),
            ('Programski jezici i strukture podataka', 8)
        ]
        cur.executemany(insert_script, insert_values)
        conn.commit()

        # ============================= UBACIVANJE ZAPISA U TABELU UPIS =============================
        insert_script = ''' INSERT INTO upis (student_id, kurs_id, ocena) VALUES (%s, %s, %s); '''
        insert_values = [
            (1, 1, 8),
            (1, 2, 6),
            (1, 3, 8),
            (2, 1, 5),
            (2, 4, 10),
            (3, 5, 6),
            (4, 1, 8),
            (4, 2, 7),
            (5, 5, 10)
        ]
        cur.executemany(insert_script, insert_values)
        conn.commit()
    except Exception as e:
        print(f"Error pri unosu podataka: {e}")
        exit(1)
    
    # ============================= ISPIS POMOCU PANDAS DF =============================
    try:
        cur.execute("SELECT * FROM student;")
        conn.commit()
        df_student = pd.DataFrame(cur.fetchall())

        cur.execute("SELECT * FROM kurs;")
        conn.commit()
        df_kurs = pd.DataFrame(cur.fetchall())

        cur.execute("SELECT * FROM upis;")
        conn.commit()
        df_upis = pd.DataFrame(cur.fetchall())

        print("\nIspis svih studenata: \n", df_student)
        print("\nIspis svih kurseva: \n", df_kurs)
        print("\nIspis svih upisa: \n", df_upis)
    except Exception as e:
        print(f"Error pri ispisu podataka: {e}")
        exit(1)
    
    try:
        # ============================= IZMENA OCENE =============================
        izmena_ocene = input("\nKom studentu menjate ocenu? (indeks studenta, naziv kursa i nova ocena, odvojeno zarezom): ")
        student, kurs, nova_ocena = [x.strip() for x in izmena_ocene.split(",")]

        cur.execute("UPDATE upis SET ocena = %s WHERE student_id = (SELECT id FROM student WHERE indeks = %s) AND kurs_id = (SELECT kurs_id FROM kurs WHERE naziv = %s);", (nova_ocena, student, kurs))
        conn.commit()

        '''
        cur.execute("UPDATE upis SET ocena = 9 WHERE student_id = 1 AND kurs_id = 2;")
        conn.commit()
        cur.execute("SELECT s.ime, s.prezime, s.indeks, k.naziv, u.ocena FROM student s "
                    "JOIN upis u ON s.id = u.student_id "
                    "JOIN kurs k ON u.kurs_id = k.kurs_id "
                    "WHERE s.id = 1 AND u.kurs_id = 2;")
        conn.commit()
        '''

        cur.execute("SELECT s.ime, s.prezime, s.indeks, k.naziv, u.ocena FROM student s "
                    "JOIN upis u ON s.id = u.student_id "
                    "JOIN kurs k ON u.kurs_id = k.kurs_id "
                    "WHERE s.id = (SELECT id FROM student WHERE indeks = %s) AND u.kurs_id = (SELECT kurs_id FROM kurs WHERE naziv = %s);", (student, kurs))
        conn.commit()

        df_nova_ocena = pd.DataFrame(cur.fetchall())
        print("Ispis upisa studenta sa novom ocenom: \n", df_nova_ocena)
    except Exception as e:
        print(f"Error pri izmeni ocene: {e}")
        exit(1)

    try:
        # ============================= DODAVANJE NOVOG STUDENTA I UPIS =============================
        novi_student = input("\nUnesite ime, prezime i indeks novog studenta (odvojeno zarezom): ")
        ime, prezime, indeks = [x.strip() for x in novi_student.split(",")]
        cur.execute("INSERT INTO student (ime, prezime, indeks) VALUES (%s, %s, %s);", (ime, prezime, indeks))
        conn.commit()

        novi_upis = input("Unesite naziv kursa i ocenu za novog studenta (odvojeno zarezom): ")
        kurs, ocena = [x.strip() for x in novi_upis.split(",")]
        cur.execute("INSERT INTO upis (student_id, kurs_id, ocena) VALUES ((SELECT id FROM student WHERE indeks = %s), (SELECT kurs_id FROM kurs WHERE naziv = %s), %s);", (indeks, kurs, ocena))
        conn.commit()

        '''
        cur.execute("INSERT INTO student (ime, prezime, indeks) VALUES ('Ana', 'Anic', 'RA6/2025');"
                    "INSERT INTO upis (student_id, kurs_id, ocena) VALUES ((SELECT id FROM student WHERE indeks = 'RA6/2025'), 5, 7);")
        conn.commit()
        cur.execute("SELECT s.ime, s.prezime, s.indeks, k.naziv, u.ocena FROM student s "
                    "JOIN upis u ON s.id = u.student_id "
                    "JOIN kurs k ON u.kurs_id = k.kurs_id "
                    "WHERE s.id = (SELECT id FROM student WHERE indeks = 'RA6/2025') AND u.kurs_id = 5;")
        conn.commit()
        '''

        cur.execute("SELECT s.ime, s.prezime, s.indeks, k.naziv, u.ocena FROM student s "
                    "JOIN upis u ON s.id = u.student_id "
                    "JOIN kurs k ON u.kurs_id = k.kurs_id "
                    "WHERE s.id = (SELECT id FROM student WHERE indeks = %s) AND u.kurs_id = (SELECT kurs_id FROM kurs WHERE naziv = %s);", (indeks, kurs))
        conn.commit()

        df_novi_upis = pd.DataFrame(cur.fetchall())
        print("Ispis novog studenta i upisa: ", df_novi_upis)
    except Exception as e:
        print(f"Error pri dodavanju novog studenta i upisa: {e}")
        exit(1)



    print("\n============================= TESTOVI =============================\n")
    try:
        # svi studenti sa upisanim kursevima i ocenama
        cur.execute("SELECT s.ime, s.prezime, s.indeks, k.naziv, u.ocena FROM student s "
                    "JOIN upis u ON s.id = u.student_id "
                    "JOIN kurs k ON u.kurs_id = k.kurs_id;")
        conn.commit()

        df_svi_upisi = pd.DataFrame(cur.fetchall())
        print("Ispis svih studenata sa upisanim kursevima i ocenama: \n", df_svi_upisi)
    except Exception as e:
        print(f"Error test 1: {e}")
        exit(1)

    try:
        # svi studenti sa ocenom vecom od 8
        cur.execute("SELECT s.ime, s.prezime, s.indeks, u.ocena FROM student s "
                    "JOIN upis u ON s.id = u.student_id "
                    "WHERE u.ocena > 8;")
        conn.commit()

        df_veci = pd.DataFrame(cur.fetchall())
        print("\nIspis svih studenata sa ocenom vecom od 8: \n", df_veci)
    except Exception as e:
        print(f"Error test 2: {e}")
        exit(1)

    try:
        # prosecna ocena po kursu
        cur.execute("SELECT k.naziv, AVG(u.ocena) as prosecna_ocena FROM kurs k "
                    "JOIN upis u ON k.kurs_id = u.kurs_id "
                    "GROUP BY k.naziv;")
        conn.commit()

        df_prosecna_ocena = pd.DataFrame(cur.fetchall())
        print("\nIspis prosecne ocene po kursu: \n", df_prosecna_ocena)
    except Exception as e:
        print(f"Error test 3: {e}")
        exit(1)

    try:
        # broj studenata upisanih na svaki kurs
        cur.execute("SELECT k.naziv, COUNT(u.student_id) as broj_studenata FROM kurs k "
                    "JOIN upis u ON k.kurs_id = u.kurs_id "
                    "GROUP BY k.naziv;")
        conn.commit()

        df_broj_studenata = pd.DataFrame(cur.fetchall())
        print("\nIspis broja studenata po kursu: \n", df_broj_studenata)
    except Exception as e:
        print(f"Error test 4: {e}")
        exit(1)

    try:
        # pretraga studenata po prezimenu
        prezime = input("\nUnesite prezime studenta za pretragu: ")
        cur.execute("SELECT * FROM student WHERE prezime = %s;", (prezime,))
        conn.commit()

        df_prezime = pd.DataFrame(cur.fetchall())
        print("Ispis studenata sa trazenim prezimenom: \n", df_prezime)
    except Exception as e:
        print(f"Error test 5: {e}")
        exit(1)

except Exception as e:
    print(f"Error pri konekciji: {e}")
    exit(1)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
