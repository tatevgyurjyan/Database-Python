import mysql.connector
import logging

logging.basicConfig(filename='test_log_db.log',
                    format='%(asctime)s: - %(levelname)s:%(message)s',
                    level=logging.DEBUG)
try:
    class Database:

        def db_connection(self):
            con = mysql.connector.connect(user='qwallity',
                                          password='mysqlpass',
                                          host='qwallitydb.cywlir8bfmdo.eu-central-1.rds.amazonaws.com',
                                          database='qwallity_db')
            logging.info("Connected with Database!")
            return con
    
        def inner_join(self):
            try:
                db = self.db_connection()
                cursor = db.cursor()
                query = """select title from qwallity_db.course_type
                inner join qwallity_db.courses on course_type.type_id = courses.coursetype
                where coursetype = 2"""
                cursor.execute(query)
                rows = cursor.fetchall()
                logging.info("Query is executed!")
                return str(rows)
            except Exception as e:
                logging.error(e)

        def db_close(self):
            self.db_connection().close()
            logging.info("Connection closed!")

    db_data = Database()
    db_data.inner_join()

    file = open("course_Names.txt", "w")
    file.write(db_data.inner_join())
    logging.info("Course names are retrieved!")

except Exception as e:
    logging.exception(e)

finally:
    db_data.db_close()
    file.close()
