import coreapi,csv,argparse,os
import psycopg2

url= os.environ['FFIVE_URL']
token= os.environ['FFIVE_TOKEN']
dbhost= os.environ['FFIVE_DBHOST']
dbname= os.environ['FFIVE_DBNAME']
dbuser= os.environ['FFIVE_DBUSER']
dbpassword= os.environ['FFIVE_DBPASS']

auth = coreapi.auth.TokenAuthentication(token=token)
client = coreapi.Client(auth=auth)
schema = client.get(url)
conn = psycopg2.connect(host=dbhost, dbname=dbname, user=dbuser, password=dbpassword)

def extract_list(obj, due_date_start= None, full_list=[] ,page=1):
    action = [obj, "list"]
    start_fieldname = "due_date_start"

    if obj == "pulse" or obj == "high-five":
        start_fieldname= "created_on_start"
    if obj == "one-on-one":
        start_fieldname= "scheduled_on_start"

    if due_date_start:
        response = client.action(schema, action, params= {"page":page, start_fieldname:due_date_start})
    else:
        response = client.action(schema, action, params= {"page":page})
    if response['next']:
        full_list = full_list+extract_list(obj,due_date_start,full_list,page+1)
    return full_list+response['results']

def create_db(conn):
    cur = conn.cursor()
    cur.execute("""
        DROP SCHEMA ffive CASCADE;
        CREATE SCHEMA IF NOT EXISTS ffive;
        CREATE TABLE IF NOT EXISTS ffive.users(
        id integer,
        first_name text,
        last_name text,
        email text);

        CREATE TABLE IF NOT EXISTS ffive.groups(
        id integer,
        name text);

        CREATE TABLE IF NOT EXISTS ffive.reports(
        id integer,
        user_id integer,
        due_date date);

        CREATE TABLE IF NOT EXISTS ffive.pulses(
        id integer,
        report integer,
        user_id integer,
        create_ts timestamp,
        value integer);

        CREATE TABLE IF NOT EXISTS ffive.oneonones(
        id integer,
        user_1 integer,
        user_1_role text,
        user_2 integer,
        user_2_role text,
        is_draft boolean,
        for_date date,
        end_ts timestamp,
        create_ts timestamp);

        CREATE TABLE IF NOT EXISTS ffive.highfives(
        id integer,
        report integer,
        create_ts timestamp,
        message_text text,
        creator_id integer);

        CREATE TABLE IF NOT EXISTS ffive.highfivementions(
        creator_id integer,
        receiver_id integer);
        """)
    conn.commit()

def insert_users(conn):
    users= extract_list('user')
    print("Found %s users" % len(users))
    for i in users:
        user_id= i['id']
        first_name= i['first_name']
        last_name= i['last_name']
        email= i['email']

        cur= conn.cursor()
        cur.execute("INSERT INTO ffive.users (id,first_name,last_name,email) values (%s,%s,%s,%s)", (user_id, first_name, last_name, email))
        conn.commit()

def insert_groups(conn):
    groups= extract_list('group')
    print("Found %s groups" % len(groups))
    for i in groups:
        group_id= i['id']
        group_name= i['name']

        cur= conn.cursor()
        cur.execute("INSERT INTO ffive.groups (id,name) values (%s,%s)", (group_id, group_name))
        conn.commit()

def insert_reports(conn, due_date=None):
    reports= extract_list('report',due_date)
    print("Found %s reports" % len(reports))
    for i in reports:
        report_id= i['id']
        user= i['user'].split('/')[-2]
        due_date= i['due_date']
        
        cur= conn.cursor()
        cur.execute("INSERT INTO ffive.reports (id,user_id,due_date) values (%s,%s,%s)", (report_id, user, due_date))
        conn.commit()

def insert_pulses(conn, due_date=None):
    pulses= extract_list('pulse',due_date)
    print("Found %s pulses" % len(pulses))
    for i in pulses:
        pulse_id= i['id']
        report= i['report'].split('/')[-2]
        user= i['user'].split('/')[-2]
        create_ts= i['create_ts']
        value= i['value']
        
        cur= conn.cursor()
        cur.execute("INSERT INTO ffive.pulses (id,report,user_id,create_ts,value) values (%s,%s,%s,%s,%s)", (pulse_id, report, user, create_ts, value))
        conn.commit()

def insert_oneonones(conn, due_date=None):
    oneonones= extract_list('one-on-one',due_date)
    print("Found %s one-on-ones" % len(oneonones))
    for i in oneonones:
        oneonone_id= i['id']
        user_1= i['user_1'].split('/')[-2]
        user_1_role= i['user_1_role']
        user_2= i['user_2'].split('/')[-2]
        user_2_role= i['user_2_role']
        is_draft= i['is_draft']
        for_date= i['for_date']
        end_ts= i['end_ts']
        create_ts= i['create_ts']
        
        cur= conn.cursor()
        cur.execute("INSERT INTO ffive.oneonones (id,user_1,user_1_role,user_2,user_2_role,is_draft,for_date,end_ts,create_ts) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (oneonone_id, user_1, user_1_role, user_2, user_2_role, is_draft, for_date, end_ts, create_ts))
        conn.commit()

def insert_highfives(conn, due_date=None):
    highfives= extract_list('high-five',due_date)
    print("Found %s high-fives" % len(highfives))
    for i in highfives:
        highfive_id= i['id']
        report= None
        if(i['report']):
            report= i['report'].split('/')[-2]
        create_ts= i['create_ts']
        message_text= i['text']
        creator_id= i['creator'].split('/')[-2]
        
        cur= conn.cursor()
        cur.execute("INSERT INTO ffive.highfives (id,report,create_ts,message_text,creator_id) values (%s,%s,%s,%s,%s)", (highfive_id, report, create_ts, message_text, creator_id))
        conn.commit()

        for j in i['receivers']:
            cur= conn.cursor()
            receiver_id= j['id']
            cur.execute("INSERT INTO ffive.highfivementions (creator_id,receiver_id) values (%s,%s)", (creator_id, receiver_id))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help= "Start due date for reports, pulses, one-on-ones and high-fives")
    args= vars(ap.parse_args())

    due_date= args['date']
    print("Collecting 15five data after %s..." % due_date)

    create_db(conn)
    insert_users(conn)
    insert_groups(conn)
    insert_reports(conn, due_date)
    insert_pulses(conn, due_date)
    insert_oneonones(conn, due_date)
    insert_highfives(conn, due_date)

    conn.close()  

if __name__ == "__main__":
    main()