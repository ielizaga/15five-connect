import logging, coreapi,csv,argparse,os
import psycopg2

url= os.environ['FFIVE_URL']
token= os.environ['FFIVE_TOKEN']
dbhost= os.environ['FFIVE_DBHOST']
dbname= os.environ['FFIVE_DBNAME']
dbuser= os.environ['FFIVE_DBUSER']
dbpassword= os.environ['FFIVE_DBPASS']

# TODO: initializing these here doesn't make sense, these should be executed after argument parsing

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

def read_object(obj, objid):
    action= [obj, "read"]
    response= client.action(schema, action, params= {"id": objid})
    return response

def truncate_db(conn):
    print("Truncating tables in ffive_schema")
    cur= conn.cursor()
    cur.execute("""
        TRUNCATE TABLE ffive_staging.users;
        TRUNCATE TABLE ffive_staging.groups;
        TRUNCATE TABLE ffive_staging.reports;
        TRUNCATE TABLE ffive_staging.pulses;
        TRUNCATE TABLE ffive_staging.oneonones;
        TRUNCATE TABLE ffive_staging.highfives;
        TRUNCATE TABLE ffive_staging.highfivementions;
    """)
    conn.commit()

def create_db(conn):
    print("Dropping and recreating ffive_staging schema...")
    cur = conn.cursor()
    cur.execute("""
        DROP SCHEMA IF EXISTS ffive_staging CASCADE;
        CREATE SCHEMA IF NOT EXISTS ffive_staging;
        CREATE TABLE IF NOT EXISTS ffive_staging.users(
        id integer,
        first_name text,
        last_name text,
        email text,
        emp_id text,
        title text,
        is_active boolean,
        is_reporter boolean,
        is_reviewer boolean,
        is_company_admin boolean,
        first_login_ts timestamp,
        last_login_ts timestamp,
        reviewer_id integer
        );

        CREATE TABLE IF NOT EXISTS ffive_staging.groups(
        id integer,
        name text);

        CREATE TABLE IF NOT EXISTS ffive_staging.user_groups(
        userid integer,
        groupid integer);

        CREATE TABLE IF NOT EXISTS ffive_staging.reports(
        id integer,
        user_id integer,
        due_date date,
        reporting_period text,
        reporting_period_start date,
        reporting_period_end date,
        submit_ts timestamp,
        reviewed_ts timestamp,
        reviewed_by integer,
        was_submitted_late boolean);

        CREATE TABLE IF NOT EXISTS ffive_staging.pulses(
        id integer,
        report integer,
        user_id integer,
        create_ts timestamp,
        value integer);

        CREATE TABLE IF NOT EXISTS ffive_staging.oneonones(
        id integer,
        user_1 integer,
        user_1_role text,
        user_2 integer,
        user_2_role text,
        is_draft boolean,
        for_date date,
        end_ts timestamp,
        create_ts timestamp);

        CREATE TABLE IF NOT EXISTS ffive_staging.highfives(
        id integer,
        report integer,
        create_ts timestamp,
        message_text text,
        creator_id integer);

        CREATE TABLE IF NOT EXISTS ffive_staging.highfivementions(
        highfive_id integer,
        creator_id integer,
        receiver_id integer);
        """)
    conn.commit()

def insert_users(conn):
    users= extract_list('user')
    print("Found %s users" % len(users))
    for i in users:
        user_fields= read_object('user', i['id'])

        user_id= i['id']
        first_name= i['first_name']
        last_name= i['last_name']
        email= i['email']
        emp_id= user_fields['employee_id']
        title= user_fields['title']
        is_active= user_fields['is_active']
        is_reporter= user_fields['is_reporter']
        is_reviewer= user_fields['is_reviewer']
        is_company_admin= user_fields['is_company_admin']
        first_login_ts= user_fields['first_login_ts']
        last_login_ts= user_fields['last_login_ts']
        reviewer_id= user_fields['reviewer_id']
        
        # TODO: Debug logging?
        # print(user_id, first_name, last_name, email, emp_id, title, is_active, is_reporter, is_reviewer, is_company_admin, first_login_ts, last_login_ts, reviewer_id)
        
        cur= conn.cursor()
        cur.execute("INSERT INTO ffive_staging.users values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (user_id, first_name, last_name, email, emp_id, title, is_active, is_reporter, is_reviewer, is_company_admin, first_login_ts, last_login_ts, reviewer_id))
        conn.commit()

        for cgroup in user_fields['company_groups_ids']:
            cur= conn.cursor()
            cur.execute("INSERT INTO ffive_staging.user_groups values (%s, %s)", (user_id, cgroup))
            conn.commit()

def insert_groups(conn):
    groups= extract_list('group')
    print("Found %s groups" % len(groups))
    for i in groups:
        group_id= i['id']
        group_name= i['name']

        cur= conn.cursor()
        cur.execute("INSERT INTO ffive_staging.groups (id,name) values (%s,%s)", (group_id, group_name))
        conn.commit()

def insert_reports(conn, due_date=None):
    reports= extract_list('report',due_date)
    print("Found %s reports" % len(reports))
    for i in reports:
        report_fields= read_object('report',i['id'])
        report_id= i['id']
        user= i['user'].split('/')[-2]
        due_date= i['due_date']
        reporting_period= report_fields['reporting_period']
        reporting_period_start= report_fields['reporting_period_start'],
        reporting_period_end= report_fields['reporting_period_end']
        submit_ts= report_fields['submit_ts']
        reviewed_ts= report_fields['reviewed_ts']
        reviewed_by= None
        if report_fields['reviewed_by']:
            reviewed_by= report_fields['reviewed_by'].split('/')[-2]
        was_submitted_late= report_fields['was_submitted_late']
        
        cur= conn.cursor()
        cur.execute("INSERT INTO ffive_staging.reports values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (report_id, user, due_date, reporting_period, reporting_period_start, reporting_period_end, submit_ts, reviewed_ts, reviewed_by, was_submitted_late))
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
        cur.execute("INSERT INTO ffive_staging.pulses (id,report,user_id,create_ts,value) values (%s,%s,%s,%s,%s)", (pulse_id, report, user, create_ts, value))
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
        cur.execute("INSERT INTO ffive_staging.oneonones (id,user_1,user_1_role,user_2,user_2_role,is_draft,for_date,end_ts,create_ts) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (oneonone_id, user_1, user_1_role, user_2, user_2_role, is_draft, for_date, end_ts, create_ts))
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
        cur.execute("INSERT INTO ffive_staging.highfives (id,report,create_ts,message_text,creator_id) values (%s,%s,%s,%s,%s)", (highfive_id, report, create_ts, message_text, creator_id))
        conn.commit()

        for j in i['receivers']:
            cur= conn.cursor()
            receiver_id= j['id']
            cur.execute("INSERT INTO ffive_staging.highfivementions (highfive_id, creator_id,receiver_id) values (%s,%s,%s)", (highfive_id, creator_id, receiver_id))

def main():
    ap= argparse.ArgumentParser()
    ap.add_argument("--date", required= True, help= "Start due date for reports, pulses, one-on-ones and high-fives")
    ap.add_argument("--truncate", action= "store_true", help= "Truncate tables instead of recreating them")
    args= vars(ap.parse_args())

    due_date= args['date']
    truncate= args['truncate']
    print("Collecting 15five data after %s..." % due_date)

    if truncate:
        truncate_db(conn)
    else:
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
