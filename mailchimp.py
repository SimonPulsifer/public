#################################################################################################
#MailChimp
#
#Add new emails to ONDP MailChimp
#

import argparse
import db_connector

import hashlib
from mailchimp3 import MailChimp
import psycopg2.extras


#python c:/scripts/reports/mailchimp.py --vault vault_on

def main():

    parser = argparse.ArgumentParser(description='Data Source')
    parser.add_argument('--vault', required=False, default = 'C:\\Scripts\\configs\\vault.json', help='configuration parameter file')
    args = parser.parse_args()


    run(db_connector.connector(args.vault)["conn"])   

def run(conn):

    client = MailChimp(mc_api='xxxxxxxxxxxxxxxxxxxxxx', mc_user='uuuuuuuuuuuuuuu')
    list_id = 'lllllllllllllll'

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sql = """SELECT DISTINCT email,firstname, lastname, phone FROM field.signups
                WHERE type = 'Worker' AND create_dt > now() - interval '1 day' AND email is not null AND lastname is not null"""
    cur.execute (sql)
    results = cur.fetchall()

    count = 0
    for row in results:
        email_hash = hashlib.md5(row["email"].lower().encode(encoding='utf-8')).hexdigest()
        
        try:
            client.lists.members.get(list_id=list_id, subscriber_hash=email_hash)
            print('email exists', row["email"])
        except:
            count += 1
            if row["phone"]:
                client.lists.members.create(list_id,  {
                'email_address': row["email"],
                'status': 'subscribed',
                'merge_fields': {'FNAME': row["firstname"],'LNAME': row["lastname"],'PHONE': row["phone"] },})
            else:
                client.lists.members.create(list_id,  {
                'email_address': row["email"],
                'status': 'subscribed',
                'merge_fields': {'FNAME': row["firstname"],'LNAME': row["lastname"] },})
            print('new email', row["email"])

    print('added', count, 'new emails')

###############################################################################
# RUN
#

if __name__ == '__main__':
    main()
