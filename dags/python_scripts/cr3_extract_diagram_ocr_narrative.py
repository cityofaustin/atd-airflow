#!/usr/bin/python3

import io
import os
import re
import sys
import json
import boto3
import argparse
import psycopg2
from   pdf2image import convert_from_path, convert_from_bytes
import pytesseract

# Configure the available arguments to the program
parser = argparse.ArgumentParser(description='Extract CR3 diagrams and narratives')
parser.add_argument('-v', action='store_true', help='Be verbose')
parser.add_argument('--cr3-source', metavar=('bucket', 'path'), nargs=2, required=True, help='Where can we hope to find CR3 files on S3?')
parser.add_argument('--batch-size', metavar='int', required=True, help='How many cr3s to attempt to process?')
parser.add_argument('--update-narrative', action='store_true', help='Update narrative in database')
parser.add_argument('--update-timestamp', action='store_true', help='Update timestamp in database')
parser.add_argument('--save-diagram', metavar=('bucket', 'path'), nargs=2, help='Save diagram PNG in a S3 bucket and path')
parser.add_argument('--crash-id', metavar='int', type=int, nargs=1, help='Specific crash ID to operate on')
parser.add_argument('--db-host', metavar='string', nargs=1, required=True, help='DB: host')
parser.add_argument('--db-database', metavar='string', nargs=1, required=True, help='DB: database name')
parser.add_argument('--db-username', metavar='string', nargs=1, required=True, help='DB: username')
parser.add_argument('--db-password', metavar='string', nargs=1, required=True, help='DB: password')
args = parser.parse_args()

s3 = boto3.client('s3')

# connect to a postgres database expecting atd_txdot_crashes
pg = psycopg2.connect(host=args.db_host[0], database=args.db_database[0], user=args.db_username[0], password=args.db_password[0])

# either, get the next N crashes which have not been processed, as known by checking if "processed_date" is null or not
next_group = 'select crash_id from atd_txdot_crashes where processed_date is null order by crash_id desc limit ' + str(args.batch_size)
# or, select a certain crash
set_group = 'select crash_id from atd_txdot_crashes where crash_id in (%s) order by crash_id desc limit ' + str(args.batch_size)

# iterate through the crashes returned
crashes = pg.cursor()
crashes.execute(set_group if args.crash_id else next_group, (args.crash_id))
crash = crashes.fetchone()
while crash is not None:
    crash_id = crash[0]


    # load up the next row, in case something fails and we continue back up to the top of the loop
    crash = crashes.fetchone()


    # be verbose
    if (args.v):
        print("\n")
        print('Preparing to operate on crash_id: ' + str(crash_id))


    # do we want to indicate in the database that an attempt was made to process the CR3.
    if (args.update_timestamp):
        try:
            ts = pg.cursor()
            ts.execute('update atd_txdot_crashes set processed_date = now() where crash_id = %s', [crash_id])
            pg.commit()
            ts.close()
        except (Exception, psycopg2.DatabaseError) as error:
            sys.stderr.write("Error: Failed updating the timestamp for this process running\n")
            print(error)
        finally:
            if ts is not None:
                ts.close()


    # build url and download the CR3
    key = args.cr3_source[1] + '/' + str(crash_id) + '.pdf'
    obj = []
    try:
        pdf = s3.get_object(Bucket=args.cr3_source[0], Key=key)
    except:
        sys.stderr.write("Error: Failed to get PDF from the S3 object\n")
        continue


    # render the pdf into an array of raster images
    pages = []
    try:
        pages = convert_from_bytes(pdf['Body'].read(), 150)
    except: 
        sys.stderr.write("Error: PDF Read for crash_id (" + str(crash_id) + ") failed.\n")
        continue


    # crop out the narrative and diagram into PIL.Image objects
    try:
        narrative = pages[1].crop((96,3683,2580,6049))
        diagram = pages[1].crop((2589,3531,5001,6048))
    except:
        sys.stderr.write("Error: Failed to extract the image of the narative and diagram from image in memory\n")
        continue


    # use tesseract to OCR the text
    text = ''
    try:
        text = (pytesseract.image_to_string(narrative))
        if (args.v):
            print("Extracted Text:\n")
            print(text)
    except:
        sys.stderr.write("Error: Failed to OCR the narrative\n")
        continue


    # do we want to save a PNG file from the image data that was cropped out where the crash diagram is expected to be?
    if (args.save_diagram):
        try:
            # never touch the disk; store the image data in a few steps to get to a variable of binary data
            buffer = io.BytesIO()
            diagram.save(buffer, format='PNG')
            output_diagram = s3.put_object(Body=buffer.getvalue(), Bucket=args.save_diagram[0], Key=args.save_diagram[1] + '/' + str(crash_id) + '.png')
        except:
            sys.stderr.write("Error: Faild setting s3 object containing the diagram PNG file\n")
            continue


    # do we want to store the OCR'd text results from the attempt in the database for the current crash id?
    if (args.update_narrative):
        try:
            narrative = pg.cursor()
            narrative.execute('update atd_txdot_crashes set ocr_narrative = %s where crash_id = %s', (text, crash_id))
            pg.commit()
            narrative.close()
        except (Exception, psycopg2.DatabaseError) as error:
            sys.stderr.write("Error: Failed updating the narrative from the OCR output\n")
            print(error)
        finally:
            if narrative is not None:
                narrative.close()
