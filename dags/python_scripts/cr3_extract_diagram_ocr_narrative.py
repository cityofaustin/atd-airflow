#!/usr/bin/python3

import io
import os
import re
import sys
import json
import boto3
import argparse
import requests
from   pdf2image import convert_from_path, convert_from_bytes
import pytesseract

# Configure the available arguments to the program
parser = argparse.ArgumentParser(description='Extract CR3 diagrams and narratives')
parser.add_argument('-v', action='store_true', help='Be verbose')
parser.add_argument('-d', action='store_true', help='Check for digitally created PDFs')
parser.add_argument('--cr3-source', metavar=('bucket', 'path'), nargs=2, required=True, help='Where can we hope to find CR3 files on S3?')
parser.add_argument('--batch-size', metavar='int', default=1, help='How many cr3s to attempt to process?')
parser.add_argument('--update-narrative', action='store_true', help='Update narrative in database')
parser.add_argument('--update-timestamp', action='store_true', help='Update timestamp in database')
parser.add_argument('--save-diagram-s3', metavar=('bucket', 'path'), nargs=2, help='Save diagram PNG in a S3 bucket and path')
parser.add_argument('--save-diagram-disk', metavar=('path'), nargs=1, help='Save diagram PNG to disk in a certain directory')
parser.add_argument('--crash-id', metavar='int', type=int, nargs=1, default=[0], help='Specific crash ID to operate on')
args = parser.parse_args()

s3 = boto3.client('s3')

def update_crash_processed_date(crash_id: int) -> bool:
    if (args.v):
        print("update_crash_processed_date(" + str(crash_id) + ")")
    query = """
    mutation update_crash_processsed_date($crash_id: Int) {
        update_atd_txdot_crashes(where: {crash_id: {_eq: $crash_id}}, _set: {cr3_ocr_extraction_date: "now"}) {
	    affected_rows
        }
    }
    """
    response = requests.post(
        url=os.getenv("HASURA_ENDPOINT"),
        headers={
            "Accept": "*/*",
            "content-type": "application/json",
            "x-hasura-admin-secret": os.getenv("HASURA_ADMIN_KEY")
            },
        json={
            "query": query,
            "variables": {
                "crash_id": crash_id
                }
            }
        )
    try:
        return response.json()["data"]["update_atd_txdot_crashes"]["affected_rows"] > 0
    except (KeyError, TypeError):
        sys.stderr.write("ERROR")
        sys.stderr.write(response.json())


def update_crash_narrative(crash_id: int, narrative: str, metadata: dict) -> bool:
    if (args.v):
        print("update_crash_narrative(crash_id: " + str(crash_id) + ", ...)")

    if not(isinstance(metadata, dict)):
        metadata = {}
    metadata['narrative'] = narrative

    query = """
    mutation update_crash_narrative($crash_id: Int, $metadata: jsonb) {
        update_atd_txdot_crashes(where: {crash_id: {_eq: $crash_id}}, _set: {cr3_file_metadata: $metadata}) {
	    affected_rows
        }
    }
    """
    response = requests.post(
        url=os.getenv("HASURA_ENDPOINT"),
        headers={
            "Accept": "*/*",
            "content-type": "application/json",
            "x-hasura-admin-secret": os.getenv("HASURA_ADMIN_KEY")
            },
        json={
            "query": query,
            "variables": {
                "crash_id": crash_id,
                "metadata": metadata
                }
            }
        )
    try:
        return response.json()["data"]["update_atd_txdot_crashes"]["affected_rows"] > 0
    except (KeyError, TypeError):
        sys.stderr.write("ERROR")
        sys.stderr.write(response.json())



get_batch = """
query find_cr3s($limit: Int) {
  atd_txdot_crashes(where: {
      cr3_ocr_extraction_date: {_is_null: true},
      crash_id: {_gt: 10000}
      crash_date: {_gte: "2015-01-01"}
      },
    limit: $limit) {
      crash_id,
      cr3_ocr_extraction_date,
      cr3_file_metadata
  }
}
"""
batch_variables = { "limit": int(args.batch_size) }

get_single_cr3 = """
query find_cr3s($crash_id: Int, $limit: Int) {
  atd_txdot_crashes(where: {
      crash_id: {_eq: $crash_id}
      },
    limit: $limit) {
      crash_id,
      cr3_ocr_extraction_date,
      cr3_file_metadata
  }
}
"""
single_cr3_variables = { "crash_id": args.crash_id[0], "limit": int(args.batch_size) }

graphql = ''
variables = {}

if (args.crash_id[0]):
    graphql = get_single_cr3
    variables = single_cr3_variables
else:
    graphql = get_batch
    variables = batch_variables

response = requests.post(
  url=os.getenv("HASURA_ENDPOINT"),
  headers={
    "Accept": "*/*",
    "content-type": "application/json",
    "x-hasura-admin-secret": os.getenv("HASURA_ADMIN_KEY")
    },
  json={
    "query": graphql,
    "variables": variables
    }
  )

for crash in response.json()['data']['atd_txdot_crashes']:
    # be verbose
    if (args.v):
        print('Preparing to operate on crash_id: ' + str(crash['crash_id']))

    # do we want to indicate in the database that an attempt was made to process the CR3.
    if (args.update_timestamp):
        update_crash_processed_date(crash['crash_id'])

    # build url and download the CR3
    if (args.v):
        print('Pulling CR3 PDF from S3');
    key = args.cr3_source[1] + '/' + str(crash['crash_id']) + '.pdf'
    obj = []
    try:
        pdf = s3.get_object(Bucket=args.cr3_source[0], Key=key)
    except:
        sys.stderr.write("Error: Failed to get PDF from the S3 object\n")
        continue

    # render the pdf into an array of raster images
    if (args.v):
        print('Rendering PDF into images');
    pages = []
    try:
        pages = convert_from_bytes(pdf['Body'].read(), 150)
    except: 
        sys.stderr.write("Error: PDF Read for crash_id (" + str(crash['crash_id']) + ") failed.\n")
        continue


    # write a rendered image to disk for debugging
    #pages[1].save('/home/frank/Desktop/page.png')

    if (args.d):
        if (args.v):
            print('Excuting a check for a digitally created PDF');
        digital_end_to_end = True
        # these pixels are expected to be black on digitally created PDFs
        pixels = [(110,3520), (3080, 3046), (3050, 2264), (2580, 6056), (1252, 154), (2582, 4166), (1182, 1838)]
        for pixel in pixels:
            rgb_pixel = pages[1].getpixel(pixel)
            if not(rgb_pixel[0] == 0 and rgb_pixel[1] == 0 and rgb_pixel[2] == 0):
                digital_end_to_end = False
            if (args.v):
                print('Pixel' + "(%04d,%04d)" % pixel + ': ' + str(rgb_pixel))
        if (args.v):
            print('PDF Digital End to End?: ' + str(digital_end_to_end));
        if not(digital_end_to_end):
            if (args.v):
                sys.stderr.write("Error: Non-digitally created PDF detected.\n")
            continue


    # crop out the narrative and diagram into PIL.Image objects
    if (args.v):
        print('Cropping narrative and diagram from images');
    try:
        narrative_image = pages[1].crop((96,3683,2580,6049))
        diagram_image = pages[1].crop((2589,3531,5001,6048))
    except:
        sys.stderr.write("Error: Failed to extract the image of the narative and diagram from image in memory\n")
        continue


    # use tesseract to OCR the text
    if (args.v):
        print('OCRing narrative')
    narrative = ''
    try:
        narrative = (pytesseract.image_to_string(narrative_image))
        if (args.v):
            print("Extracted Text:\n")
            print(narrative)
    except:
        sys.stderr.write("Error: Failed to OCR the narrative\n")
        continue

    # do we want to save a PNG file from the image data that was cropped out where the crash diagram is expected to be?
    if (args.save_diagram_s3):
        if (args.v):
            print('Saving PNG of diagram to S3')
        try:
            # never touch the disk; store the image data in a few steps to get to a variable of binary data
            buffer = io.BytesIO()
            diagram_image.save(buffer, format='PNG')
            output_diagram = s3.put_object(Body=buffer.getvalue(), Bucket=args.save_diagram_s3[0], Key=args.save_diagram_s3[1] + '/' + str(crash['crash_id']) + '.png')
        except:
            sys.stderr.write("Error: Faild setting s3 object containing the diagram PNG file\n")
            continue

    # do we want to save a PNG file from the image data to disk?
    if (args.save_diagram_disk):
        if (args.v):
            print('Saving PNG of diagram to disk')
        try:
            path = args.save_diagram_disk[0] + '/' + str(crash['crash_id']) + '.png'
            diagram_image.save(path)
        except:
            sys.stderr.write("Error: Faild diagram PNG file to disk\n")

    # do we want to store the OCR'd text results from the attempt in the database for the current crash id?
    if (args.update_narrative):
        update_crash_narrative(crash['crash_id'], narrative, crash['cr3_file_metadata'])

    if (args.v):
        print("\n")
