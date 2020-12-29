import os
import re
#import glob

#import tqdm
import boto3
s3 = boto3.resource('s3')

import terracotta as tc

# settings
DB_NAME = 'tc.sqlite'
#RASTER_GLOB = r'/path/to/rasters/*.tif'
#RASTER_NAME_PATTERN = r'(?P<sensor>\w{2})_(?P<tile>\w{5})_(?P<date>\d{8})_(?P<band>\w+).tif'
KEYS = ('sensor', 'tile', 'date', 'band')
KEY_DESCRIPTIONS = {
    'sensor': 'Sensor short name',
    'tile': 'Sentinel-2 tile ID',
    'date': 'Sensing date',
    'band': 'Band or index name'
}
#S3_BUCKET = 'tc-testdata'
#S3_RASTER_FOLDER = 'rasters'
#S3_PATH = f's3://{S3_BUCKET}/{S3_RASTER_FOLDER}'

driver = tc.get_driver(DB_NAME)

# create an empty database if it doesn't exist
if not os.path.isfile(DB_NAME):
    driver.create(KEYS, KEY_DESCRIPTIONS)

# sanity check
assert driver.key_names == KEYS

available_datasets = driver.get_datasets()
#raster_files = list(glob.glob(RASTER_GLOB))
#pbar = tqdm.tqdm(raster_files)

#'http://landsat-pds.s3.amazonaws.com/
#https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/
#https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/34/H/BK/2019/2/S2B_34HBK_20190228_0_L2A/B02.tif
aws_rasters = [['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/34/H/BK/2019/2/S2B_34HBK_20190228_0_L2A/','S2','34HBK','20190228'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/33/H/YC/2019/2/S2B_33HYC_20190218_0_L2A/','S2','33HYC','20190218'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/34/H/BJ/2019/1/S2B_34HBJ_20190126_0_L2A/','S2','34HBJ','20190126'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/34/H/BH/2019/2/S2B_34HBH_20190205_0_L2A/','S2','34HBH','20190205'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/34/H/CJ/2019/1/S2A_34HCJ_20190121_0_L2A/','S2','34HCJ','20190121'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/33/H/YE/2019/2/S2A_33HYE_20190213_0_L2A/','S2','33HYE','20190213'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/34/H/CH/2019/1/S2B_34HCH_20190106_0_L2A/','S2','34HCH','20190106'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/34/H/CK/2019/1/S2A_34HCK_20190121_0_L2A/','S2','34HCK','20190121'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/34/H/DH/2019/1/S2B_34HDH_20190106_0_L2A/','S2','34HDH','20190106'],
 ['https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/33/H/YD/2019/1/S2A_33HYD_20190124_0_L2A/','S2','33HYD','20190124']]

#for raster_path in pbar:
for raster_path in aws_rasters:
    for band in ['B02', 'B03', 'B04']:

        #pbar.set_postfix(file=raster_path)

        #raster_filename = os.path.basename(raster_path)

        # extract keys from filename
        #match = re.match(RASTER_NAME_PATTERN, raster_filename)
        #if match is None:
        #    raise ValueError(f'Input file {raster_filename} does not match raster pattern')

        #keys = match.groups()

        # skip already processed data
        #if keys in available_datasets:
        #    continue
        print('Connecting to db for keys: ',raster_path[1], raster_path[2],raster_path[3], band)
        print('For file name: ', raster_path[0]+band+'.tif')
        with driver.connect():
            # since the rasters will be served from S3, we need to pass the correct remote path
            #driver.insert(keys, raster_path, override_path=f'{S3_PATH}/{raster_filename}')
            driver.insert([raster_path[1], raster_path[2],raster_path[3], band], raster_path[0]+band+'.tif')
print('created...')
# upload database to S3
#s3.meta.client.upload_file(DB_NAME, S3_BUCKET, DB_NAME)