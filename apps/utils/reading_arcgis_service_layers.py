# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 09:49:33 2021

@author: DIASF
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import urllib.parse
import requests

def query_arcgis_feature_server(url_feature_server='', cols=None, headers={}, rename_columns_with_alias=False):
    '''
    This function downloads all of the features available on a given ArcGIS 
    feature server. The function is written to bypass the limitations imposed
    by the online service, such as only returning up to 1,000 or 2,000 featues
    at a time.

    Parameters
    ----------
    url_feature_server : string
        Sting containing the URL of the service API you want to query. It should 
        end in a forward slash and look something like this:
        'https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Counties/FeatureServer/0/'
    cols : list of strings
        List of column names that will be returned in the final query.
    headers: dict
        Dictionary containing the headers for the URL request.
    
    Returns
    -------
    geodata_final : gpd.GeoDataFrame
        This is a GeoDataFrame that contains all of the features from the 
        Feature Server. After calling this function, the `geodata_final` object 
        can be used to store the data on disk in several different formats 
        including, but not limited to, Shapefile (.shp), GeoJSON (.geojson), 
        GeoPackage (.gpkg), or PostGIS.
        See https://geopandas.org/en/stable/docs/user_guide/io.html#writing-spatial-data
        for more details.

    '''
    if url_feature_server == '':
        geodata_final = gpd.GeoDataFrame()
        return geodata_final

    # Fixing last character in case the URL provided didn't end in a 
    # forward slash
    if url_feature_server[-1] != '/':
        url_feature_server = url_feature_server + '/'
    
    # Getting the layer definitions. This contains important info such as the 
    # name of the column used as feature_ids/object_ids, among other things.
    layer_def = requests.get(url_feature_server + '?f=pjson', headers=headers).json()
    
    # The `objectIdField` is the column name used for the 
    # feature_ids/object_ids
    if 'objectIdField' in layer_def:
        fid_colname = layer_def['objectIdField']
    else: 
        fid_colname = 'OBJECTID'
    # The `maxRecordCount` tells us the maximum number of records this REST 
    # API service can return at once. The code below is written such that we 
    # perform multiple calls to the API, each one being short enough never to 
    # go beyond this limit.
    record_count_max = layer_def['maxRecordCount']
    
    # Part of the URL that specifically requests only the object IDs
    url_query_get_ids = (f'query?f=geojson&returnIdsOnly=true'
                         f'&where={fid_colname}+is+not+null')
    
    url_comb = url_feature_server + url_query_get_ids
    
    # Getting all the object IDs
    service_request = requests.get(url_comb, headers=headers)
    if 'properties' in service_request.json():
        all_objectids = np.sort(service_request.json()['properties']['objectIds'])
    else:
        all_objectids = np.sort(service_request.json()['objectIds'])
    
    if len(all_objectids) == 0:
        geodata_final = gpd.GeoDataFrame()
        return geodata_final
    
    # This variable will store all the parts of the multiple queries. These 
    # parts will, at the end, be concatenated into one large GeoDataFrame.
    geodata_parts = []
    
    # Defining which columns will be included in the GeoDataFrame returned
    # by this function.
    if cols is None:
        col_str = '*'
    else:
        if fid_colname in cols:
            col_str = ','.join(cols)
        else:
            col_str = ','.join([fid_colname] + cols)
    
    # This part of the query is fixed and never actually changes
    url_query_fixed = (f'query?f=geojson&outFields={col_str}&where=')
    
    # Identifying the largest query size allowed per request. This will dictate 
    # how many queries will need to be made. We start the search at
    # the max record count, but that generates errors sometimes - the query 
    # might time out because it's too big. If the test query times out, we try 
    # shrink the query size until the test query goes through without 
    # generating a time-out error.
    block_size = min(record_count_max, len(all_objectids))
    worked = False
    while not worked:
        # Moving the "cursors" to their appropriate locations
        id_start = all_objectids[0]
        id_end = all_objectids[block_size-1]

        readable_query_string = (f'{fid_colname}>={id_start} '
                                 f'and {fid_colname}<={id_end}')
        
        url_query_variable =  urllib.parse.quote(readable_query_string)
    
        url_comb = url_feature_server + url_query_fixed + url_query_variable
        
        url_get = requests.get(url_comb, headers=headers)
        
        if 'error' in url_get.json():
            block_size = int(block_size/2)+1
        else:
            geodata_part = gpd.read_file(url_get.text)
            
            geodata_parts.append(geodata_part.copy())
            worked = True
    
    # Performing the actual query to the API multiple times. This skips the 
    # first few rows/features in the data because those rows were already 
    # captured in the query performed in the code chunk above.
    for i in range(block_size, len(all_objectids), block_size):
        # Moving the "cursors" to their appropriate locations and finding the 
        # limits of each block
        sub_list = all_objectids[i:i + block_size]
        id_start = sub_list[0]
        id_end = sub_list[-1]

        readable_query_string = (f'{fid_colname}>={id_start} '
                                 f'and {fid_colname}<={id_end}')
        
        # Encoding from readable text to URL
        url_query_variable =  urllib.parse.quote(readable_query_string)
    
        # Constructing the full request URL
        url_comb = url_feature_server + url_query_fixed + url_query_variable
        
        # Actually performing the query and storing its results in a 
        # GeoDataFrame
        url_get = requests.get(url_comb, headers=headers)
        geodata_part =  (gpd.read_file(url_get.text, driver='GeoJSON'))
        
        # Appending the result to `geodata_parts`
        if geodata_part.shape[0] > 0:
            geodata_parts.append(geodata_part)

    # Concatenating all of the query parts into one large GeoDataFrame
    geodata_final = (pd.concat(geodata_parts, 
                               ignore_index=True)
                     .sort_values(by=fid_colname)
                     .reset_index(drop=True))
    
    # Checking if any object ID is missing
    ids_queried = set(geodata_final[fid_colname])
    for i,this_id in enumerate(all_objectids):
        if this_id not in ids_queried:
            print('WARNING! The following ObjectID is missing from the final '
                  f'GeoDataFrame: ObjectID={this_id}')
            pass
    
    # Checking if any object ID is included twice
    geodata_temp = geodata_final[[fid_colname]].copy()
    geodata_temp['temp'] = 1
    geodata_temp = (geodata_temp
                    .groupby(fid_colname)
                    .agg({'temp':'sum'})
                    .reset_index())
    geodata_temp = geodata_temp.loc[geodata_temp['temp']>1].copy()
    for i,this_id in enumerate(geodata_temp[fid_colname].values):
        n_times = geodata_temp['temp'].values[i]
        print('WARNING! The following ObjectID is included multiple times in'
              f'the final GeoDataFrame: ObjectID={this_id}\tOccurrences={n_times}')
    
    if rename_columns_with_alias:
        alias_renaming_dict = {this_item['name']:this_item['alias'] for this_item in layer_def['fields']}
        new_column_order = [this_item[1] for this_item in alias_renaming_dict.items()]
        geodata_final = geodata_final.rename(columns=alias_renaming_dict)
    else:
        new_column_order = [this_item['name'] for this_item in layer_def['fields']]
    
    geodata_final = geodata_final[new_column_order + [geodata_final._geometry_column_name]]
    
    return geodata_final

def make_list_query(objectid_list, fid_colname, url_feature_server):
    url_query_fixed = ('query?f=geojson&outFields=*&where=')
    subset_of_objectids = (str(objectid_list)
                           .replace(" ","")
                           .replace("[","(")
                           .replace("]",")"))
    readable_query_string = (f'{fid_colname} in {subset_of_objectids}')
    url_query_variable =  urllib.parse.quote(readable_query_string)
    url_comb = url_feature_server + url_query_fixed + url_query_variable
    return url_comb

def query_arcgis_feature_server_listing(url_feature_server):
    
    # Fixing last character in case the URL provided didn't end in a 
    # forward slash
    if url_feature_server[-1] != '/':
        url_feature_server = url_feature_server + '/'
    
    # Getting the layer definitions. This contains important info such as the 
    # name of the column used as feature_ids/object_ids, among other things.
    layer_def = requests.get(url_feature_server + '?f=pjson').json()
    
    fid_colname = layer_def['objectIdField']
    
    # The `maxRecordCount` tells us the maximum number of records this REST 
    # API service can return at once. The code below is written such that we 
    # perform multiple calls to the API, each one being short enough never to 
    # go beyond this limit.
    record_count_max = layer_def['maxRecordCount']
    
    # Part of the URL that specifically requests only the object IDs
    url_query_get_ids = (f'query?f=geojson&returnIdsOnly=true'
                         f'&where={fid_colname}+is+not+null')
    
    url_comb = url_feature_server + url_query_get_ids
    
    service_request = requests.get(url_comb)
    
    # Fishing out the object IDs
    all_objectids = service_request.json()['properties']['objectIds']
    
    # This is the maximum acceptable string length of a URL containing query
    # arguments. 2000 characters should be a safe bet.
    request_length_max = 2000

    # This variable will store all the parts of the query. These parts will, at
    # the end, be concatenated into one large dataframe.
    geodata_parts = []
    
    query_start = 0
    query_end = 1
    queries_performed = 0
    ids_queried = set()
    while query_end < len(all_objectids)+1:
        # See what the "current" and "next" query URLs look like. 
        this_query_url = make_list_query(objectid_list=all_objectids[query_start:query_end], 
                                         fid_colname=fid_colname, 
                                         url_feature_server=url_feature_server)
        next_query_url = make_list_query(all_objectids[query_start:query_end+1], 
                                         fid_colname=fid_colname, 
                                         url_feature_server=url_feature_server)
        next_query_num_elements = query_end+1 - query_start
        
        # If the next query violates any of the two main conditions (URL too 
        # long or requesting too many features), or if we reach the end of the 
        # list of object IDs to query, just stop "growing" the query and 
        # actually perform it. 
        if ((len(next_query_url) > request_length_max) or 
            (next_query_num_elements >= record_count_max) or 
            (query_end == len(all_objectids))):
            # Perform this_query, store its results and move counters/cursors
            this_query_geodata =  (gpd.read_file(this_query_url, 
                                                 driver='GeoJSON'))
            geodata_parts.append(this_query_geodata.copy())
            
            # Adding ids to "queried" set
            _ = [ids_queried.add(this_id) for this_id in 
                 all_objectids[query_start:query_end]]
            
            # Moving the counters/cursors
            queries_performed += 1
            query_start = query_end
            query_end = query_start + 1
            pass
        else:
            # If none of the conditions were violated, just "grow" the query by 
            # adding another element to it
            query_end+=1
    
    # Checking if any object ID is missing
    for i,this_id in enumerate(all_objectids):
        if this_id not in ids_queried:
            print('WARNING! The following ObjectID was not queried: '
                  f'Index={i} \t ObjectID={this_id}')
            pass
    
    geodata_part_final = (pd.concat(geodata_parts, 
                                    ignore_index=True)
                          .sort_values(by=fid_colname)
                          .reset_index(drop=True))
    
    return geodata_part_final




def query_arcgis_feature_server_old(feature_server_url):
    layer_def = requests.get(feature_server_url + '?f=pjson').json()
    
    fid_colname = layer_def['objectIdField']
    
    url_feature_server = feature_server_url

    url_query_fixed = ('/query?f=geojson&outFields=*&where=')
    
    url_query_variable =   (f'{fid_colname}+is+not+null')
    
    url_comb = url_feature_server + url_query_fixed + url_query_variable
    
    geodata_parts = []
    
    geodata_parts.append(gpd.read_file(url_comb, 
                                       driver='GeoJSON'))
    
    query_size = geodata_parts[0].shape[0]
    
    if query_size == 0:
        return None
    
    max_found_object_id = geodata_parts[0][fid_colname].max()
    
    num_blocks_found = 1
    while True:
        
        url_query_variable = urllib.parse.quote(f'{fid_colname} > '
                                                f'{max_found_object_id}')
    
        url_comb = url_feature_server + url_query_fixed + url_query_variable
    
        geodata_part = (gpd.read_file(url_comb, 
                                      driver='GeoJSON'))
        
        if geodata_part.shape[0] == 0:
            break
        else:
            max_found_object_id = geodata_part[fid_colname].max()
            geodata_parts.append(geodata_part)
            num_blocks_found += 1
            
    
    objectids_set = set(np.concatenate([df[fid_colname].unique() 
                                        for df in geodata_parts]))
    
    missing_objectids = [x for x in 
                         np.array(range(max_found_object_id)) 
                         if x not in objectids_set]
    
    block_size = 100
    n_blocks = int(np.ceil(len(missing_objectids) / block_size))
    
    for i in range(n_blocks):
        block_start = i * block_size
        block_end = block_start + block_size
        
        list_of_objectids = (str(missing_objectids[block_start:block_end])
                             .replace(" ","")
                             .replace("[","(")
                             .replace("]",")"))
        readable_query_string = (f'{fid_colname} in {list_of_objectids}')
        url_query_variable =  urllib.parse.quote(readable_query_string)
    
        url_comb = url_feature_server + url_query_fixed + url_query_variable
        
        geodata_part_miss =  (gpd.read_file(url_comb, 
                                            driver='GeoJSON'))
        
        if geodata_part_miss.shape[0] > 0:
            geodata_parts.append(geodata_part_miss)
    
    
    geodata_part_final = (pd.concat(geodata_parts, 
                                    ignore_index=True)
                          .sort_values(by=fid_colname)
                          .reset_index(drop=True))
    
    return geodata_part_final
