"""
Title: Extract Service Info From Portal

Version: 4.0

Description:
    loops throuh services in selected groups
    and pulls out the information into excel.
    The script also does a comparison with previous
    output versions to determine what has changed.

    
    
Author: Gordon McLachlan
Date Created: 06/07/2022
 
"""
import arcpy
from arcpy import env
import os
import datetime
import time

    
from arcgis.gis import GIS
from arcgis.mapping import WebMap
from arcgis.features import FeatureLayerCollection
import pandas as pd
import numpy as np

import re
from re import sub
import html
import unicodedata
import shutil



def df_excel_sheet(in_excel, in_sheet):
    """
        read in excel sheet and convert to dataframe
    """
    
    # open excel in data frame
    xl = pd.ExcelFile(in_excel)
    df = xl.parse(in_sheet)

    return df

def compare_excel(curr_xl, prev_xl, sheet_name, writer):

    # convert to dataframe
    df_data_report_current = df_excel_sheet(curr_xl, sheet_name)
    df_data_report_previous = df_excel_sheet(prev_xl, sheet_name)

    # get the key columns
    df_data_report_current_keycol = df_data_report_current[['Service ID','Title', 'Date Data Was Lasted Edited', 'Data Number', 'URL']]
    df_data_report_previous_keycol = df_data_report_previous[['Service ID','Title', 'Date Data Was Lasted Edited', 'Data Number', 'URL']]

    # outer merge
    df_merged = df_data_report_current_keycol.merge(df_data_report_previous_keycol, how='outer', left_on='Service ID', right_on='Service ID')

    # rename
    df_merged = df_merged.rename(columns={"Title_x": "Title_Cur", 
                                          "Date Data Was Lasted Edited_x": "Date Data Was Lasted Edited_Cur", 
                                          "Data Number_x": "Data Number_Cur", 
                                          "URL_x": "URL_Cur", 
                                          "Title_y": "Title_Pre", 
                                          "Date Data Was Lasted Edited_y": 
                                          "Date Data Was Lasted Edited_Pre",
                                          "Data Number_y": "Data Number_Pre", 
                                          "URL_y": "URL_Pre"})
    

    # get all the removed services
    df_removed = df_merged[df_merged['Title_Cur'].isna()]
    df_removed = df_removed[['Service ID','Title_Pre', 'Date Data Was Lasted Edited_Pre', 'Data Number_Pre', 'URL_Pre']]
    df_removed = df_removed.rename(columns={"Title_Pre": "Title", "Date Data Was Lasted Edited_Pre": "Date Data Was Lasted Edited", "Data Number_Pre": "Data Number", "URL_Pre": "URL"})
    # print(df_removed)


    # get all new services
    df_new = df_merged[df_merged['Title_Pre'].isna()]
    df_new = df_new[['Service ID','Title_Cur', 'Date Data Was Lasted Edited_Cur', 'URL_Cur']]
    df_new = df_new.rename(columns={"Title_Cur": "Title", "Date Data Was Lasted Edited_Cur": "Date Data Was Lasted Edited", "Data Number_Cur": "Data Number", "URL_Cur": "URL"})
    # print(df_new)
    

    # differences
    # filter out the "new"
    df_dif = df_merged[df_merged['Title_Cur'].str.len() > 0]
    # filter out the "removed"
    df_dif = df_dif[df_merged['Title_Pre'].str.len() > 0]
    
    
    ## CHECK DIFFERENCES
    # I tried to do this in singel query but the null values were confusing the queries. So split up to make sure it is picking up correct data
    # CHECK THE DATA NUMBER DIFFERENCES
    # check if curr is null but previously it wasnt
    comparison_column = np.where(
                                (df_dif['Data Number_Cur'].isna()) & (df_dif['Data Number_Pre'].notna()), 
                                False, True) 
    df_dif["equal"] = comparison_column

    # check if  curr not null but previously it was
    comparison_column = np.where(
                                (df_dif['Data Number_Cur'].notna()) & (df_dif['Data Number_Pre'].isna()), 
                                False, df_dif["equal"]) 
    df_dif["equal"] = comparison_column

    # check if values are differeny
    comparison_column = np.where(
                                ((df_dif['Data Number_Cur'].notna()) & (df_dif['Data Number_Pre'].notna())) & (df_dif["Data Number_Cur"]!=df_dif["Data Number_Pre"]), 
                                False, df_dif["equal"]) 
    df_dif["equal"] = comparison_column


    # CHECK THE TITLE DIFFERENCES
    comparison_column = np.where(
                                (df_dif['Title_Cur'].isna()) & (df_dif['Title_Pre'].notna()), 
                                False, df_dif["equal"]) 
    df_dif["equal"] = comparison_column

    # check if  curr not null but previously it was
    comparison_column = np.where(
                                (df_dif['Title_Cur'].notna()) & (df_dif['Title_Pre'].isna()), 
                                False, df_dif["equal"]) 
    df_dif["equal"] = comparison_column

    # check if values are differeny
    comparison_column = np.where(
                                ((df_dif['Title_Cur'].notna()) & (df_dif['Title_Pre'].notna())) & (df_dif["Title_Cur"]!=df_dif["Title_Pre"]), 
                                False, df_dif["equal"]) 
    df_dif["equal"] = comparison_column


    # CHECK THE DATE DIFFERENCES
    comparison_column = np.where(
                                (df_dif['Date Data Was Lasted Edited_Cur'].isna()) & (df_dif['Date Data Was Lasted Edited_Pre'].notna()), 
                                False, df_dif["equal"]) 
    df_dif["equal"] = comparison_column

    # check if  curr not null but previously it was
    comparison_column = np.where(
                                (df_dif['Date Data Was Lasted Edited_Cur'].notna()) & (df_dif['Date Data Was Lasted Edited_Pre'].isna()), 
                                False, df_dif["equal"]) 
    df_dif["equal"] = comparison_column

    # check if values are differeny
    comparison_column = np.where(
                                ((df_dif['Date Data Was Lasted Edited_Cur'].notna()) & (df_dif['Date Data Was Lasted Edited_Pre'].notna())) & (df_dif["Date Data Was Lasted Edited_Cur"]!=df_dif["Date Data Was Lasted Edited_Pre"]), 
                                False, df_dif["equal"]) 
    df_dif["equal"] = comparison_column



    # clean up findings
    df_dif = df_dif[df_dif['equal'] == False]
    df_dif = df_dif.iloc[:, lambda df: [0, 1, 2, 3, 4]]
    df_dif = df_dif.rename(columns={"Title_Cur": "Title", "Date Data Was Lasted Edited_Cur": "Date Data Was Lasted Edited", "Data Number_Cur": "Data Number", "URL_Cur": "URL"})


    # export to excel

    df_dif.to_excel(writer,sheet_name = 'Updated {}'.format(sheet_name), index=False, header=True)
    df_new.to_excel(writer,sheet_name = 'New {}'.format(sheet_name), index=False, header=True)
    df_removed.to_excel(writer,sheet_name = 'Removed {}'.format(sheet_name), index=False, header=True)



def handle_break(data):
    if data is not None:
        text = data.strip()
        if len(text) > 0:
            text = sub('</b>', '\n', text)
            text = sub('<br />', '\n', text)
            text = text.replace('<b>', '')
            text = text.replace('<br>', '')
            return text

def handle_div(data):
    if data is not None:
        text = data.strip()
        if len(text) > 0:
            text = text.replace('<div>', '')
            text = text.replace('</div>', '')
            return text

def handle_para(data):
    if data is not None:
        text = data.strip()
        if len(text) > 0:
            text = text.replace('<p>', '')
            text = text.replace('</p>', '')
            return text

def find_list_index(check_el, in_list):

    val = 0
    for el in in_list:
        if check_el.upper() in el.upper():
            return val
        else:
            val = val + 1


def delete_carriage_returns_string(in_str):
    """
        Delete carriage return and new lines. Replace with a bank space
    """
    str_new = in_str.replace("\r", " ").replace("\n", " ")
    
    return str_new

def remove_rogue_html(in_str):
    """
        remove known rogue html elements from input string, caused by formatting
    """   

    if in_str is None:
        return None

    remove_el_list = [
                        "<span style='font-weight:bold;'>",
                        '<SPAN STYLE="font-weight:bold;">',
                        "<span style='font-family:inherit;'>",
                        "<span style='font-family:inherit; font-weight:bold;'>",
                        "<span style='font-size:16px;'>",
                        "<span style='font-family:inherit; font-size:16px;'>",
                        "<span style='font-size:14.6667px;'>",
                        "<span style='font-size:14px;'>",
                        "<span style='background-color:rgb(232, 235, 250); color:rgb(36, 36, 36);'>"
                        "<span style='font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif;'>"
                        "<span style='font-size:11.0pt; font-family:Calibri,sans-serif;'>",
                        "<span style='font-weight:bold; font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'> <span style='font-weight:bold; font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'>",
                        "<span style='font-weight:bold; font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'>",
                        "<span style='font-weight:bold; font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'>",
                        "<span style='font-size:11pt; font-family:Calibri, sans-serif;'>",
                        "<span style='font-family:Calibri, sans-serif; font-size:14.6667px;'>",
                        "<span style='font-family:Calibri, sans-serif; font-size:11pt;'>"
                        "<font face='inherit'>",
                        "<font size='3'>",
                        "<font face='Arial, sans-serif'>",
                        "<font color='#242424' face='-apple-system, BlinkMacSystemFont, Segoe UI, system-ui, Apple Color Emoji, Segoe UI Emoji, Segoe UI Web, sans-serif'>",
                        "<font face='-apple-system, BlinkMacSystemFont, Segoe UI, system-ui, Apple Color Emoji, Segoe UI Emoji, Segoe UI Web, sans-serif'><span style='font-size:14px;'>",
                        "<font style='font-family:inherit; font-size:16px;'>",
                        "<font face='-apple-system, BlinkMacSystemFont, Segoe UI, system-ui, Apple Color Emoji, Segoe UI Emoji, Segoe UI Web, sans-serif'>",
                        "<font face='-apple-system, BlinkMacSystemFont, Segoe UI, system-ui, Apple Color Emoji, Segoe UI Emoji, Segoe UI Web, sans-serif'>",
                        "<span style='font-size:14px;'>",
                        "<span style='font-family:inherit; font-weight:bold; font-size:16px;'>",
                        "<span style='font-family:&quot;Avenir Next W01&quot;, &quot;Avenir Next W00&quot;, &quot;Avenir Next&quot;, Avenir, &quot;Helvetica Neue&quot;, sans-serif; font-size:16px;'>",
                        "<span style='color:rgb(36, 36, 36); font-family:-apple-system, BlinkMacSystemFont, &quot;Segoe UI&quot;, system-ui, &quot;Apple Color Emoji&quot;, &quot;Segoe UI Emoji&quot;, &quot;Segoe UI Web&quot;, sans-serif; font-size:14px; background-color:rgb(232, 235, 250);'>",
                        "<span style='background-color:rgb(232, 235, 250); color:rgb(36, 36, 36); font-family:-apple-system, BlinkMacSystemFont, &quot;Segoe UI&quot;, system-ui, &quot;Apple Color Emoji&quot;, &quot;Segoe UI Emoji&quot;, &quot;Segoe UI Web&quot;, sans-serif; font-size:14px;'>",
                        "<span style='font-family:-apple-system, BlinkMacSystemFont, &quot;Segoe UI&quot;, system-ui, &quot;Apple Color Emoji&quot;, &quot;Segoe UI Emoji&quot;, &quot;Segoe UI Web&quot;, sans-serif; color:rgb(36, 36, 36); font-size:14px; background-color:rgb(232, 235, 250);'>",
                        "<span style='color:rgba(0, 0, 0, 0.9); font-family:&quot;Segoe UI VSS (Regular)&quot;, &quot;Segoe UI&quot;, -apple-system, BlinkMacSystemFont, Roboto, &quot;Helvetica Neue&quot;, Helvetica, Ubuntu, Arial, sans-serif, &quot;Apple Color Emoji&quot;, &quot;Segoe UI Emoji&quot;, &quot;Segoe UI Symbol&quot;; font-size:14px;'>",
                        "<span style='color:rgb(36, 36, 36); font-family:-apple-system, BlinkMacSystemFont, &quot;Segoe UI&quot;, system-ui, &quot;Apple Color Emoji&quot;, &quot;Segoe UI Emoji&quot;, &quot;Segoe UI Web&quot;, sans-serif; font-size:14px;'>",
                        "<span style='font-family:-apple-system, BlinkMacSystemFont, Segoe UI, system-ui, Apple Color Emoji, Segoe UI Emoji, Segoe UI Web, sans-serif; font-size:14px;'>",
                        "<span style='font-weight:bold; font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'> <span style='font-weight:bold; font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'>",
                        "<span style='font-size:medium; font-family:inherit;'>", 
                        "<span style='font-size:11.0pt; font-family:Calibri,sans-serif;'>",
                        "<span style='font-family:Calibri, sans-serif; font-size:14.6667px;'>",
                        "<span style='font-size:11pt; font-family:Calibri, sans-serif;'>",
                        "<span style='font-family:Calibri, sans-serif; font-size:14.6667px;'>",
                        "<p style='margin-top:0px; margin-bottom:1.5rem;'>",
                        "<p style='margin-top:0px; margin-bottom:1.5rem; font-family:&quot;Avenir Next W01&quot;, &quot;Avenir Next W00&quot;, &quot;Avenir Next&quot;, Avenir, &quot;Helvetica Neue&quot;, sans-serif; font-size:16px;'>",
                        "<p style='margin-top:0px; margin-bottom:1.5rem; font-family:&quot;Avenir Next W01&quot;, &quot;Avenir Next W00&quot;, &quot;Avenir Next&quot;, Avenir, &quot;Helvetica Neue&quot;, sans-serif; font-size:16px;'>",
                        "<p style='margin:0 0 0 0;'>",
                        "<p style='font-family:inherit; font-size:16px; margin-top:0px; margin-bottom:1.5rem;'>",
                        "<p style='margin-top:0px; margin-bottom:0px;'>",
                        "<p style='margin-top:0px; margin-bottom:0px;'><font face='-apple-system, BlinkMacSystemFont, Segoe UI, system-ui, Apple Color Emoji, Segoe UI Emoji, Segoe UI Web, sans-serif'><span style='font-size:14px;'>",
                        "<p style='font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'>",
                        "<p style='font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'>",
                        "<font face='Avenir Next W01, Avenir Next W00, Avenir Next, Avenir, Helvetica Neue, sans-serif'>",
                        "<font face='Avenir Next W01, Avenir Next W00, Avenir Next, Avenir, Helvetica Neue, sans-serif'><span style='font-size:16px;'>",
                        "<font style='font-family:inherit;'>",
                        "<font face='Avenir Next W01, Avenir Next W00, Avenir Next, Avenir, Helvetica Neue, sans-serif' style='font-family:&quot;Avenir Next W01&quot;, &quot;Avenir Next W00&quot;, &quot;Avenir Next&quot;, Avenir, &quot;Helvetica Neue&quot;, sans-serif; font-size:16px;'>",
                        "<font color='rgba(0, 0, 0, 0.9)' face='Segoe UI VSS (Regular), Segoe UI, -apple-system, BlinkMacSystemFont, Roboto, Helvetica Neue, Helvetica, Ubuntu, Arial, sans-serif, Apple Color Emoji, Segoe UI Emoji, Segoe UI Symbol'><span style='font-size:14px;'>",
                        "<font color='#242424' face='-apple-system, BlinkMacSystemFont, Segoe UI, system-ui, Apple Color Emoji, Segoe UI Emoji, Segoe UI Web, sans-serif'>",
                        "<font style='font-family:inherit; font-size:16px;'>",
                        "<div style='max-width:100%; display:inherit;'>",
                        "<div style='font-family:inherit;'>",
                        "<div style='margin-bottom:3rem;'>",
                        "<div style='text-align:Left;'>",
                        "<div style='font-family:&quot;Avenir Next W01&quot;, &quot;Avenir Next W00&quot;, &quot;Avenir Next&quot;, Avenir, &quot;Helvetica Neue&quot;, sans-serif; font-size:16px;'>",
                        "<div style='font-family:&quot;Avenir Next W01&quot;, &quot;Avenir Next W00&quot;, &quot;Avenir Next&quot;, Avenir, &quot;Helvetica Neue&quot;, sans-serif; font-size:16px; max-width:100%; display:inherit;'>",
                        "<div style='box-sizing:border-box; font-family:-apple-system, BlinkMacSystemFont, &quot;Segoe UI&quot;, system-ui, &quot;Apple Color Emoji&quot;, &quot;Segoe UI Emoji&quot;, &quot;Segoe UI Web&quot;, sans-serif; font-size:14px;'>",
                        "<div style='font-family:inherit; font-size:16px;'>",
                        "<div style='font-size:16px; font-family:inherit;'>",
                        "<div style='font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;'>",
                        "<div style='font-family:Avenir Next W01, Avenir Next W00, Avenir Next, Avenir, Helvetica Neue, sans-serif;'><div style='font-size:16px; font-family:inherit;'>",
                        "<div style='font-size:16px; font-family:inherit;'>",
                        "<div style='font-family:Avenir Next W01, Avenir Next W00, Avenir Next, Avenir, Helvetica Neue, sans-serif;'>",
                        "<div style='font-size:16px; font-family:inherit;'>",
                        "</li><li><span style='font-size:medium; font-family:Segoe UI, Arial, sans-serif;'>",
                        "</li><li><span style='font-family:Segoe UI, Arial, sans-serif;'>",
                        "</span>",
                        "<span>",
                        "<span />",
                        "<span >",
                        "<font>",
                        "</font>",
                        "amp;",
                        "<a href=",
                        "rel='nofollow ugc' style='font-family:inherit;' target='_blank'>",
                        "rel='nofollow ugc' style='font-family:Avenir Next, Avenir, Helvetica Neue, Helvetica, Arial, sans-serif; font-size:15px;' target='_blank'>",
                        "rel='nofollow ugc' target='_blank'>",
                        "<a>",
                        "</a>",
                        "<p>",
                        "</p>",
                        "<ul>",
                        "</ul>",
                        "</ul>",
                        "</li>",
                        "&lt;/h4&gt;",
                        "<a target='_blank'>",
                        "&quot;",
                        "&lt;o:p&gt;&lt;/o:p&gt;",
                        "&lt;",
                        "&gt;"
    ]

    for el in remove_el_list:
        in_str = in_str.replace(el, "")
        in_str = in_str.replace(el.upper(), "")
    
    return in_str


# add start and end to string
def find_el_in_string(in_string, start_el, end_el=None):
    
    # declaring variable
    substring_select = ''

    # clean in string
    in_string = remove_rogue_html(in_string)

    if in_string is None:
        return None

    elif end_el is None:
        # set selection based on there being no defined ending to the string
        #print(in_string.split(start_el))
        try:
            substring_select = (in_string.split(start_el))[1]
        except:
            substring_select = ''
        

    elif start_el in in_string:
        # collect the sub string between the two input values
        try:
            substring_select = (in_string.split(start_el))[1].split(end_el)[0]
        except:
            substring_select = ''

    else:
        # sometimes no description tag is put in at the start, so just split by end
        if start_el == 'Description:':
            try:
                substring_select = in_string.split(end_el)[0]
            except:
                substring_select = ''
        else:
            return None

    # clean up big space at start
    substring_select_clean = substring_select.replace('                            ', '')

    # remove unicode characters
    substring_select_clean = unicodedata.normalize("NFKD", substring_select_clean)

    # delete carriage retruns
    substring_select_clean = delete_carriage_returns_string(substring_select_clean)

    # remove leadinga and trailing blank spaces
    substring_select_clean = substring_select_clean.strip()

    return substring_select_clean



def get_basic_info(portal, grp_feat, gis):

    # HTML cleaning - not great but does the job
    tag_re = re.compile(r'(<!--.*?-->|<[^>]*>)')

    print(grp_feat)
    # print(dir(grp_feat))
    # collect/set the variables
    service_id = str(grp_feat.id)
    service_title = grp_feat.title
    service_type = grp_feat.type
    service_group = portal
    service_tags = (','.join(grp_feat.tags))
    service_modified_raw = time.localtime(grp_feat.modified/1000)
    # service_modifed = service_modified_raw.strtime('%Y-%m-%d')
    service_modifed = time.strftime('%Y-%m-%d', service_modified_raw)
    service_created_raw = time.localtime(grp_feat.created/1000)
    service_created = time.strftime('%Y-%m-%d', service_created_raw)
    service_snippet = grp_feat.snippet
    service_summary = grp_feat.snippet
    service_crs = grp_feat.spatialReference
    service_url = 'https://onemap-northsea-uk.bpglobal.com/portal/home/item.html?id={}'.format(service_id)#grp_feat.url
    service_cats_raw = grp_feat.categories
    service_status = grp_feat.content_status

    # clean up cats
    service_cats = ''
    if len(service_cats_raw) > 0:
        for cat in service_cats_raw:
            split_count = cat.count('/')
            if split_count > 1:
                cat_no_first = cat #cat[1:]
                cat_split = cat_no_first.split('/')
                raw_list = cat_split[-1]
                if service_cats =='':
                    service_cats += raw_list
                else:
                    service_cats += ',{}'.format(raw_list)
                    
            else:
                service_cats = cat #cat[1:]
                
    # get portal user name
    portal_owner_id = grp_feat.owner
    portal_owner_user = gis.users.get(username=portal_owner_id)
    portal_owner_full_name = portal_owner_user.fullName
    service_owner = '{} - {}'.format(portal_owner_id, portal_owner_full_name)


    # Remove well-formed tags, fixing mistakes by legitimate users
    # no_tags_desc = tag_re.sub('', str(grp_feat.description))
    no_tags_desc = handle_break(grp_feat.description)
    no_tags_desc = handle_div(no_tags_desc)
    no_tags_desc = handle_para(no_tags_desc)
    no_tags_lic = tag_re.sub('', str(grp_feat.licenseInfo))  
    # Clean up anything else by escaping
    service_description = None
    if no_tags_desc is not None:
        service_description = html.escape(no_tags_desc)
    service_license = None
    if no_tags_lic is not None:
        service_license = html.escape(no_tags_lic)

    # search description text for layer, aprx and source
    lyr_loc = find_el_in_string(no_tags_desc, 'Layer File Location:', 'CRS:')
    aprx_loc = find_el_in_string(no_tags_desc, 'APRX Location:', 'Layer File Location:')
    source_loc = find_el_in_string(no_tags_desc, 'Source:', 'Data Class:')
    date_loc = find_el_in_string(no_tags_desc, 'Data Last Edited:', 'Data Number:')
    desc_loc = find_el_in_string(no_tags_desc, 'Description:', 'Source:', )
    data_num_loc = find_el_in_string(no_tags_desc, 'Data Number:', 'APRX Location:')
    contact_loc = find_el_in_string(no_tags_desc, 'Contact:', 'Portal Owner:')
    rep_wp_loc = find_el_in_string(no_tags_desc, 'Responsible Work Package:', 'Contact:') 
    class_loc = find_el_in_string(no_tags_desc, 'Data Class:', 'Revision:') 
    approve_loc = find_el_in_string(no_tags_desc, 'Data approved by Work Package Manager:', 'Terms of Use:')
    terms_loc = find_el_in_string(no_tags_desc, 'Terms of Use:')
    revision_loc = find_el_in_string(no_tags_desc, 'Revision:', 'Data Last Edited:')
    crs_self_loc = find_el_in_string(no_tags_desc, 'CRS:', 'Responsible Work Package:')


    # check if downloadable
    service_downloadable = 'No'
    if 'DOWNLOADABLE' in service_title.upper():
        service_downloadable = 'Yes'
    
    if service_type == 'Shapefile':
         service_downloadable = 'Yes'    


    # empty variables to be filled later
    service_docloc = ''
    #service_cats = ''

    print(service_title)
    print('   {}'.format(service_type))
    print('   {}'.format(service_owner))
    print('   {}'.format(service_url))

    # filter out maps apps and tools
    if service_type in ('Web Map', 'WMS', 'Web Mapping Application', 
                        'Form', 'Table Layer', 'Data Store', 'Dashboard',
                        'Site Application', 'Geoprocessing Service', 'Code Attachment',
                        'AppBuilder Extension', 'Scene Package', 'Image Service', 'Scene Service',
                        'Image', 'Feature Collection', 'Web Scene', 'File Geodatabase',
                        'Vector Tile Package', 'StoryMap', 'Network Analysis Service', 'WMTS'):
        #print('is a {}'.format(service_type))
            out_list = []
            outlist = [
                        service_id,
                        service_title,
                        service_type, 
                        service_group, 
                        '', 
                        service_created,
                        service_modifed,
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        service_owner,
                        '',
                        service_summary,
                        desc_loc,
                        '',
                        '',
                        '',
                        '',
                        '',
                        terms_loc,
                        service_tags,
                        service_cats,
                        service_status,
                        service_url,
                        no_tags_desc
                        ]

            return outlist

    else:
        #print('is a {}'.format(service_type))
        # check to see if central is onwer as this token doesnt have permision to access details
        
        if service_owner in ('CentralAdmin', 'CentralAdmin - Central Admin', 'esri_livingatlas', 'esri_livingatlas - Esri'):
            #print('is owned by {}'.format(service_owner))
            pass

        # this is a permisions error I cannot get around
        elif service_title in('Ops_Dash_Master_Layers_Forecast', 
                            'Ops_Dash_Master_Layers_Temperature', 
                            'Ops_Dash_Master_Layers_Wave',
                            'Ops_Dash_Master_Layers_Wind') and service_owner == 'wamvg6':
            #print('excluded from analysis manually')
            #print('is owned by {}'.format(service_owner))
            pass
        elif service_type == 'Shapefile':
            # create list of outputs
            out_list = []
            outlist = [
                        service_id,
                        service_title,
                        service_type, 
                        service_group, 
                        service_downloadable, 
                        service_created,
                        service_modifed,
                        date_loc,
                        revision_loc,
                        data_num_loc,
                        class_loc,
                        source_loc,
                        contact_loc,
                        rep_wp_loc,
                        service_owner,
                        approve_loc,
                        service_summary,
                        desc_loc,
                        '',
                        aprx_loc,
                        lyr_loc,
                        service_crs,
                        crs_self_loc,
                        terms_loc,
                        service_tags,
                        service_cats,
                        service_status,
                        service_url,
                        no_tags_desc
                        ]


            return outlist
        else:
            #print('is owned by {}'.format(service_owner))
            try:
                grp_feat_flc = FeatureLayerCollection.fromitem(grp_feat)
            except:
                grp_feat_flc = 'None'
            # print(grp_feat_flc.properties)

            # to get description from the actual metadata from the uploaded data rather than service
            # service_summary = grp_feat_flc.properties.serviceDescription
            # service_description = grp_feat_flc.properties.description
            
            try:
                service_docloc = grp_feat_flc.properties.documentInfo.Title
            except:
                service_docloc = 'None'

            # try:
            #     service_cats = grp_feat_flc.properties.documentInfo.Category
            # except:
            #     service_cats = 'None'

            # create list of outputs
            out_list = []
            outlist = [
                        service_id,
                        service_title,
                        service_type, 
                        service_group, 
                        service_downloadable, 
                        service_created,
                        service_modifed,
                        date_loc,
                        revision_loc,
                        data_num_loc,
                        class_loc,
                        source_loc,
                        contact_loc,
                        rep_wp_loc,
                        service_owner,
                        approve_loc,
                        service_summary,
                        desc_loc,
                        service_docloc,
                        aprx_loc,
                        lyr_loc,
                        service_crs,
                        crs_self_loc,
                        terms_loc,
                        service_tags,
                        service_cats,
                        service_status,
                        service_url,
                        no_tags_desc
                        ]


            return outlist


def format_excel(xl_path, sheet_name, row_count, col_count):
    writer = pd.ExcelWriter(xl_path, engine='xlsxwriter')

    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets[sheet_name]

    cell_format = workbook.add_format({'bold': True, 'font_color': 'red'})
    cell_format = workbook.set_border(style=1)
    title_format = workbook.add_format({'bold': True, 'bg_color': '#999999'})

    start_row = 1
    start_col = 1
    end_row = row_count
    end_col = col_count
    worksheet.conditional_format(start_row, start_col, end_row, end_col, {'format':   cell_format})
    worksheet.conditional_format(start_row, start_col, start_row, end_col, {'format':   title_format})

    writer.save()



def run_extract_info(excel_report_output):

    # credentails
    pw = "pw"
    un = "un"

    portal = r"https://onemap-northsea-uk.bpglobal.com/portal"

     # create basic structure
    df_proc_export = pd.DataFrame(columns=['Service ID',
                                            'Title',
                                            'Type',
                                            'Group',
                                            'Downloadable',
                                            'Date Created',
                                            'Date Last Modified',
                                            'Date Data Was Lasted Edited',
                                            'Revision',
                                            'Data Number',
                                            'Class (1-4)',
                                            'Source',
                                            'Contact(s)',
                                            'Responsible WP',
                                            'Portal Owner',
                                            'Data approved by WPM',
                                            'Summary',
                                            'Description',
                                            'APRX Uploaded From',
                                            'APRX Location',
                                            'Layer File Location',
                                            'CRS Service',
                                            'CRS Self Reported',
                                            'Terms of Use',
                                            'Tags', 
                                            'Categories',
                                            'Status',
                                            'URL',
                                            'Raw Description'])   


    # Log in to the portal
    print('login in...')
    gis = GIS(portal, un, pw)

    # loop through these groups, not all the portal
    portal_list = (['Morgan and Mona'])
    print(portal_list)

    for portal in portal_list:
        # get the group information
        grp = gis.groups.search('title:{}'.format(portal))
        print(grp)
        # loop through features in group
        grp_items = grp[0].content()
        for grp_feat in grp_items:
            service_id = str(grp_feat.id)

            feat_info = get_basic_info(portal, grp_feat, gis)
            
            # append data to the dataframe
            if feat_info is None:
                pass
            else:
                df_proc_export = df_proc_export.append(
                                                        {'Service ID': feat_info[0],
                                                        'Title': feat_info[1],
                                                        'Type': feat_info[2],
                                                        'Group': feat_info[3],
                                                        'Downloadable': feat_info[4],
                                                        'Date Created': feat_info[5],
                                                        'Date Last Modified': feat_info[6],
                                                        'Date Data Was Lasted Edited': feat_info[7],
                                                        'Revision': feat_info[8],
                                                        'Data Number': feat_info[9],
                                                        'Class (1-4)': feat_info[10],
                                                        'Source': feat_info[11],
                                                        'Contact(s)': feat_info[12],
                                                        'Responsible WP': feat_info[13],
                                                        'Portal Owner': feat_info[14],
                                                        'Data approved by WPM': feat_info[15],
                                                        'Summary': feat_info[16],
                                                        'Description': feat_info[17],
                                                        'APRX Uploaded From': feat_info[18],
                                                        'APRX Location': feat_info[19],
                                                        'Layer File Location': feat_info[20],
                                                        'CRS Service': feat_info[21],
                                                        'CRS Self Reported': feat_info[22],
                                                        'Terms of Use': feat_info[23],
                                                        'Tags': feat_info[24],
                                                        'Categories': feat_info[25],
                                                        'Status': feat_info[26],
                                                        'URL': feat_info[27],
                                                        'Raw Description': feat_info[28]}, ignore_index=True)

        
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #    print(df_proc_export)


    # filter the dataframe
    service_tag_list = ['Map Service', 'Feature Service', 'WMS', 'WMTS']
    df_proc_services = df_proc_export[df_proc_export.Type.isin(service_tag_list)]

    downloads_tag_list = ['Shapefile']
    df_proc_downloads = df_proc_export[df_proc_export.Type.isin(downloads_tag_list)]    

    maps_tag_list = ['Web Map']
    df_proc_maps = df_proc_export[df_proc_export.Type.isin(maps_tag_list)]    

    tools_tag_list = ['Site Application', 'Web Mapping Application', 'Code Attachment', 'Geoprocessing Service', 'Dashboard', 'Form', 'Data Store']
    df_proc_tools = df_proc_export[df_proc_export.Type.isin(tools_tag_list)]   

    writer = pd.ExcelWriter(excel_report_output)
    df_proc_services.to_excel(writer, sheet_name = 'Services', index=False, header=True)
    df_proc_downloads.to_excel(writer, sheet_name = 'Downloadable', index=False, header=True)
    df_proc_maps.to_excel(writer, sheet_name = 'Maps', index=False, header=True)
    df_proc_tools.to_excel(writer, sheet_name = 'Apps and Tools', index=False, header=True)
    writer.save()

def rename_and_copy(excel_report_output, out_folder):


    rename_excel_report_output = ''
    prev_week_info = ''

    if os.path.isfile(excel_report_output):
        print('Make copy and rename')
        # get date and week for report output name (date last modified)
        date_mod = datetime.datetime.fromtimestamp(os.path.getmtime(excel_report_output))
        yyyy_date = date_mod.strftime('%Y%m%d')
        week_no = date_mod.isocalendar()[1]
        ms = date_mod.strftime('%H%M')
        prev_week_info = '{}_{}_Week_{}'.format(yyyy_date, ms, week_no)

        # make a copy of the report with new name
        rename_out_file_name = '{}_MoMo_MetaDataReport.xlsx'.format(prev_week_info)
        rename_excel_report_output = os.path.join(out_folder,rename_out_file_name)
        #os.rename(excel_report_output, rename_excel_report_output)
        shutil.copy(excel_report_output,rename_excel_report_output)
    
    return rename_excel_report_output, prev_week_info


def run_comparison_info(excel_report_output, rename_excel_report_output, prev_week_info, out_comp_folder):
    # Run the comparison
    # get current data info
    date_mod = datetime.datetime.fromtimestamp(os.path.getmtime(excel_report_output))
    yyyy_date = date_mod.strftime('%Y%m%d')
    week_no = date_mod.isocalendar()[1]
    ms = date_mod.strftime('%H%M')
    cur_week_info = '{}_{}_Week_{}'.format(yyyy_date, ms, week_no)


    # report output
    os.path.join(out_comp_folder, '{}_Compairson_{}.xlsx'.format(cur_week_info, prev_week_info))
    excel_export_comparison = os.path.join(out_comp_folder, '{}_Compairson_{}.xlsx'.format(cur_week_info, prev_week_info))


    # compare the excels on a certain sheet
    writer = pd.ExcelWriter(excel_export_comparison)
    compare_excel(excel_report_output, rename_excel_report_output, 'Services', writer)
    compare_excel(excel_report_output, rename_excel_report_output, 'Downloadable', writer)   
    writer.save()
    
    return excel_export_comparison

# Main Function
def main():
    
    # get date and week for report output name
    # report output
    #excel_report_output = r'C:\Users\9325pb\OneDrive - BP\NorthSea\MoMo\Data\Portal Data Links\Weekly List\{}_Week_{}_MoMo_MetaDataReport.xlsx'.format(yyyy_date, week_no)
    out_folder_weekly = r'\\aadanfusw0-fb3b\Digital\dataWorx\Geospatial\Region\NorthSea\Geospatial\Scratch\GMcLachlan\Projects\MoMo\Docs\Data_Lists\Weekly_Lists'
    out_folder_main = r'\\aadanfusw0-fb3b\Digital\dataWorx\Geospatial\Region\NorthSea\Geospatial\Scratch\GMcLachlan\Projects\MoMo\Docs\Data_Lists\Meta_Data_Report'
    out_file_name = 'MoMo_MetaDataReport.xlsx'
    out_comp_folder =  r'\\aadanfusw0-fb3b\Digital\dataWorx\Geospatial\Region\NorthSea\Geospatial\Scratch\GMcLachlan\Projects\MoMo\Docs\Data_Lists\Comparison_List'

    # the full metadata report location
    excel_report_output = os.path.join(out_folder_main,out_file_name)
    
    # create a copy of current report, renaming it
    rename_excel_report_output, prev_week_info = rename_and_copy(excel_report_output, out_folder_weekly)

    # extract the new 
    run_extract_info(excel_report_output)

    # run comparison
    excel_export_comparison = run_comparison_info(excel_report_output, rename_excel_report_output, prev_week_info, out_comp_folder)
    


if __name__ == "__main__":
    
    print('starting analysis')
    now = datetime.datetime.now()
    print("time started:", now)
    
    main()

    print('completed')
    now = datetime.datetime.now()
    print("time finished:", now)
