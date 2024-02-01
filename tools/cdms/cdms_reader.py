# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import string
from netCDF4 import Dataset, num2date
import sys
import datetime
import csv
from collections import OrderedDict
import logging


#TODO: Get rid of numpy errors?
#TODO: Update big SDAP README

LOGGER =  logging.getLogger("cdms_reader")

def assemble_matches(filename):
    """
    Read a CDMS netCDF file and return a list of matches.
    
    Parameters
    ----------
    filename : str
        The CDMS netCDF file name.
    
    Returns
    -------
    matches : list
        List of matches. Each list element is a dictionary.
        For match m, netCDF group GROUP (PrimaryData or SecondaryData), and
        group variable VARIABLE:
        matches[m][GROUP]['matchID']: MatchedRecords dimension ID for the match
        matches[m][GROUP]['GROUPID']: GROUP dim dimension ID for the record
        matches[m][GROUP][VARIABLE]: variable value 
    """
   
    try:
        # Open the netCDF file

        with Dataset(filename, 'r') as cdms_nc:
            # Check that the number of groups is consistent w/ the MatchedGroups
            # dimension
            assert len(cdms_nc.groups) == cdms_nc.dimensions['MatchedGroups'].size,\
                ("Number of groups isn't the same as MatchedGroups dimension.")
            
            matches = []
            matched_records = cdms_nc.dimensions['MatchedRecords'].size
           
            # Loop through the match IDs to assemble matches
            for match in range(0, matched_records):
                match_dict = OrderedDict()
                # Grab the data from each platform (group) in the match
                for group_num, group in enumerate(cdms_nc.groups):
                    match_dict[group] = OrderedDict()
                    match_dict[group]['matchID'] = match
                    ID = cdms_nc.variables['matchIDs'][match][group_num]
                    match_dict[group][group + 'ID'] = ID
                    for var in cdms_nc.groups[group].variables.keys():
                        match_dict[group][var] = cdms_nc.groups[group][var][ID]
    
                    # Create a UTC datetime field from timestamp
                    dt = num2date(match_dict[group]['time'],
                                  cdms_nc.groups[group]['time'].units)
                    match_dict[group]['datetime'] = dt
                LOGGER.info(match_dict)
                matches.append(match_dict)
            
            return matches
    except (OSError, IOError) as err:
        LOGGER.exception("Error reading netCDF file " + filename)
        raise err

def assemble_matches_by_primary(filename):
    """
    Read a CDMS netCDF file and return a list of matches, in which secondary data
    points are grouped together by their primary data point match.
   
    This function returns matches in a different order than the 'assemble_matches' function.
    In this function, all secondary data is associated with its primary match without the need
    to access multiple matches. 

    Parameters
    ----------
    filename : str
        The CDMS netCDF file name.
    
    Returns
    -------
    matches : list
        List of matches. Each list element is a dictionary that maps a primary record to all of its associated secondary records.
        For match m, netCDF group GROUP (PrimaryData or SecondaryData), and
        group variable VARIABLE:

        matches[m][GROUP]['matchID']: MatchedRecords dimension ID for the match
        matches[m][GROUP]['GROUPID']: GROUP dim dimension ID for the record
        matches[m][GROUP][VARIABLE]: variable value. Each VARIABLE is returned as a masked array. 

        ex. To access the first secondary time value available for a given match:
            matches[m]['SecondaryData']['time'][0]
    """
   
    try:
        # Open the netCDF file
        with Dataset(filename, 'r') as cdms_nc:
            # Check that the number of groups is consistent w/ the MatchedGroups
            # dimension
            assert len(cdms_nc.groups) == cdms_nc.dimensions['MatchedGroups'].size,\
                ("Number of groups isn't the same as MatchedGroups dimension.")
           
            matched_records = cdms_nc.dimensions['MatchedRecords'].size
            primary_matches = cdms_nc.groups['PrimaryData'].dimensions['dim'].size
            matches = [OrderedDict()] * primary_matches

            for match in range(matched_records):
                PID = int(cdms_nc.variables['matchIDs'][match][0])
        
                if len(matches[PID]) == 0: #establishes ordered dictionary for first match[PID]
                    matches[PID] = OrderedDict()

                for group_num, group in enumerate(cdms_nc.groups):
                    
                    if group_num == 0: #primary
                        
                        if group not in matches[PID].keys(): #initialization
                                matches[PID][group] = OrderedDict()
                                matches[PID][group]['matchID'] = []

                        matches[PID][group]['matchID'].append(match)
                        ID = cdms_nc.variables['matchIDs'][match][group_num]
                        matches[PID][group][group + 'ID'] = ID

                        for var in cdms_nc.groups[group].variables.keys():
                            matches[PID][group][var] = cdms_nc.groups[group][var][ID]
                        
                        dt = num2date(matches[PID][group]['time'], cdms_nc.groups[group]['time'].units)
                        matches[PID][group]['datetime'] = dt

                    elif group_num == 1: #secondary

                        if group not in matches[PID].keys(): #initialization
                            matches[PID][group] = OrderedDict()
                            matches[PID][group]['matchID'] = []
                            matches[PID][group][group + 'ID'] = []
                            matches[PID][group]['datetime'] = []
                        
                        matches[PID][group]['matchID'].append(match)
                        ID = cdms_nc.variables['matchIDs'][match][group_num]
                        matches[PID][group][group + 'ID'].append(ID)
                        
                        for var in cdms_nc.groups[group].variables.keys():
                            if var not in matches[PID][group].keys():
                                matches[PID][group][var] = []
                            matches[PID][group][var].append(cdms_nc.groups[group][var][ID])

                        dt = num2date(matches[PID][group]['time'], cdms_nc.groups[group]['time'].units)
                        matches[PID][group]['datetime'].append(dt[0])
                 
            return matches
    except (OSError, IOError) as err:
        LOGGER.exception("Error reading netCDF file " + filename)
        raise err

def matches_to_csv(matches, csvfile):
    """
    Write the CDMS matches to a CSV file. Include a header of column names
    which are based on the group and variable names from the netCDF file.
    
    Parameters
    ----------
    matches : list
        The list of dictionaries containing the CDMS matches as returned from
        assemble_matches.      
    csvfile : str
        The name of the CSV output file.
    """
    # Create a header for the CSV. Column names are GROUP_VARIABLE or
    # GROUP_GROUPID.
    header = []
    for key, value in matches[0].items():
        for otherkey in value.keys():
            header.append(key + "_" + otherkey)
    
    try:
        # Write the CSV file
        with open(csvfile, 'w') as output_file:
            csv_writer = csv.writer(output_file)
            csv_writer.writerow(header)
            for match in matches:
                row = []
                for group, data in match.items():
                    for value in data.values():
                        row.append(value)
                csv_writer.writerow(row)
    except (OSError, IOError) as err:
        LOGGER.exception("Error writing CSV file " + csvfile)
        raise err

def get_globals(filename):
    """
    Write the CDMS  global attributes to a text file. Additionally,
     within the file there will be a description of where all the different
     outputs go and how to best utlize this program.
    
    Parameters
    ----------      
    filename : str
        The name of the original '.nc' input file.
    
    """
    x0 = "README / cdms_reader.py Program Use and Description:\n"
    x1 = "\nThe cdms_reader.py program reads a CDMS netCDF (a NETCDF file with a matchIDs variable)\n"
    x2 = "file into memory, assembles a list of matches of primary and secondary data\n"
    x3 = "and optionally\n"
    x4 = "output the matches to a CSV file. Each matched pair contains one primary\n"
    x5 = "data record and one secondary data record.\n"
    x6 = "\nBelow, this file wil list the global attributes of the .nc (NETCDF) file.\n"
    x7 = "If you wish to see a full dump of the data from the .nc file,\n"
    x8 = "please utilize the ncdump command from NETCDF (or look at the CSV file).\n"
    try:
        with Dataset(filename, "r", format="NETCDF4") as ncFile:
            txtName = filename.replace(".nc", ".txt")
            with open(txtName, "w") as txt:
                txt.write(x0 + x1 +x2 +x3 + x4 + x5 + x6 + x7 + x8)
                txt.write("\nGlobal Attributes:")
                for x in ncFile.ncattrs():
                    txt.write(f'\t :{x} = "{ncFile.getncattr(x)}" ;\n')


    except (OSError, IOError) as err:
        LOGGER.exception("Error reading netCDF file " + filename)
        print("Error reading file!")
        raise err

def create_logs(user_option, logName):
    """
    Write the CDMS log information to a file. Additionally, the user may
    opt to print this information directly to stdout, or discard it entirely.
    
    Parameters
    ----------      
    user_option : str
        The result of the arg.log 's interpretation of
         what option the user selected.
    logName : str
        The name of the log file we wish to write to,
        assuming the user did not use the -l option.
    """
    if user_option == 'N':
        print("** Note: No log was created **")


    elif user_option == '1':
        #prints the log contents to stdout
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[
                            logging.StreamHandler(sys.stdout)
                            ])
                
    else:
        #prints log to a .log file
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[
                            logging.FileHandler(logName)
                            ])
        if user_option != 1 and user_option != 'Y':
            print(f"** Bad usage of log option. Log will print to {logName} **")

    


if __name__ == '__main__':
    """
    Execution:
        python cdms_reader.py filename
        OR
        python3 cdms_reader.py filename 
        OR
        python3 cdms_reader.py filename -c -g 
        OR
        python3 cdms_reader.py filename --csv --meta

    Note (For Help Try):
            python3 cdms_reader.py -h
            OR
            python3 cdms_reader.py --help

    """
   
    u0 = '\n%(prog)s -h OR --help \n'
    u1 = '%(prog)s filename -c -g\n%(prog)s filename --csv --meta\n'
    u2 ='Use -l OR -l1 to modify destination of logs'
    p = argparse.ArgumentParser(usage= u0 + u1 + u2)

    #below block is to customize user options
    p.add_argument('filename', help='CDMS netCDF file to read')
    p.add_argument('-c', '--csv', nargs='?', const= 'Y', default='N',
     help='Use -c or --csv to retrieve CSV output')
    p.add_argument('-g', '--meta', nargs='?', const='Y', default='N',
     help='Use -g or --meta to retrieve global attributes / metadata')
    p.add_argument('-l', '--log', nargs='?', const='N', default='Y',
     help='Use -l or --log to AVOID creating log files, OR use -l1 to print to stdout/console') 

    #arguments are processed by the next line
    args = p.parse_args()

    logName = args.filename.replace(".nc", ".log")
    create_logs(args.log, logName)
    
    cdms_matches = assemble_matches(args.filename)

    if args.csv == 'Y' :
        matches_to_csv(cdms_matches, args.filename.replace(".nc",".csv"))

    if args.meta == 'Y' :
        get_globals(args.filename)

