# CDMS_reader.py
The functions in cdms_reader.py read a CDMS netCDF file into memory, assemble a list of matches from a primary (satellite) and secondary (satellite or in situ) data set, and optionally outputs the matches to a CSV file. Each matched pair contains one primary data record and one in secondary data record.

The CDMS netCDF files holds the two groups (`PrimaryData` and `SecondaryData`). The `matchIDs` netCDF variable contains pairs of IDs (matches) which reference a primary data record and a secondary data record in their respective groups. These records have a many-to-many relationship; one primary record may match to many in secondary records, and one secondary record may match to many primary records. The `assemble_matches` function assembles the individual data records into pairs based on their `dim` group dimension IDs as paired in the `matchIDs` variable.

## Requirements
This tool was developed and tested with Python 3.9.13.
Imported packages:
* argparse
* string
* netcdf4
* sys
* datetime
* csv
* collections
* logging

## Functions
### Function: `assemble_matches(filename)`
Read a CDMS netCDF file into memory and return a list of matches from the file.

#### Parameters 
- `filename` (str): the CDMS netCDF file name.
    
#### Returns
- `matches` (list): List of matches.

Each list element in `matches` is a dictionary organized as follows:
	For match `m`, netCDF group `GROUP` ('PrimaryData' or 'SecondaryData'), and netCDF group variable `VARIABLE`:

`matches[m][GROUP]['matchID']`: netCDF `MatchedRecords` dimension ID for the match
`matches[m][GROUP]['GROUPID']`: GROUP netCDF `dim` dimension ID for the record
`matches[m][GROUP][VARIABLE]`: variable value 

For example, to access the timestamps of the primary data and the secondary data of the first match in the list, along with the `MatchedRecords` dimension ID and the groups' `dim` dimension ID:
```python
matches[0]['PrimaryData']['time']
matches[0]['SecondaryData']['time']
matches[0]['PrimaryData']['matchID']
matches[0]['PrimaryData']['PrimaryDataID']
matches[0]['SecondaryData']['SecondaryDataID']
```

Data is returned in one-to-one matches (ex. matches[0] returns one secondary record associated with its primary match)

### Function: `assemble_matches_by_primary(filename)`
Read a CDMS netCDF file and return a list of matches, in which secondary data points are grouped together by their primary data point match.
This function is intended to return results in a similar fashion as the original `assemble_matches` function, however the number of results and formatting of the data is different. Namely, all secondary is associated with its primary match and can be accessed without having to reach across multiple matches.   

#### Parameters 
- `filename` (str): the CDMS netCDF file name.
    
#### Returns
- `matches` (list): Three dimensional array of matches. Matches are returned as primary records grouped together with all associated secondary records.  

Each list element in `matches` is an array that contains the following sub-elements:
	For match `m` (indexed on the number of 'PrimaryData' points), netCDF group `GROUP` ('PrimaryData' or 'SecondaryData'), and netCDF group vraiable `VARIABLE`: 

`matches[m][GROUP]['matchID']`: netCDF `MatchedRecords` dimension ID for each match. Returned as an ordered array.
`matches[m][GROUP]['GROUPID']`: GROUP netCDF `dim` dimension ID for each record. Returned as a single element for 'PrimaryData' and an ordered array for 'SecondaryData'
`matches[m][GROUP][VARIABLE]`: variable value. Returned as an ordered array. 

For example, to access the timestamps of the primary data and the secondary data of the first match in the list, along with the `MatchedRecords` dimension ID and the groups' `dim` dimension ID:
```python
matches[0]['PrimaryData']['time']
matches[0]['SecondaryData']['time'] (returns an array of secondary time values, with order preserved)
matches[0]['PrimaryData']['matchID']
matches[0]['PrimaryData']['PrimaryDataID']
matches[0]['SecondaryData']['SecondaryDataID'] (returns an array of secondary data ID values, with order preserved)
```

Data is returned in one-to-many matches (ex. matches[0] returns an array of secondary record data associates with its primary match)


### Function: `matches_to_csv(matches, csvfile)`
Write the CDMS matches to a CSV file. Include a header of column names which are based on the group and variable names from the netCDF file.
    
#### Parameters:
- `matches` (list): the list of dictionaries containing the CDMS matches as returned from the `assemble_matches` function.
- `csvfile` (str): the name of the CSV output file.

### Function: `get_globals(filename)`
Write the CDMS global attributes to a text file. Additionally,
within the file there will be a description of where all the different
outputs go and how to best utlize this program.

#### Parameters:
- `filename` (str): the name of the original '.nc' input file

### Function: `create_logs(user_option, logName)`
Write the CDMS log information to a file. Additionally, the user may
opt to print this information directly to stdout, or discard it entirely.

#### Parameters
- `user_option` (str): The result of the arg.log 's interpretation of
what option the user selected.
- `logName` (str): The name of the log file we wish to write to,
assuming the user did not use the -l option.

## Usage
For example, to read some CDMS netCDF file called `cdms_file.nc`:
### Command line
The main function for `cdms_reader.py` takes one `filename` parameter (`cdms_file.nc` argument in this example) for the CDMS netCDF file to read and calls the `assemble_matches` function. If the -c parameter is utilized, the `matches_to_csv` function is called to write the matches to a CSV file `cdms_file.csv`. If the -g parameter is utilized, the `get_globals` function is called to show them the files globals attributes as well as a short explanation of how the files can be best utlized. Logs of the program are kept automatically in `cdms_file.log` but can be omitted or rerouted with the -l parameter. P.S. when using the --csv, --log, or --meta options, these are the same three commands but --log cannot take any parameters like its' recommended syntax (-l) does.
```
python cdms_reader.py cdms_file.nc -c -g
```
python3 cdms_reader.py cdms_file.nc -c -g
```
python3 cdms_reader.py cdms_file.nc --csv --meta
### Importing `assemble_matches`
```python
from cdms_reader import assemble_matches
matches = assemble_matches('cdms_file.nc')
```
