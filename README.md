# Assignment 2 - CIS4010 Cloud Computing
## Yousif Jamal; yjamal; 1160861

### Overview
- 0 Changes have been made to the original .csv files - they should work based on the 06 Feb 22 version that I downloaded off of courselink
- A2Functions.py does all the actual legwork of the program, A2Main.py just connects to aws and runs the while loop/takes user input
- All Assignment functionality is present
- Create Table takes ~2 mins as it needs to ensure that all tables are made and considered "active" before it can allow any further manipulation
- You can utilize all modules through the interactive menu, it prompts you for all necessary commands it requires


### Requirements
- All contained in the requirements.txt file
- Python 3.10 or later
- packages: boto3, pandas (pip3.10 install boto3 pip install pandas), tabulate
- S5-S3.conf with your AWS credentials contained within the same directory as the source files
- ensure you "chmod u+x A2Main.py" In order to run the program (with the requirements already downloaded through pip3.10 & the source files in the right place)



### REPORTS
- Given the option to either have a PDF or display on Cmd Line by Prof - I chose the Command line
- it's assumed that Years correspond between all tables. So an addition of 1969 population for Argentina implies an addition of 1969 GDP 
- Report A may take upwards of 3 minutes to calculate the population depending on your dataset (this is because it has to compare the derived
population density value against all other possible density values of that year, for every single year globally - thus it must calculate all 
densities for all countries for all years, then rank them) - it was 1:30 - 2minutes give or take with a 40 country dataset
- The Economics should display shortly thereafter as it isn't as computationally intensive.
- Report B is pretty fast, wont have the same wait as report A.
- Report B only asks for a year to display the appropriate information for the gdppc, population & density


### Table Schemas for adding individual records (what you need to input (all fields necessary) when prompted for JSON Add):
Once you enter the country and table, you'll be prompted with JSON, here are your options (include all fields unless told otherwise):
- yjamal_shortlist_area: {'ISO3': 'yourInput', 'Area' : 'yourInput'}
- yjamal_shortlist_capitals: {'ISO3': 'yourInput', 'Capital' : 'yourInput'}
- yjamal_shortlist_languages: {'ISO3': 'yourInput', 'Langauges' : 'yourInput'} ; when adding a list of languages list the value as follows:
{'Languages': ['Language1','Language2']}
- yjamal_un_shortlist: {'ISO3': 'yourInput', 'Full Country Name' : 'yourInput', 'ISO2' : 'yourInput'}
- yjamal_shortlist_curpop: {'Currency': 'yourInput', '1970':'yourInput', ..., '2019' : 'yourInput'}; Any years not input will be autofilled with <empty>
- yjamal_shortlist_gdppc: { '1970':'yourInput', ..., '2019' : 'yourInput'}; Any years not input will be autofilled with <empty>

### Updating Individual Records
- type in 'Update' in the Manual add/delete/update menu (no apostrophes) and then enter the requisite information.
- For the JSON, you only have to enter in one of the attribute names (or a new attribute name) like so {'NewOrOldAttribute':'NewValue'}, unlike 
for addition of a new row where you needed all attributes (except for the year-dependant tables)
- When updating languages and inserting more than one, do so as follows: {'Languages': ['Language1','Language2']} else just {'Languages':'newValue'}