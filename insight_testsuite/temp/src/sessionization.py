import pandas as pd
from pathlib import Path
import datetime
import sys

def get_time_diff(timeValues):
    '''Function to calculate time difference between successive records'''
    j=len(timeValues)
    newValues=[]
    newValues.append(0)
    for i in range(1,j):
        newValues.append((datetime.datetime.strptime(timeValues[i],'%H:%M:%S') - datetime.datetime.strptime(timeValues[i-1],'%H:%M:%S')).total_seconds())
    return newValues

def get_active_state(timediff,sessionTimeout):
    '''Function to see if a session is active. This uses the sessionTimeout variable'''
    stateValues=[]
    stateValues.append(False)
    for i in range(1,len(timediff)):
        if timediff[i]<float(sessionTimeout):
            stateValues.append(False)
        else:
            stateValues.append(True)
    return stateValues
def check_inputs(inputPath, filename, outputPath):
    '''Function that checks if all required inputs are present and are valid'''
    if not Path(inputPath).is_dir():
        print("Input Path does not exist.. Please provide a valid directory path")
        return False
    if not Path(outputPath).is_dir():
        print("Output Path does not exist.. Please provide a valid directory path")
        return False
    if not Path(inputPath+'/input/'+filename).is_file:
        print("File does not exist.. Please provide a valid file name")
        return False
    if not Path(inputPath+'/input/'+'inactivity_period.txt').is_file:
        print("Inactivity period file does not exist in given directory.. \n Please provide a path where the input file and inactivity file exist")
        return False
    return True

def get_session_timeout(inputPath):
    '''Function that accesses value from inactivity_period.txt'''
    return open(inputPath + '/input/' + 'inactivity_period.txt').read()

def create_output_file(outputPath):
    '''Creates an output file to write the desired output'''
    return open(outputPath+'/output/sessionization.txt','w')
def write_line(file,str):
    ''''Prints a line into the output file'''
    file.write(str+'\n')
    return
def process_log(inputPath, filename, outputPath):
    '''Reads the log file from the input path and categorizes the records into sessions'''
    if not check_inputs(inputPath, filename, outputPath):
        print("Invalid inputs!! Program terminated")
        return

    sessionTimeout = get_session_timeout(inputPath)
    outputFile=create_output_file(outputPath)

    inputRequests = pd.read_csv(inputPath+'/input/'+filename)
    ipaddresses = inputRequests.groupby(['ip'])

    startIndex = 0
    sessionSummary=[]
    pendingSummary=[]

    for name,groups in ipaddresses:

        dateval=groups['date'].values
        timeval=groups['time'].values
        timediff=get_time_diff(groups['time'].values)
        timeout=get_active_state(timediff,sessionTimeout)
        cik=groups['cik'].values
        startIndex=0
        for j in range(len(groups)):
            if timeout[j]:
                startTime=dateval[startIndex] + ' ' + timeval[startIndex]
                endTime=dateval[j-1] + ' ' + timeval[j-1]
                duration=(datetime.datetime.strptime(endTime,'%Y-%m-%d %H:%M:%S')-datetime.datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S')).total_seconds()+1
                pagecount=j-1-startIndex
                sessionSummary.append([name,startTime,endTime,duration,pagecount])
                startIndex=j

    sortedSummary=sorted(sessionSummary,key=lambda t: datetime.datetime.strptime(str(t[2]),'%Y-%m-%d %H:%M:%S'),reverse=False)

    for record in sortedSummary:
        write_line(outputFile, str(record[0])+','+str(record[1])+','+str(record[2])+','+str(record[3])+','+str(record[4]))
    for record in pendingSummary:
        write_line(outputFile, str(record[0])+','+str(record[1])+','+str(record[2])+','+str(record[3])+','+str(record[4]))

#### Getting the input filepath and name
inputPath = sys.argv[1]
filename = sys.argv[2]
outputPath = sys.argv[3]

print('Input Path:',inputPath)
print('filename:',filename)
print('Output Path:', outputPath)
##### Starting to process the log
process_log(inputPath,filename,outputPath)

