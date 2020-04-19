import json
import copy
#export _JAVA_OPTIONS="-Xmx256M"

countriesDictionary = {}
monthDictionary = {"0": "Jan", "1": "Feb", "2": "Mar", "3": "Apr", "4": "May", "5": "Jun", "6": "Jul", "7": "Aug", "8": "Sep", "9": "Oct", "10": "Nov", "11": "Dec"}

with open('./countries.txt') as f: #make dictionary associating country codes with country names
    for line in f:
        countryCode = line[0] + line[1]
        if line[-2] == ' ':
            countryName = line[3:-2]
        else:
            countryName = line[3:-1]
        countriesDictionary[countryCode] = countryName

countriesDictionary['FO'] = 'Faroe Islands' #hardcoding in because not present in provided country list

dataDictionary = {}
for k, v in countriesDictionary.items():
    dataDictionary[k] = {} #{AC: {}, US: {}, ...}

#print(templateDictionary)

for k, v in dataDictionary.items():
    tempDic = {}
    templateDictionary = {}
    year = 1900
    while year < 2021:
        templateDictionary["country"] = k
        templateDictionary["year"] = str(year)
        templateDictionary["months"] = [[], [], [], [], [], [], [], [], [], [], [], []] #array for all measurements in that month in that year
        tempDic[str(year)] = copy.deepcopy(templateDictionary)
        #templateDictionary[str(year)] = [[], [], [], [], [], [], [], [], [], [], [], []] #array for all measurements in that month in that year
        #templateDictionary[str(year)] = {"Jan": [], "Feb": [], "Mar": [], "Apr": [], "May": [], "Jun": [], "Jul": [], "Aug": [], "Sep": [], "Oct": [], "Nov": [], "Dec": []} #array for all measurements in that month in that year
        year += 1
    dataDictionary[k] = copy.deepcopy(tempDic)#dict(templateDictionary)
    #dataDictionary[k]["country"] = k

#print(dataDictionary)

with open('./ghcnm.tavg.v4.0.1.20200407.qcf.dat') as datFile:
    for line in datFile:
        stationID = line[2:12] #gives station ID, not used
        operatingOn = line[0] + line[1] #gives country code/dictionary key
        year = line[11:15]
        startMonthIndex = 19
        endMonthIndex = 24
        for m in range(0, 12): #12 months
            #month = str(m)
            monthTempWithSpaces = line[startMonthIndex:endMonthIndex]
            monthTemp = ""
            for c in monthTempWithSpaces:
                if c != ' ':
                    monthTemp += c
            #if (int(year) >= 1900):
                #dataDictionary[operatingOn][year]["country"] = operatingOn
            if (int(year) >= 1900) and (monthTemp != "-9999"):
                print(operatingOn + " | " + year + " | " + str(m))
                dataDictionary[operatingOn][year]["months"][m].append(int(monthTemp)/100.0)
                #dataDictionary[operatingOn][year][monthDictionary[str(m)]].append(int(monthTemp)/100.0)
                #dataDictionary[operatingOn][year][m].append(int(monthTemp)/100.0) #divide by 100.0 to get whole degrees C as per readme.txt
            startMonthIndex += 8
            endMonthIndex += 8
            
            

'''with open('TemperatureData.json', 'a') as f:
    for k, v in dataDictionary.items():
        for key, val in v.items(): #for each year
            f.write(json.dumps(val) + '\n')'''
            
def isAllNull(months):
    #check = True
    for x in months:
        if x != []:
            return False
    return True

with open('TemperatureData2.json', 'a') as f:
    for k, v in dataDictionary.items():
        for key, val in v.items(): #for each year
            if isAllNull(val['months']) != True:
                f.write(json.dumps(val) + '\n')
"""
for k, v in dataDictionary.items():
    with open('./DataByCountry/' + k + '.json', 'a') as f:
        f.truncate()
        for key, val in v.items(): #for each year
            #tempDic = {}
            #tempDic[key] = val
            #f.write(json.dumps(tempDic) + '\n')
            f.write(json.dumps(val) + '\n')
"""



#with open('TemperatureData.json', 'w') as f:
#    json.dump(dataDictionary, f, indent=4, sort_keys=True)

#{"year": "1900", "months": [[]]}
