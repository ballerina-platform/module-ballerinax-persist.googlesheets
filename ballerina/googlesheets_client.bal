// Copyright (c) 2023 WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/persist;
import ballerina/url;
import ballerina/http;
import ballerina/time;
import ballerina/lang.regexp;
import ballerinax/googleapis.sheets;

type Table record {
    map<json>[] cols;
    RowValues[] rows;
    int parsedNumHeaders;
};

type RowValues record {
    map<json>[] c;
};

# The client used by the generated persist clients to abstract and
# execute API calls that are required to perform CRUD operations.
public isolated client class GoogleSheetsClient {

    private final sheets:Client googleSheetClient;
    private final http:Client httpSheetsClient;
    private final http:Client httpAppScriptClient;
    private final string spreadsheetId;
    private final int sheetId;
    private final string entityName;
    private final string tableName;
    private final string range;
    private final map<SheetFieldMetadata> & readonly fieldMetadata;
    private final map<string> & readonly dataTypes;
    private final string[] & readonly keyFields;
    private isolated function (string[]) returns stream<record {}, persist:Error?>|persist:Error query;
    private isolated function (anydata) returns record {}|persist:Error queryOne;
    private final (map<isolated function (record {}, string[]) returns record {}[]|persist:Error>) & readonly associationsMethods;

    # Initializes the `GSheetClient`.
    #
    # + googleSheetClient - The `sheets:Client`, which is used to execute google sheets operations
    # + httpSheetsClient - The `http:Client`, which is used to execute http requests
    # + sheetMetadata - Metadata of the entity
    # + spreadsheetId - Id of the spreadsheet
    # + sheetId - Id of the sheet
    # + return - A `persist:persist:Error` if the client creation fails
    public isolated function init(sheets:Client googleSheetClient, http:Client httpSheetsClient, http:Client httpAppScriptClient, SheetMetadata & readonly sheetMetadata, string & readonly spreadsheetId, int & readonly sheetId) returns persist:Error? {
        self.entityName = sheetMetadata.entityName;
        self.spreadsheetId = spreadsheetId;
        self.tableName = sheetMetadata.tableName;
        self.fieldMetadata = sheetMetadata.fieldMetadata;
        self.range = sheetMetadata.range;
        self.httpSheetsClient = httpSheetsClient;
        self.keyFields = sheetMetadata.keyFields;
        self.googleSheetClient = googleSheetClient;
        self.httpAppScriptClient = httpAppScriptClient;
        self.dataTypes = sheetMetadata.dataTypes;
        self.query = sheetMetadata.query;
        self.queryOne = sheetMetadata.queryOne;
        self.associationsMethods = sheetMetadata.associationsMethods;
        self.sheetId = sheetId;
    }

    # Performs an append operation to insert entity instances into a table.
    #
    # + insertRecords - The entity records to be inserted into the table
    # + return - Error on failure, or `()` on success
    public isolated function runBatchInsertQuery(record {}[] insertRecords) returns persist:Error? {
        string[] fieldMetadataKeys = self.fieldMetadata.keys();
        foreach record {} rowValues in insertRecords {
            string metadataValue = self.generateMetadataValue(self.keyFields, rowValues);
            SheetBasicType[] values = [];
            foreach string key in fieldMetadataKeys {
                string dataType = self.dataTypes.get(key).toString();
                if dataType == "time:Date" || dataType == "time:TimeOfDay" || dataType == "time:Civil" || dataType == "time:Utc" {
                    SheetTimeType|error timeValue = rowValues.get(key).ensureType();
                    if timeValue is error {
                        return <persist:Error>error(timeValue.message());
                    }
                    string|error value = self.timeToString(timeValue);
                    if value is error {
                        return <persist:Error>error(value.message());
                    }
                    values.push(value);
                } else {
                    SheetBasicType|error value = rowValues.get(key).ensureType();
                    if value is error {
                        return <persist:Error>error(value.message());
                    }
                    values.push(value);
                }
            }
            string a1Range = string `${self.tableName}!${self.range}`;
            http:Response|error response = self.httpAppScriptClient->/.post({
                "function": "myFunction",
                "parameters": [
                    self.tableName, metadataValue, values, self.spreadsheetId, a1Range, self.sheetId
                ]
            });
            if response is error {
                return <persist:Error>error(response.message());
            }
            json|error responseJson = response.getJsonPayload();
            if responseJson is error {
                return <persist:Error>error(responseJson.message());
            }
            AppScriptJsonResponseRecord|error responseJsonRecord = responseJson.cloneWithType();
            if responseJsonRecord is error {
                return <persist:Error>error(responseJsonRecord.message());
            }
            if responseJsonRecord.done == false {
                return <persist:Error>error("Error: Connection to the AppScript API failed. ");
            }
            ErrorRecord? errorRecord = responseJsonRecord.'error;
            if errorRecord !is () {
                if errorRecord.details[0].errorMessage.includes("Duplicate Record", 0) {
                    return <persist:AlreadyExistsError>error(string `Duplicate key: ${self.generateKeyArrayString(self.keyFields, rowValues)}`);
                } else {
                    return <persist:Error>error(string `Error: ${errorRecord.details[0].errorMessage} on ${self.generateKeyArrayString(self.keyFields, rowValues)}`);
                }
            }
        }
    }

    # Performs an SQL `SELECT` operation to read a single entity record from the database.
    #
    # + rowType - The type description of the entity to be retrieved
    # + rowTypeWithIdFields - The type description of the entity to be retrieved with the key fields included
    # + typeMap - The data types of the record
    # + key - The value of the key (to be used as the `WHERE` clauses)
    # + fields - The fields to be retrieved
    # + include - The relations to be retrieved (SQL `JOINs` to be performed)
    # + typeDescriptions - The type descriptions of the relations to be retrieved
    # + return - A record in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function runReadByKeyQuery(typedesc<record {}> rowType, typedesc<record {}> rowTypeWithIdFields, map<anydata> typeMap, anydata key, string[] fields = [], string[] include = [], typedesc<record {}>[] typeDescriptions = []) returns record {}|persist:Error {
        record {} 'object = check self.queryOne(key);
        'object = persist:filterRecord('object, self.addKeyFields(fields));
        check self.getManyRelations('object, fields, include, typeDescriptions);
        self.removeUnwantedFields('object, fields);
        do {
            return check 'object.cloneWithType(rowType);
        } on fail error e {
            return <persist:Error>e;
        }
    }

    # + rowType - The type description of the entity to be retrieved
    # + typeMap - The data types of the record
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A stream of records in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function runReadQuery(typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [], string[] include = [])
    returns stream<record {}, error?>|persist:Error {
        return self.query(self.addKeyFields(fields));
    }

    # + rowType - The type description of the entity to be retrieved
    # + typeMap - The data types of the record
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A stream of records in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function readTableAsStream(typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [], string[] include = []) returns stream<record {}, persist:Error?>|persist:Error {
        string query = "select *";
        string|error encodedQuery = url:encode(query, "UTF-8");
        if encodedQuery is error {
            return <persist:Error>error(encodedQuery.message());
        }
        http:QueryParams queries = {"gid": self.sheetId, "range": self.range, "tq": <string>encodedQuery, "tqx": "out:csv"};
        http:Response|error response = self.httpSheetsClient->/d/[self.spreadsheetId]/gviz/tq(params = queries);
        if response is error {
            return <persist:Error>error(response.message());
        }
        string|error textResponse = response.getTextPayload();
        if textResponse !is error {
            string[] responseRows = re `\n`.split(textResponse);
            record {}[] rowTable = [];
            if responseRows.length() == 0 {
                return <persist:Error>error("Error: the spreadsheet is not initialised correctly.");
            } else if responseRows.length() == 1 {
                return rowTable.toStream();
            }
            string[] columnNames = re `,`.split(responseRows[0]);
            foreach string rowString in responseRows.slice(1) {
                int i = 0;
                record {} rowArray = {};
                string[] rowValues = re `,`.split(rowString);
                foreach string rowValue in rowValues {
                    string columnName = re ` `.replaceAll(re `"`.replaceAll(columnNames[i], ""), "");
                    string value = re `"`.replaceAll(rowValue, "");
                    string dataType = self.dataTypes.get(columnName).toString();
                    if dataType == "time:Date" || dataType == "time:TimeOfDay" || dataType == "time:Civil" || dataType == "time:Utc" {
                        SheetFieldType|error typedValue = self.dataConverter(value, dataType);
                        if typedValue is error {
                            return <persist:Error>error(typedValue.message());
                        } else if typedValue is time:Civil {
                            rowArray[columnName] = <time:Civil>typedValue;
                        } else if typedValue is time:Date {
                            rowArray[columnName] = <time:Date>typedValue;
                        } else if typedValue is time:TimeOfDay {
                            rowArray[columnName] = <time:TimeOfDay>typedValue;
                        } else if typedValue is time:Utc {
                            rowArray[columnName] = <time:Utc>typedValue;
                        }
                    } else {
                        SheetFieldType|error typedValue = self.dataConverter(value, dataType);
                        if typedValue is error {
                            return <persist:Error>error(typedValue.message());
                        }
                        rowArray[columnName] = typedValue;
                    }
                    i = i + 1;
                }
                rowTable.push(rowArray);
            }
            return rowTable.toStream();
        } else {
            return <persist:Error>error(textResponse.message());
        }
    }

    # Performs an SQL `UPDATE` operation to update multiple entity records in the database.
    #
    # + key - the key of the entity
    # + updateRecord - the record to be updated
    # + return - `()` if the operation is performed successfully.
    # A `ForeignKeyViolationError` if the operation violates a foreign key constraint.
    # A `persist:Error` if the operation fails due to another reason.
    public isolated function runUpdateQuery(anydata key, record {} updateRecord) returns persist:Error? {
        string[] entityKeys = self.fieldMetadata.keys();
        SheetBasicType[] values = [];
        string metadataValue;
        if key is map<anydata> {
            metadataValue = self.generateMetadataValue(self.keyFields, key);
        } else {
            metadataValue = key.toString();
        }
        sheets:DeveloperMetadataLookupFilter filter = {locationType: "ROW", metadataKey: self.tableName, metadataValue: metadataValue};
        sheets:ValueRange[]|error rows = self.googleSheetClient->getRowByDataFilter(self.spreadsheetId, self.sheetId, filter);
        if rows is error {
            return <persist:Error>error(rows.message());
        }

        if rows.length() == 0 {
            return persist:getNotFoundError(self.entityName, key);
        } else if rows.length() > 1 {
            return <persist:Error>error(string `Multiple elements found for given key: ${key.toString()}`);
        }
        foreach string entityKey in entityKeys {
            if !updateRecord.hasKey(entityKey) && self.keyFields.indexOf(entityKey) != () {
                if key is map<anydata> {
                    (int|string|decimal)|error value = key.get(entityKey).ensureType();
                    if value is error {
                        return <persist:Error>error(value.message());
                    }
                    values.push(value);
                } else if (key is int|string|decimal|float) {
                    values.push(key);
                }
            } else if !updateRecord.hasKey(entityKey) && self.keyFields.indexOf(entityKey) == () {
                int indexOfKey = <int>self.fieldMetadata.keys().indexOf(entityKey, 0);
                string dataType = self.dataTypes.get(entityKey).toString();
                if dataType == "boolean" || dataType == "int" || dataType == "float" || dataType == "decimal" {
                    SheetNumericType|error value = self.valuesFromString(rows[0].values[indexOfKey].toString(), dataType);
                    if value is error {
                        return <persist:Error>error(value.message());
                    }
                    values.push(value);
                } else {
                    values.push(rows[0].values[indexOfKey]);
                }
            } else {
                SheetBasicType|error value;
                string dataType = self.dataTypes.get(entityKey).toString();

                if dataType == "time:Date" || dataType == "time:TimeOfDay" || dataType == "time:Civil" || dataType == "time:Utc" {
                    SheetTimeType|error timeValue = updateRecord.get(entityKey).ensureType();
                    if timeValue is error {
                        return <persist:Error>error(timeValue.message());
                    }

                    value = self.timeToString(timeValue);
                    if value is error {
                        return <persist:Error>error(value.message());
                    }
                    values.push(value);
                } else {
                    value = updateRecord.get(entityKey).ensureType();
                    if value is error {
                        return <persist:Error>error(value.message());
                    }
                    values.push(value);
                }
            }
        }
        error? response = self.googleSheetClient->updateRowByDataFilter(self.spreadsheetId, self.sheetId, filter, values, "USER_ENTERED");
        if response is error {
            return <persist:Error>error(response.message());

        }
    }

    # Performs an SQL `DELETE` operation to delete an entity record from the database.
    #
    # + deleteKey - The key used to delete an entity record
    # + return - `()` if the operation is performed successfully or a `persist:Error` if the operation fails
    public isolated function runDeleteQuery(anydata deleteKey) returns persist:Error? {
        string metadataValue;
        if deleteKey is map<anydata> {
            metadataValue = self.generateMetadataValue(self.keyFields, deleteKey);
        } else {
            metadataValue = deleteKey.toString();
        }
        sheets:DeveloperMetadataLookupFilter filter = {locationType: "ROW", metadataKey: self.tableName, metadataValue: metadataValue};
        sheets:ValueRange[]|error rows = self.googleSheetClient->getRowByDataFilter(self.spreadsheetId, self.sheetId, filter);
        if rows is error {
            if (deleteKey is map<anydata>) {
                return <persist:Error>error(string `Not found: ${self.generateKeyArrayString(self.keyFields, deleteKey)}`);
            } else {
                return <persist:Error>error(string `Not found: ${deleteKey.toString()}`);
            }
        }
        if rows.length() == 0 {
            return <persist:Error>error("no element found for delete");
        }
        error? response = self.googleSheetClient->deleteRowByDataFilter(self.spreadsheetId, self.sheetId, filter);
        if response is error {
            return <persist:Error>error(response.message());
        }
    }

    public isolated function getKeyFields() returns string[] {
        return self.keyFields;
    }

    public isolated function getManyRelations(record {} 'object, string[] fields, string[] include, typedesc<record {}>[] typeDescriptions) returns persist:Error? {
        foreach int i in 0 ..< include.length() {
            string entity = include[i];
            string[] relationFields = from string 'field in fields
                where 'field.startsWith(entity + "[].")
                select 'field.substring(entity.length() + 3, 'field.length());
            if relationFields.length() is 0 {
                continue;
            }
            isolated function (record {}, string[]) returns record {}[]|error associationsMethod = self.associationsMethods.get(entity);
            record {}[]|error relations = associationsMethod('object, relationFields);
            if relations is error {
                return <persist:Error>error("unsupported data format");
            }
            'object[entity] = relations;
        }
    }

    public isolated function addKeyFields(string[] fields) returns string[] {
        string[] updatedFields = fields.clone();
        foreach string key in self.keyFields {
            if updatedFields.indexOf(key) is () {
                updatedFields.push(key);
            }
        }
        return updatedFields;
    }

    private isolated function dataConverter(json value, string dataType) returns SheetFieldType|error {
        if dataType == "int" {
            return int:fromString(value.toString());
        } else if dataType == "time:Date" || dataType == "time:TimeOfDay" || dataType == "time:Civil" || dataType == "time:Utc" {
            return self.stringToTime(value.toString(), dataType);
        } else if dataType == "string" || dataType == "ENUM" {
            return value.toString();
        } else if dataType == "decimal" {
            return decimal:fromString(value.toString());
        } else if dataType == "float" {
            return float:fromString(value.toString());
        } else if dataType == "boolean" {
            return value.toString() == "TRUE";
        } else {
            return <error>error("unsupported data format");
        }
    }

    private isolated function generateMetadataValue(string[] keyFields, map<anydata> rowValues) returns string {
        string metadataValue = "";
        foreach string key in keyFields {
            if metadataValue != "" {
                metadataValue += ":";
            }
            metadataValue += rowValues[key].toString();
        }
        return metadataValue;
    }

    private isolated function generateKeyArrayString(string[] keyFields, map<anydata> rowValues) returns string {
        string metadataValue = "";
        foreach string key in keyFields {
            if metadataValue != "" {
                metadataValue += ",";
            }
            metadataValue += string `"${rowValues[key].toString()}"`;
        }
        return string `[${metadataValue}]`;
    }

    private isolated function generateKeyRecord(string[] keyFields, map<anydata> rowValues) returns record {} {
        record {} keyRecord = {};
        foreach string key in keyFields {
            keyRecord[key] = rowValues[key];
        }
        return keyRecord;
    }

    private isolated function removeUnwantedFields(record {} 'object, string[] fields) {
        foreach string keyField in self.keyFields {
            if fields.indexOf(keyField) is () {
                _ = 'object.remove(keyField);
            }
        }
    }

    private isolated function timeToString(SheetTimeType timeValue) returns string|error {

        if timeValue is time:Civil {
            return self.civilToString(timeValue);
        }

        if timeValue is time:Utc {
            return time:utcToString(timeValue);
        }

        if timeValue is time:Date {
            return string `${timeValue.day}-${timeValue.month}-${timeValue.year}`;
        }

        if timeValue is time:TimeOfDay {
            return string `${timeValue.hour}-${timeValue.minute}-${(timeValue.second).toString()}`;
        }

        return <persist:Error>error("Error: unsupported time format");

    }

    private isolated function civilToString(time:Civil civil) returns string|error {
        string civilString = string `${civil.year}-${(civil.month.abs() > 9? civil.month: string `0${civil.month}`)}-${(civil.day.abs() > 9? civil.day: string `0${civil.day}`)}`;
        civilString += string `T${(civil.hour.abs() > 9? civil.hour: string `0${civil.hour}`)}:${(civil.minute.abs() > 9? civil.minute: string `0${civil.minute}`)}`;
        if civil.second !is () {
            time:Seconds seconds = <time:Seconds>civil.second;
            civilString += string `:${(seconds.abs() > (check decimal:fromString("9"))? seconds: string `0${seconds}`)}`;
        }
        if civil.utcOffset !is () {
            time:ZoneOffset zoneOffset = <time:ZoneOffset>civil.utcOffset;
            civilString += (zoneOffset.hours >= 0? "+" : "-");
            civilString += string `${zoneOffset.hours.abs() > 9? zoneOffset.hours.abs() : string `0${zoneOffset.hours.abs()}`}`;
            civilString += string `:${(zoneOffset.minutes.abs() > 9? zoneOffset.minutes.abs(): string `0${zoneOffset.minutes.abs()}`)}`;
            time:Seconds? seconds = zoneOffset.seconds;
            if seconds !is () {
                civilString += string `:${(seconds.abs() > 9d? seconds: string `0${seconds.abs()}`)}`;
            } else {
                civilString += string `:00`;
            }

        } if civil.timeAbbrev !is () {
            civilString += string `(${<string>civil.timeAbbrev})`;
        }
        return civilString;
    }

    private isolated function stringToCivil(string civilString) returns time:Civil|error {
        time:ZoneOffset? zoneOffset = ();
        string civilTimeString = "";
        string civilDateString = "";
        string? timeAbbrev = ();
        regexp:Span? find = re `\(.*\)`.find(civilString.trim(), 0);
        if find !is () {
            timeAbbrev = civilString.trim().substring(find.startIndex+1, find.endIndex-1);
        }
        string[] civilArray = re `T`.split(re `\(.*\)`.replace(civilString.trim(), ""));
        civilDateString = civilArray[0];
        find = re `\+|-`.find(civilArray[1], 0);
        if find !is () {
            int sign = +1;
            if civilArray[1].includes("-") {
                sign = -1;
            }
            string[] civilTimeOffsetArray = re `\+|-`.split(civilArray[1]);
            civilTimeString = civilTimeOffsetArray[0];
            string[] zoneOffsetStringArray = re `:`.split(civilTimeOffsetArray[1]);
            zoneOffset = {hours: sign * (check int:fromString(zoneOffsetStringArray[0])), minutes: sign * (check int:fromString(zoneOffsetStringArray[1])), seconds: sign * (check decimal:fromString(zoneOffsetStringArray[2]))};
        } else {
            civilTimeString = civilArray[1];
        }
        string[] civilTimeStringArray = re `:`.split(civilTimeString);
        string[] civilDateStringArray = re `-`.split(civilDateString);
        int year = check int:fromString(civilDateStringArray[0]);
        int month = check int:fromString(civilDateStringArray[1]);
        int day = check int:fromString(civilDateStringArray[2]);
        int hour = check int:fromString(civilTimeStringArray[0]);
        int minute = check int:fromString(civilTimeStringArray[1]);
        decimal second = check decimal:fromString(civilTimeStringArray[2]);
        return <time:Civil>{year: year, month: month, day: day, hour: hour, minute: minute, second: second, timeAbbrev: timeAbbrev, utcOffset: zoneOffset};
    }

    private isolated function valuesFromString(string value, string dataType) returns SheetNumericType|error {

        match dataType {
            "int" => {
                return int:fromString(value);
            }
            "boolean" => {
                if value == "TRUE" {
                    return true;
                } else {
                    return false;
                }
            }
            "decimal" => {
                return decimal:fromString(value);
            }
            _ => {
                return float:fromString(value.toString());
            }
        }
    }

    private isolated function stringToTime(string timeValue, string dataType) returns SheetTimeType|error {
        if dataType == "time:TimeOfDay" {
            string[] timeValues = re `-`.split(timeValue);
            time:TimeOfDay output = {hour: check int:fromString(timeValues[0]), minute: check int:fromString(timeValues[1]), second: check decimal:fromString(timeValues[2])};
            return output;
        } else if dataType == "time:Date" {
            string[] timeValues = re `-`.split(timeValue);
            time:Date output = {day: check int:fromString(timeValues[0]), month: check int:fromString(timeValues[1]), year: check int:fromString(timeValues[2])};
            return output;
        } else if dataType == "time:Civil" {
            return self.stringToCivil(timeValue);
        } else if dataType == "time:Utc" {
            return time:utcFromString(timeValue);
        } else {
            return <error>error("Error: unsupported time format");
        }
    }

}
