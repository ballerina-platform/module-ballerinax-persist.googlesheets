// Copyright (c) 2022 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
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

public type SheetMetadata record {|
    string entityName;
    string tableName;
    map<SheetFieldMetadata> fieldMetadata;
    string[] keyFields;
    string range;
    map<string> dataTypes;
    isolated function (string[]) returns stream<record {}, persist:Error?>|persist:Error query;
    isolated function (anydata) returns record {}|persist:NotFoundError queryOne;
    map<isolated function (record {}, string[]) returns record {}[]|persist:Error> associationsMethods;
|};

# Represents the metadata associated with a simple field in the entity record.
#
# + columnName - The name of the spreadsheet column to which the field is mapped
# + columnId - The alphabetical Id of the column
public type SimpleSheetFieldMetadata record {|
    string columnName;
    string columnId;
|};

# Represents the metadata associated with a field of an entity.
# Only used by the generated persist clients and `persist:GoogleSheetsClient`.
#
public type SheetFieldMetadata SimpleSheetFieldMetadata;

