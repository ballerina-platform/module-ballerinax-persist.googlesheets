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
import ballerina/time;

# Generic type that can used to store any of the types supported by google sheets
#
public type SheetFieldType string|int|decimal|boolean|float|time:Date|time:TimeOfDay|time:Civil|time:Utc;

# Generic type that can used to store any of time types supported by sheets
#
public type SheetTimeType time:Date|time:TimeOfDay|time:Civil|time:Utc;

# Generic type that can used to store any of basic numeric types supported by sheets
#
public type SheetNumericType int|decimal|boolean|float;

# Generic type that can used to store any of basic types supported by sheets
#
public type SheetBasicType int|string|decimal|boolean|float;

# Record type to represent error response coming from appscript
#
# + done - Status of the response
# + response - Record to store the response if the request is successful
# + error - Record to store the error if the request is failed
public type AppScriptJsonResponseRecord record {
    boolean done;
    json response?;
    ErrorRecord 'error?;
};

#Record types to store errors in Appscript response
public type ErrorRecord record {
    string message;
    int code;
    ErrorDetails[] details;
};

#Record types to store details
public type ErrorDetails record {
    string errorType;
    string errorMessage;
    string \@type;
};
