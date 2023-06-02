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
import ballerina/test;
import ballerinax/googleapis.sheets;

configurable string & readonly refreshToken = os:getEnv("REFRESH_TOKEN");
configurable string & readonly clientId = os:getEnv("CLIENT_ID");
configurable string & readonly clientSecret = os:getEnv("CLIENT_SECRET");
configurable string & readonly spreadsheetId = os:getEnv("SPREADSHEET_ID");

@test:BeforeSuite
function initSpreadsheet() returns error? {
    sheets:ConnectionConfig spreadsheetConfig = {
        auth: {
            clientId: clientId,
            clientSecret: clientSecret,
            refreshToken: refreshToken,
            refreshUrl:sheets:REFRESH_URL
        }
    };
    sheets:Client spreadsheetClient = check new (spreadsheetConfig);
    string[] sheetNames = ["OrderItem", "Employee", "Workspace", "Building", "Department", "OrderItemExtended"];
    sheets:Spreadsheet spreadSheet = check spreadsheetClient->openSpreadsheetById(spreadsheetId);
    foreach sheets:Sheet sheet in spreadSheet.sheets {
        if sheetNames.indexOf(sheet.properties.title, 0) !is () {
            check spreadsheetClient->removeSheet(spreadsheetId, sheet.properties.sheetId);
            _ = check spreadsheetClient->addSheet(spreadSheet.spreadsheetId, sheet.properties.title);
        }
    }

    _ = check spreadsheetClient->appendValue(spreadSheet.spreadsheetId, ["orderId", "itemId", "quantity" ,"notes"], {sheetName: "OrderItem", startIndex: "A1", endIndex: "E1"}, "USER_ENTERED");
    _ = check spreadsheetClient->appendValue(spreadSheet.spreadsheetId, ["empNo", "firstName", "lastName", "birthDate", "gender", "hireDate", "departmentDeptNo", "workspaceWorkspaceId"], {sheetName: "Employee", startIndex: "A1", endIndex: "I1"}, "USER_ENTERED");
    _ = check spreadsheetClient->appendValue(spreadSheet.spreadsheetId, ["workspaceId", "workspaceType", "locationBuildingCode"], {sheetName: "Workspace", startIndex: "A1", endIndex: "D1"}, "USER_ENTERED");
    _ = check spreadsheetClient->appendValue(spreadSheet.spreadsheetId, ["buildingCode", "city", "state", "country", "postalCode", "type"], {sheetName: "Building", startIndex: "A1", endIndex: "G1"}, "USER_ENTERED");
    _ = check spreadsheetClient->appendValue(spreadSheet.spreadsheetId, ["deptNo", "deptName"], {sheetName: "Department", startIndex: "A1", endIndex: "C1"}, "USER_ENTERED");
    _ = check spreadsheetClient->appendValue(spreadSheet.spreadsheetId, ["orderId", "itemId", "CustomerId", "paid", "ammountPaid", "ammountPaidDecimal", "arivalTimeCivil", "arivalTimeUtc", "arivalTimeDate", "arivalTimeTimeOfDay", "orderType"], {sheetName: "OrderItemExtended", startIndex: "A1", endIndex: "L1"}, "USER_ENTERED");
    rainierClient = check new ();
}

GoogleSheetsRainierClient rainierClient =  check new ();

OrderItemExtended orderItemExtended1 = {
    orderId: "order-1",
    itemId: "item-1",
    CustomerId: 1,
    paid: false,
    ammountPaid: 10.5f,
    ammountPaidDecimal: 10.5,
    arivalTimeCivil: {"utcOffset":{"hours":5,"minutes":30},"timeAbbrev":"Asia/Colombo","dayOfWeek":1,"year":2021,"month":4,"day":12,"hour":23,"minute":20,"second":50.52},
    arivalTimeUtc: [1684493685, 0.998012000],
    arivalTimeDate: {year: 2021, month: 4, day: 12},
    arivalTimeTimeOfDay: {hour: 17, minute: 50, second: 50.52},
    orderType: INSTORE
};

OrderItemExtended orderItemExtendedRetrieved = {
    orderId: "order-1",
    itemId: "item-1",
    CustomerId: 1,
    paid: false,
    ammountPaid: 10.5f,
    ammountPaidDecimal: 10.5,
    arivalTimeCivil: {"timeAbbrev":"Z","dayOfWeek":1 ,"year":2021,"month":4,"day":12,"hour":17,"minute":50,"second":50.52},
    arivalTimeUtc: [1684493685, 0.998012000],
    arivalTimeDate: {year: 2021, month: 4, day: 12},
    arivalTimeTimeOfDay: {hour: 17, minute: 50, second: 50.52},
    orderType: INSTORE
};

OrderItemExtended orderItemExtended2 = {
    orderId: "order-2",
    itemId: "item-2",
    CustomerId: 1,
    paid: false,
    ammountPaid: 10.5f,
    ammountPaidDecimal: 10.5,
    arivalTimeCivil: {"utcOffset":{"hours":5,"minutes":30},"timeAbbrev":"Asia/Colombo","dayOfWeek":1 ,"year":2024,"month":4,"day":12,"hour":17,"minute":50,"second":50.52},
    arivalTimeUtc: [1684493685, 0.998012000],
    arivalTimeDate: {year: 2021, month: 4, day: 12},
    arivalTimeTimeOfDay: {hour: 17, minute: 50, second: 50.52},
    orderType: ONLINE
};

public type EmployeeInfo record {|
    string firstName;
    string lastName;
    record {|
        string deptName;
    |} department;
    Workspace workspace;
|};

OrderItemExtended orderItemExtended2Retrieved = {
    orderId: "order-2",
    itemId: "item-2",
    CustomerId: 1,
    paid: false,
    ammountPaid: 10.5f,
    ammountPaidDecimal: 10.5,
    arivalTimeCivil: {"timeAbbrev":"Z","dayOfWeek":5 ,"year":2024,"month":4,"day":12,"hour":12,"minute":20,"second":50.52},
    arivalTimeUtc: [1684493685, 0.998012000],
    arivalTimeDate: {year: 2021, month: 4, day: 12},
    arivalTimeTimeOfDay: {hour: 17, minute: 50, second: 50.52},
    orderType: ONLINE
};

OrderItemExtended orderItemExtended3 = {
    orderId: "order-3",
    itemId: "item-3",
    CustomerId: 4,
    paid: true,
    ammountPaid: 20.5f,
    ammountPaidDecimal: 20.5,
    arivalTimeCivil: {"utcOffset":{"hours":5,"minutes":30},"timeAbbrev":"Asia/Colombo","dayOfWeek":1,"year":2021,"month":4,"day":12,"hour":23,"minute":20,"second":50.52},
    arivalTimeUtc: [1684493685, 0.998012000],
    arivalTimeDate: {year: 2021, month: 4, day: 12},
    arivalTimeTimeOfDay: {hour: 17, minute: 50, second: 50.52},
    orderType: INSTORE
};

OrderItemExtended orderItemExtended3Retrieved = {
    orderId: "order-2",
    itemId: "item-2",
    CustomerId: 1,
    paid: true,
    ammountPaid: 10.5f,
    ammountPaidDecimal: 10.5,
    arivalTimeCivil: {"timeAbbrev":"Z","dayOfWeek":1 ,"year":2021,"month":4,"day":12,"hour":17,"minute":50,"second":50.52},
    arivalTimeUtc: [1684493685, 0.998012000],
    arivalTimeDate: {year: 2021, month: 4, day: 12},
    arivalTimeTimeOfDay: {hour: 17, minute: 50, second: 50.52},
    orderType: ONLINE
};

public type DepartmentInfo record {|
    string deptNo;
    string deptName;
    record {|
        string firstName;
        string lastName;
    |}[] employees;
|};

public type WorkspaceInfo record {|
    string workspaceType;
    Building location;
    Employee[] employees;
|};

public type BuildingInfo record {|
    string buildingCode;
    string city;
    string state;
    string country;
    string postalCode;
    string 'type;
    Workspace[] workspaces;
|};

Building building1 = {
    buildingCode: "building-1",
    city: "Colombo",
    state: "Western Province",
    country: "Sri Lanka",
    postalCode: "10370",
    'type: "rented"
};

Building invalidBuilding = {
    buildingCode: "building-invalid-extra-characters-to-force-failure",
    city: "Colombo",
    state: "Western Province",
    country: "Sri Lanka",
    postalCode: "10370",
    'type: "owned"
};

BuildingInsert building2 = {
    buildingCode: "building-2",
    city: "Manhattan",
    state: "New York",
    country: "USA",
    postalCode: "10570",
    'type: "owned"
};

BuildingInsert building3 = {
    buildingCode: "building-3",
    city: "London",
    state: "London",
    country: "United Kingdom",
    postalCode: "39202",
    'type: "rented"
};

Building updatedBuilding1 = {
    buildingCode: "building-1",
    city: "Galle",
    state: "Southern Province",
    country: "Sri Lanka",
    postalCode: "10890",
    'type: "owned"
};

Department department1 = {
    deptNo: "department-1",
    deptName: "Finance"
};

Department invalidDepartment = {
    deptNo: "invalid-department-extra-characters-to-force-failure",
    deptName: "Finance"
};

Department department2 = {
    deptNo: "department-2",
    deptName: "Marketing"
};

Department department3 = {
    deptNo: "department-3",
    deptName: "Engineering"
};

Department updatedDepartment1 = {
    deptNo: "department-1",
    deptName: "Finance & Legalities"
};

Employee employee1 = {
    empNo: "employee-1",
    firstName: "Tom",
    lastName: "Scott",
    birthDate: {year: 1992, month: 11, day: 13},
    gender: MALE,
    hireDate: {year: 2022, month: 8, day: 1},
    departmentDeptNo: "department-2",
    workspaceWorkspaceId: "workspace-2"
};

Employee invalidEmployee = {
    empNo: "invalid-employee-no-extra-characters-to-force-failure",
    firstName: "Tom",
    lastName: "Scott",
    birthDate: {year: 1992, month: 11, day: 13},
    gender: MALE,
    hireDate: {year: 2022, month: 8, day: 1},
    departmentDeptNo: "department-2",
    workspaceWorkspaceId: "workspace-2"
};

Employee employee2 = {
    empNo: "employee-2",
    firstName: "Jane",
    lastName: "Doe",
    birthDate: {year: 1996, month: 9, day: 15},
    gender: FEMALE,
    hireDate: {year: 2022, month: 6, day: 1},
    departmentDeptNo: "department-2",
    workspaceWorkspaceId: "workspace-2"
};

Employee employee3 = {
    empNo: "employee-3",
    firstName: "Hugh",
    lastName: "Smith",
    birthDate: {year: 1986, month: 9, day: 15},
    gender: FEMALE,
    hireDate: {year: 2021, month: 6, day: 1},
    departmentDeptNo: "department-3",
    workspaceWorkspaceId: "workspace-3"
};

Employee updatedEmployee1 = {
    empNo: "employee-1",
    firstName: "Tom",
    lastName: "Jones",
    birthDate: {year: 1994, month: 11, day: 13},
    gender: MALE,
    hireDate: {year: 2022, month: 8, day: 1},
    departmentDeptNo: "department-3",
    workspaceWorkspaceId: "workspace-2"
};

public type IntIdRecordDependent record {|
    string randomField;
|};

public type StringIdRecordDependent record {|
    string randomField;
|};

public type FloatIdRecordDependent record {|
    string randomField;
|};

public type DecimalIdRecordDependent record {|
    string randomField;
|};

public type BooleanIdRecordDependent record {|
    string randomField;
|};

public type AllTypesIdRecordDependent record {|
    string randomField;
|};

public type CompositeAssociationRecordDependent record {|
    string randomField;
    int alltypesidrecordIntType;
    decimal alltypesidrecordDecimalType;
    record {|
        int intType;
        string stringType;
        boolean booleanType;
        string randomField;
    |} allTypesIdRecord;
|};

Workspace workspace1 = {
    workspaceId: "workspace-1",
    workspaceType: "small",
    locationBuildingCode: "building-2"
};

Workspace invalidWorkspace = {
    workspaceId: "invalid-workspace-extra-characters-to-force-failure",
    workspaceType: "small",
    locationBuildingCode: "building-2"
};

Workspace workspace2 = {
    workspaceId: "workspace-2",
    workspaceType: "medium",
    locationBuildingCode: "building-2"
};

Workspace workspace3 = {
    workspaceId: "workspace-3",
    workspaceType: "small",
    locationBuildingCode: "building-2"
};

Workspace updatedWorkspace1 = {
    workspaceId: "workspace-1",
    workspaceType: "large",
    locationBuildingCode: "building-2"
};

public type EmployeeName record {|
    string firstName;
    string lastName;
|};

public type EmployeeInfo2 record {|
    readonly string empNo;
    time:Date birthDate;
    string departmentDeptNo;
    string workspaceWorkspaceId;
|};

public type WorkspaceInfo2 record {|
    string workspaceType;
    string locationBuildingCode;
|};

public type DepartmentInfo2 record {|
    string deptName;
|};

public type BuildingInfo2 record {|
    string city;
    string state;
    string country;
    string postalCode;
    string 'type;
|};
