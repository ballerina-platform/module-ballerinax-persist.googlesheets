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

import ballerina/test;
import ballerina/persist;
import ballerina/lang.runtime;

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsDepartmentDeleteTestNegative],
    enable: true
}
function gsheetsWorkspaceCreateTest() returns error? {
    runtime:sleep(40);
    string[] workspaceIds = check rainierClient->/workspaces.post([workspace1]);
    test:assertEquals(workspaceIds, [workspace1.workspaceId]);

    Workspace workspaceRetrieved = check rainierClient->/workspaces/[workspace1.workspaceId].get();
    test:assertEquals(workspaceRetrieved, workspace1);
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    enable: true
}
function gsheetsWorkspaceCreateTest2() returns error? {
    string[] workspaceIds = check rainierClient->/workspaces.post([workspace2, workspace3]);

    test:assertEquals(workspaceIds, [workspace2.workspaceId, workspace3.workspaceId]);

    Workspace workspaceRetrieved = check rainierClient->/workspaces/[workspace2.workspaceId].get();
    test:assertEquals(workspaceRetrieved, workspace2);

    workspaceRetrieved = check rainierClient->/workspaces/[workspace3.workspaceId].get();
    test:assertEquals(workspaceRetrieved, workspace3);
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsWorkspaceCreateTest],
    enable: true
}
function gsheetsWorkspaceReadOneTest() returns error? {
    Workspace workspaceRetrieved = check rainierClient->/workspaces/[workspace1.workspaceId].get();
    test:assertEquals(workspaceRetrieved, workspace1);
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsWorkspaceCreateTest],
    enable: true
}
function gsheetsWorkspaceReadOneDependentTest() returns error? {
    WorkspaceInfo2 workspaceRetrieved = check rainierClient->/workspaces/[workspace1.workspaceId].get();
    test:assertEquals(workspaceRetrieved,
        {
        workspaceType: workspace1.workspaceType,
        locationBuildingCode: workspace1.locationBuildingCode
    }
    );
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsWorkspaceCreateTest],
    enable: true
}
function gsheetsWorkspaceReadOneTestNegative() returns error? {
    Workspace|error workspaceRetrieved = rainierClient->/workspaces/["invalid-workspace-id"].get();
    if workspaceRetrieved is persist:NotFoundError {
        test:assertEquals(workspaceRetrieved.message(), "A record with the key 'invalid-workspace-id' does not exist for the entity 'Workspace'.");
    } else {
        test:assertFail("NotFoundError expected.");
    }
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsWorkspaceCreateTest, gsheetsWorkspaceCreateTest2],
    enable: true
}
function gsheetsWorkspaceReadManyTest() returns error? {
    stream<Workspace, error?> workspaceStream = rainierClient->/workspaces.get();
    Workspace[] workspaces = check from Workspace workspace in workspaceStream
        select workspace;

    test:assertEquals(workspaces, [workspace1, workspace2, workspace3]);
}

@test:Config {
    groups: ["workspace", "dependent"],
    dependsOn: [gsheetsWorkspaceCreateTest, gsheetsWorkspaceCreateTest2],
    enable: true
}
function gsheetsWorkspaceReadManyDependentTest() returns error? {
    stream<WorkspaceInfo2, error?> workspaceStream = rainierClient->/workspaces.get();
    WorkspaceInfo2[] workspaces = check from WorkspaceInfo2 workspace in workspaceStream
        select workspace;

    test:assertEquals(workspaces, [
        {workspaceType: workspace1.workspaceType, locationBuildingCode: workspace1.locationBuildingCode},
        {workspaceType: workspace2.workspaceType, locationBuildingCode: workspace2.locationBuildingCode},
        {workspaceType: workspace3.workspaceType, locationBuildingCode: workspace3.locationBuildingCode}
    ]);
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsWorkspaceReadOneTest, gsheetsWorkspaceReadManyTest, gsheetsWorkspaceReadManyDependentTest],
    enable: true
}
function gsheetsWorkspaceUpdateTest() returns error? {
    Workspace workspace = check rainierClient->/workspaces/[workspace1.workspaceId].put({
        workspaceType: "large"
    });

    test:assertEquals(workspace, updatedWorkspace1);

    Workspace workspaceRetrieved = check rainierClient->/workspaces/[workspace1.workspaceId].get();
    test:assertEquals(workspaceRetrieved, updatedWorkspace1);
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsWorkspaceReadOneTest, gsheetsWorkspaceReadManyTest, gsheetsWorkspaceReadManyDependentTest],
    enable: true
}
function gsheetsWorkspaceUpdateTestNegative1() returns error? {
    Workspace|error workspace = rainierClient->/workspaces/["invalid-workspace-id"].put({
        workspaceType: "large"
    });

    if workspace is persist:NotFoundError {
        test:assertEquals(workspace.message(), "A record with the key 'invalid-workspace-id' does not exist for the entity 'Workspace'.");
    } else {
        test:assertFail("NotFoundError expected.");
    }
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsWorkspaceUpdateTest, gsheetsWorkspaceUpdateTestNegative1],
    enable: true
}
function gsheetsWorkspaceDeleteTest() returns error? {
    Workspace workspace = check rainierClient->/workspaces/[workspace1.workspaceId].delete();
    test:assertEquals(workspace, updatedWorkspace1);

    stream<Workspace, error?> workspaceStream = rainierClient->/workspaces.get();
    Workspace[] workspaces = check from Workspace workspace2 in workspaceStream
        select workspace2;

    test:assertEquals(workspaces, [workspace2, workspace3]);
}

@test:Config {
    groups: ["workspace", "google-sheets"],
    dependsOn: [gsheetsWorkspaceDeleteTest],
    enable: true
}
function gsheetsWorkspaceDeleteTestNegative() returns error? {
    Workspace|error workspace = rainierClient->/workspaces/[workspace1.workspaceId].delete();

    if workspace is persist:NotFoundError {
        test:assertEquals(workspace.message(), "A record with the key 'workspace-1' does not exist for the entity 'Workspace'.");
    } else {
        test:assertFail("NotFoundError expected.");
    }
}
