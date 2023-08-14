// AUTO-GENERATED FILE. DO NOT MODIFY.

// This file is an auto-generated file by Ballerina persistence layer for model.
// It should not be modified by hand.

import ballerina/persist;
import ballerina/jballerina.java;
import ballerina/http;
import ballerinax/googleapis.sheets;

const ORDER_ITEM_FALSE = "orderitemfalses";

public isolated client class GooglesheetsNegativeClient {
    *persist:AbstractPersistClient;

    private final sheets:Client googleSheetClient;

    private final http:Client httpClient;

    private final map<GoogleSheetsClient> persistClients;

    public isolated function init() returns persist:Error? {
        final record {|SheetMetadata...;|} & readonly metadata = {
            [ORDER_ITEM_FALSE] : {
                entityName: "OrderItemFalse",
                tableName: "OrderItem",
                keyFields: ["orderId", "itemId"],
                range: "A:E",
                query: self.queryOrderitemfalses,
                queryOne: self.queryOneOrderitemfalses,
                dataTypes: {
                    orderId: "string",
                    itemId: "string",
                    quantity: "int",
                    notes: "string",
                    notesdummy: "string"
                },
                fieldMetadata: {
                    orderId: {columnName: "orderId", columnId: "A"},
                    itemId: {columnName: "itemId", columnId: "B"},
                    quantity: {columnName: "quantity", columnId: "C"},
                    notes: {columnName: "notes", columnId: "D"},
                    notesdummy: {columnName: "notesdummy", columnId: "E"}
                },
                associationsMethods: {}
            }
        };
        sheets:ConnectionConfig sheetsClientConfig = {
            auth: {
                clientId: clientId,
                clientSecret: clientSecret,
                refreshUrl: sheets:REFRESH_URL,
                refreshToken: refreshToken
            }
        };
        http:ClientConfiguration httpClientConfiguration = {
            auth: {
                clientId: clientId,
                clientSecret: clientSecret,
                refreshUrl: sheets:REFRESH_URL,
                refreshToken: refreshToken
            }
        };
        http:Client|error httpSheetsClient = new (string `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values`, httpClientConfiguration);
        if httpSheetsClient is error {
            return <persist:Error>error(httpSheetsClient.message());
        }
        sheets:Client|error googleSheetClient = new (sheetsClientConfig);
        if googleSheetClient is error {
            return <persist:Error>error(googleSheetClient.message());
        }
        self.googleSheetClient = googleSheetClient;
        self.httpSheetsClient = httpSheetsClient;
        map<int> sheetIds = check getSheetIds(self.googleSheetClient, metadata, spreadsheetId);
        self.persistClients = {[ORDER_ITEM_FALSE] : check new (self.googleSheetClient, self.httpClient, metadata.get(ORDER_ITEM_FALSE).cloneReadOnly(), spreadsheetId.cloneReadOnly(), sheetIds.get(ORDER_ITEM_FALSE).cloneReadOnly())};
    }

    isolated resource function get orderitemfalses(OrderItemFalseTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.googlesheets.datastore.GoogleSheetsProcessor",
        name: "query"
    } external;

    isolated resource function get orderitemfalses/[string orderId]/[string itemId](OrderItemFalseTargetType targetType = <>) returns targetType|persist:Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.googlesheets.datastore.GoogleSheetsProcessor",
        name: "queryOne"
    } external;

    isolated resource function post orderitemfalses(OrderItemFalseInsert[] data) returns [string, string][]|persist:Error {
        GoogleSheetsClient googleSheetsClient;
        lock {
            googleSheetsClient = self.persistClients.get(ORDER_ITEM_FALSE);
        }
        _ = check googleSheetsClient.runBatchInsertQuery(data);
        return from OrderItemFalseInsert inserted in data
            select [inserted.orderId, inserted.itemId];
    }

    isolated resource function put orderitemfalses/[string orderId]/[string itemId](OrderItemFalseUpdate value) returns OrderItemFalse|persist:Error {
        GoogleSheetsClient googleSheetsClient;
        lock {
            googleSheetsClient = self.persistClients.get(ORDER_ITEM_FALSE);
        }
        _ = check googleSheetsClient.runUpdateQuery({"orderId": orderId, "itemId": itemId}, value);
        return self->/orderitemfalses/[orderId]/[itemId].get();
    }

    isolated resource function delete orderitemfalses/[string orderId]/[string itemId]() returns OrderItemFalse|persist:Error {
        OrderItemFalse result = check self->/orderitemfalses/[orderId]/[itemId].get();
        GoogleSheetsClient googleSheetsClient;
        lock {
            googleSheetsClient = self.persistClients.get(ORDER_ITEM_FALSE);
        }
        _ = check googleSheetsClient.runDeleteQuery({"orderId": orderId, "itemId": itemId});
        return result;
    }

    private isolated function queryOrderitemfalses(string[] fields) returns stream<record {}, persist:Error?>|persist:Error {
        stream<OrderItemFalse, persist:Error?> orderitemfalsesStream = self.queryOrderitemfalsesStream();
        record {}[] outputArray = check from record {} 'object in orderitemfalsesStream
            select persist:filterRecord({
                ...'object
            }, fields);
        return outputArray.toStream();
    }

    private isolated function queryOneOrderitemfalses(anydata key) returns record {}|persist:NotFoundError {
        stream<OrderItemFalse, persist:Error?> orderitemfalsesStream = self.queryOrderitemfalsesStream();
        error? unionResult = from record {} 'object in orderitemfalsesStream
            where persist:getKey('object, ["orderId", "itemId"]) == key
            do {
                return {
                    ...'object
                };
            };
        if unionResult is error {
            return <persist:NotFoundError>error(unionResult.message());
        }
        return <persist:NotFoundError>error("Invalid key: " + key.toString());
    }

    private isolated function queryOrderitemfalsesStream(OrderItemFalseTargetType targetType = <>) returns stream<targetType, persist:Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.googlesheets.datastore.GoogleSheetsProcessor",
        name: "queryStream"
    } external;

    public isolated function close() returns persist:Error? {
        return ();
    }
}

