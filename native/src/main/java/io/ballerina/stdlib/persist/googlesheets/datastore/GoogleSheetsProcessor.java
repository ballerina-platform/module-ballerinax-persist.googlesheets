/*
 *  Copyright (c) 2023, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.ballerina.stdlib.persist.googlesheets.datastore;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.persist.googlesheets.Utils;

import java.util.Map;

import static io.ballerina.stdlib.persist.Constants.KEY_FIELDS;
import static io.ballerina.stdlib.persist.Constants.RUN_READ_QUERY_METHOD;
import static io.ballerina.stdlib.persist.ErrorGenerator.wrapError;
import static io.ballerina.stdlib.persist.Utils.getEntity;
import static io.ballerina.stdlib.persist.Utils.getKey;
import static io.ballerina.stdlib.persist.Utils.getMetadata;
import static io.ballerina.stdlib.persist.Utils.getPersistClient;
import static io.ballerina.stdlib.persist.Utils.getRecordTypeWithKeyFields;
import static io.ballerina.stdlib.persist.Utils.getTransactionContextProperties;
import static io.ballerina.stdlib.persist.googlesheets.Constants.RUN_READ_TABLE_AS_STREAM_METHOD;
import static io.ballerina.stdlib.persist.googlesheets.Utils.getEntityFromStreamMethod;
import static io.ballerina.stdlib.persist.googlesheets.Utils.getFieldTypes;

/**
 * This class provides the GoogleSheets query processing implementations for
 * persistence.
 *
 * @since 0.3.0
 */

public class GoogleSheetsProcessor {

    private GoogleSheetsProcessor() {
    };

    public static BStream query(Environment env, BObject client, BTypedesc targetType) {
        // This method will return `stream<targetType, persist:Error?>`

        BString entity = getEntity(env);
        BObject persistClient = getPersistClient(client, entity);
        BArray keyFields = (BArray) persistClient.get(KEY_FIELDS);
        RecordType recordType = (RecordType) targetType.getDescribingType();

        RecordType recordTypeWithIdFields = getRecordTypeWithKeyFields(keyFields, recordType);
        BTypedesc targetTypeWithIdFields = ValueCreator.createTypedescValue(recordTypeWithIdFields);

        BArray[] metadata = getMetadata(recordType);
        BArray fields = metadata[0];
        BArray includes = metadata[1];
        BArray typeDescriptions = metadata[2];
        BMap<BString, Object> typeMap = getFieldTypes(recordType);

        Map<String, Object> trxContextProperties = getTransactionContextProperties();
        return env.yieldAndRun(() -> {
            try {
                Object result = env.getRuntime().callMethod(
                        // Call `RedisClient.runReadQuery(
                        // typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [],
                        // string[] include = []
                        // )`
                        // which returns `stream<record{}|error?>|persist:Error`
                        persistClient, RUN_READ_QUERY_METHOD, new StrandMetadata(false, trxContextProperties),
                        targetTypeWithIdFields, typeMap, fields, includes);
                if (result instanceof BStream bStream) { // stream<record {}, redis:Error?>
                    return Utils.createPersistGSheetsStreamValue(bStream, targetType, fields, includes,
                            typeDescriptions, persistClient, null);
                }
                // persist:Error
                return Utils.createPersistGSheetsStreamValue(null, targetType, fields, includes, typeDescriptions,
                        persistClient, (BError) result);
            } catch (BError bError) {
                return Utils.createPersistGSheetsStreamValue(null, targetType, fields, includes, typeDescriptions,
                        persistClient, bError);
            }
        });
    }

    public static BStream queryStream(Environment env, BObject client, BTypedesc targetType) {
        // This method will return `stream<targetType, persist:Error?>`

        BString entity = getEntityFromStreamMethod(env);
        BObject persistClient = getPersistClient(client, entity);
        BArray keyFields = (BArray) persistClient.get(KEY_FIELDS);
        RecordType recordType = (RecordType) targetType.getDescribingType();

        RecordType recordTypeWithIdFields = getRecordTypeWithKeyFields(keyFields, recordType);
        BTypedesc targetTypeWithIdFields = ValueCreator.createTypedescValue(recordTypeWithIdFields);

        BArray[] metadata = getMetadata(recordType);
        BArray fields = metadata[0];
        BArray includes = metadata[1];
        BArray typeDescriptions = metadata[2];
        BMap<BString, Object> typeMap = getFieldTypes(recordType);

        Map<String, Object> trxContextProperties = getTransactionContextProperties();
        return env.yieldAndRun(() -> {
            try {
                Object result = env.getRuntime().callMethod(
                        // Call `RedisClient.runReadQuery(
                        // typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [],
                        // string[] include = []
                        // )`
                        // which returns `stream<record{}|error?>|persist:Error`
                        persistClient, RUN_READ_TABLE_AS_STREAM_METHOD, new StrandMetadata(false, trxContextProperties),
                        targetTypeWithIdFields, typeMap, fields, includes);
                if (result instanceof BStream bStream) { // stream<record {}, redis:Error?>
                    return Utils.createPersistGSheetsStreamValue(bStream, targetType, fields, includes,
                            typeDescriptions, persistClient, null);
                }
                // persist:Error
                return Utils.createPersistGSheetsStreamValue(null, targetType, fields, includes,
                        typeDescriptions, persistClient, (BError) result);
            } catch (BError bError) {
                return Utils.createPersistGSheetsStreamValue(null, targetType, fields, includes, typeDescriptions,
                        persistClient, bError);
            }
        });
    }

    public static Object queryOne(Environment env, BObject client, BArray path, BTypedesc targetType) {
        // This method will return `targetType|persist:Error`

        BString entity = getEntity(env);
        BObject persistClient = getPersistClient(client, entity);
        BArray keyFields = (BArray) persistClient.get(KEY_FIELDS);
        RecordType recordType = (RecordType) targetType.getDescribingType();

        RecordType recordTypeWithIdFields = getRecordTypeWithKeyFields(keyFields, recordType);
        BTypedesc targetTypeWithIdFields = ValueCreator.createTypedescValue(recordTypeWithIdFields);

        BArray[] metadata = getMetadata(recordType);
        BArray fields = metadata[0];
        BArray includes = metadata[1];
        BArray typeDescriptions = metadata[2];
        BMap<BString, Object> typeMap = getFieldTypes(recordType);
        Object key = getKey(env, path);

        Map<String, Object> trxContextProperties = getTransactionContextProperties();
        return env.yieldAndRun(() -> {
            try {
                return  env.getRuntime().callMethod(
                        // Call `RedisClient.runReadQuery(
                        // typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [],
                        // string[] include = []
                        // )`
                        // which returns `stream<record{}|error?>|persist:Error`
                        persistClient, RUN_READ_TABLE_AS_STREAM_METHOD, new StrandMetadata(false, trxContextProperties),
                        targetType, targetTypeWithIdFields, typeMap, key, fields, includes, typeDescriptions);
            } catch (BError bError) {
                return wrapError(bError);
            }
        });
    }
}
