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
import io.ballerina.runtime.api.Future;
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.async.Callback;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ErrorType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StreamType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.persist.ModuleUtils;
import io.ballerina.stdlib.persist.googlesheets.Constants;
import io.ballerina.stdlib.persist.googlesheets.Utils;

import java.util.Map;

import static io.ballerina.stdlib.persist.Constants.ERROR;
import static io.ballerina.stdlib.persist.Constants.KEY_FIELDS;
import static io.ballerina.stdlib.persist.Constants.RUN_READ_BY_KEY_QUERY_METHOD;
import static io.ballerina.stdlib.persist.Constants.RUN_READ_QUERY_METHOD;
import static io.ballerina.stdlib.persist.ErrorGenerator.wrapError;
import static io.ballerina.stdlib.persist.Utils.getEntity;
import static io.ballerina.stdlib.persist.Utils.getKey;
import static io.ballerina.stdlib.persist.Utils.getMetadata;
import static io.ballerina.stdlib.persist.Utils.getPersistClient;
import static io.ballerina.stdlib.persist.Utils.getRecordTypeWithKeyFields;
import static io.ballerina.stdlib.persist.Utils.getTransactionContextProperties;
import static io.ballerina.stdlib.persist.googlesheets.Utils.getEntityFromStreamMethod;
import static io.ballerina.stdlib.persist.googlesheets.Utils.getFieldTypes;


/**
 * This class provides the GoogleSheets query processing implementations for persistence.
 *
 * @since 0.3.0
 */

public class GoogleSheetsProcessor {

    private GoogleSheetsProcessor() {};

    public static BStream query(Environment env, BObject client, BTypedesc targetType) {
        // This method will return `stream<targetType, persist:Error?>`

        BString entity = getEntity(env);
        BObject persistClient = getPersistClient(client, entity);
        BArray keyFields = (BArray) persistClient.get(KEY_FIELDS);
        RecordType recordType = (RecordType) targetType.getDescribingType();

        RecordType recordTypeWithIdFields = getRecordTypeWithKeyFields(keyFields, recordType);
        BTypedesc targetTypeWithIdFields = ValueCreator.createTypedescValue(recordTypeWithIdFields);
        StreamType streamTypeWithIdFields = TypeCreator.createStreamType(recordTypeWithIdFields,
                PredefinedTypes.TYPE_NULL);

        BArray[] metadata = getMetadata(recordType);
        BArray fields = metadata[0];
        BArray includes = metadata[1];
        BArray typeDescriptions = metadata[2];
        BMap<BString, Object> typeMap = getFieldTypes(recordType);

        Map<String, Object> trxContextProperties = getTransactionContextProperties();
        String strandName = env.getStrandName().isPresent() ? env.getStrandName().get() : null;

        Future balFuture = env.markAsync();
        env.getRuntime().invokeMethodAsyncSequentially(
                // Call `GoogleSheetsClient.runReadQuery(
                //      typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [], string[] include = []
                // ` which returns `stream<record {}, error?>|persist:Error`

                persistClient, RUN_READ_QUERY_METHOD, strandName, env.getStrandMetadata(), new Callback() {
                    @Override
                    public void notifySuccess(Object o) {
                        if (o instanceof BStream) { // stream<record {}, error?>
                            BStream gSheetStream = (BStream) o;
                            balFuture.complete(Utils.createPersistGSheetsStreamValue(gSheetStream, targetType, fields,
                                    includes, typeDescriptions, persistClient, null));
                        } else { // persist:Error
                            BError persistError = (BError) o;
                            balFuture.complete(Utils.createPersistGSheetsStreamValue(null, targetType, fields, includes,
                                    typeDescriptions, persistClient, persistError));
                        }
                    }

                    @Override
                    public void notifyFailure(BError bError) {
                        BError persistError = wrapError(bError);
                        balFuture.complete(Utils.createPersistGSheetsStreamValue(null, targetType, fields, includes,
                                typeDescriptions, persistClient, persistError));
                    }
                }, trxContextProperties, streamTypeWithIdFields,
                targetTypeWithIdFields, true, typeMap, true, fields, true, includes, true
        );

        return null;
    }

    public static BStream queryStream(Environment env, BObject client, BTypedesc targetType) {
        // This method will return `stream<targetType, persist:Error?>`

        BString entity = getEntityFromStreamMethod(env);
        BObject persistClient = getPersistClient(client, entity);
        BArray keyFields = (BArray) persistClient.get(KEY_FIELDS);
        RecordType recordType = (RecordType) targetType.getDescribingType();

        RecordType recordTypeWithIdFields = getRecordTypeWithKeyFields(keyFields, recordType);
        BTypedesc targetTypeWithIdFields = ValueCreator.createTypedescValue(recordTypeWithIdFields);
        StreamType streamTypeWithIdFields = TypeCreator.createStreamType(recordTypeWithIdFields,
                PredefinedTypes.TYPE_NULL);

        BArray[] metadata = getMetadata(recordType);
        BArray fields = metadata[0];
        BArray includes = metadata[1];
        BArray typeDescriptions = metadata[2];
        BMap<BString, Object> typeMap = getFieldTypes(recordType);

        Map<String, Object> trxContextProperties = getTransactionContextProperties();
        String strandName = env.getStrandName().isPresent() ? env.getStrandName().get() : null;

        Future balFuture = env.markAsync();
        env.getRuntime().invokeMethodAsyncSequentially(
                // Call `GoogleSheetsClient.readTableAsStream(
                //      typedesc<record {}> rowType, map<anydata> typeMap, string[] fields = [], string[] include = []
                // )' which returns `stream<record {}, persist:Error?>|persist:Error`

                persistClient, Constants.RUN_READ_TABLE_AS_STREAM_METHOD, strandName, env.getStrandMetadata(),
                new Callback() {
                    @Override
                    public void notifySuccess(Object o) {
                        if (o instanceof BStream) { // stream<record {}, persist:Error?>
                            BStream gSheetStream = (BStream) o;
                            balFuture.complete(Utils.createPersistGSheetsStreamValue(gSheetStream, targetType, fields,
                                    includes, typeDescriptions, persistClient, null));
                        } else { // persist:Error
                            BError persistError = (BError) o;
                            balFuture.complete(Utils.createPersistGSheetsStreamValue(null, targetType, fields, includes,
                                    typeDescriptions, persistClient, persistError));
                        }
                    }

                    @Override
                    public void notifyFailure(BError bError) {
                        BError persistError = wrapError(bError);
                        balFuture.complete(Utils.createPersistGSheetsStreamValue(null, targetType, fields, includes,
                                typeDescriptions, persistClient, persistError));
                    }
                }, trxContextProperties, streamTypeWithIdFields,
                targetTypeWithIdFields, true, typeMap, true, fields, true, includes, true
        );
        return null;
    }

    public static Object queryOne(Environment env, BObject client, BArray path, BTypedesc targetType) {
        // This method will return `targetType|persist:Error`

        BString entity = getEntity(env);
        BObject persistClient = getPersistClient(client, entity);
        BArray keyFields = (BArray) persistClient.get(KEY_FIELDS);
        RecordType recordType = (RecordType) targetType.getDescribingType();

        RecordType recordTypeWithIdFields = getRecordTypeWithKeyFields(keyFields, recordType);
        BTypedesc targetTypeWithIdFields = ValueCreator.createTypedescValue(recordTypeWithIdFields);
        ErrorType persistErrorType = TypeCreator.createErrorType(ERROR, ModuleUtils.getModule());
        Type unionType = TypeCreator.createUnionType(recordTypeWithIdFields, persistErrorType);

        BArray[] metadata = getMetadata(recordType);
        BArray fields = metadata[0];
        BArray includes = metadata[1];
        BArray typeDescriptions = metadata[2];
        BMap<BString, Object> typeMap = getFieldTypes(recordType);
        Object key = getKey(env, path);

        Map<String, Object> trxContextProperties = getTransactionContextProperties();
        String strandName = env.getStrandName().isPresent() ? env.getStrandName().get() : null;

        Future balFuture = env.markAsync();

        env.getRuntime().invokeMethodAsyncSequentially(
                // Call `GoogleSheetsClient.runReadByKeyQuery(
                //      typedesc<record {}> rowType, typedesc<record {}> rowTypeWithIdFields, map<anydata> typeMap,
                //      anydata key, string[] fields = [], string[] include = [],
                //      typedesc<record {}>[] typeDescriptions = []
                // ) returns record {}|persist:Error

                getPersistClient(client, entity), RUN_READ_BY_KEY_QUERY_METHOD, strandName, env.getStrandMetadata(),
                new Callback() {
                    @Override
                    public void notifySuccess(Object o) {
                        balFuture.complete(o);
                    }

                    @Override
                    public void notifyFailure(BError bError) {
                        BError persistError = wrapError(bError);
                        balFuture.complete(persistError);
                    }
                },  trxContextProperties, unionType,
                targetType, true, targetTypeWithIdFields, true, typeMap, true, key, true, fields, true, includes, true,
                typeDescriptions, true
        );
        return null;
    }
}
