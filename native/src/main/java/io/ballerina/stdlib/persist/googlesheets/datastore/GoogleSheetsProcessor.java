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
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ErrorType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StreamType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.persist.ModuleUtils;
import io.ballerina.stdlib.persist.googlesheets.Constants;

import static io.ballerina.stdlib.persist.Constants.ERROR;
import static io.ballerina.stdlib.persist.Constants.KEY_FIELDS;
import static io.ballerina.stdlib.persist.Constants.RUN_READ_BY_KEY_QUERY_METHOD;
import static io.ballerina.stdlib.persist.Constants.RUN_READ_QUERY_METHOD;
import static io.ballerina.stdlib.persist.Utils.getEntity;
import static io.ballerina.stdlib.persist.Utils.getKey;
import static io.ballerina.stdlib.persist.Utils.getMetadata;
import static io.ballerina.stdlib.persist.Utils.getPersistClient;
import static io.ballerina.stdlib.persist.Utils.getRecordTypeWithKeyFields;
import static  io.ballerina.stdlib.persist.googlesheets.ModuleUtils.getModule;
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

        Future balFuture = env.markAsync();
        env.getRuntime().invokeMethodAsyncSequentially(
                persistClient, RUN_READ_QUERY_METHOD,
                null, null, new Callback() {
                    @Override
                    public void notifySuccess(Object o) {
                        RecordType streamConstraint =
                                    (RecordType) TypeUtils.getReferredType(targetType.getDescribingType());
                        if (o instanceof BError) {
                            BError error;
                            if (!((BError) o).getType().getPkg().getName().equals(Constants.PERSIST)) {
                                error = ErrorCreator.createError(ModuleUtils.getModule(), Constants.ERROR,
                                        ((BError) o).getErrorMessage(), ((BError) o).getCause(), null);
                            } else {
                                error = (BError) o;
                            }
                            BObject persistStream = ValueCreator.createObjectValue(
                                    getModule(), Constants.PERSIST_GOOGLE_SHEETS_STREAM,
                                    null, targetType,
                                    fields, includes, typeDescriptions, persistClient, error
                            );
                            balFuture.complete(
                                    ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                                            PredefinedTypes.TYPE_NULL), persistStream)
                            );
                        } else {
                            BStream gSheetStream = (BStream) o;
                            BObject persistStream = ValueCreator.createObjectValue(
                                    getModule(), Constants.PERSIST_GOOGLE_SHEETS_STREAM,
                                    gSheetStream, targetType,
                                    fields, includes, typeDescriptions, persistClient, null
                            );
                            balFuture.complete(
                                    ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                                            PredefinedTypes.TYPE_NULL), persistStream)
                            );
                        }
                    }

                    @Override
                    public void notifyFailure(BError bError) {
                        BError error;
                        if (!bError.getType().getPkg().getName().equals(Constants.PERSIST)) {
                            error = ErrorCreator.createError(ModuleUtils.getModule(), Constants.ERROR,
                                    bError.getErrorMessage(), bError.getCause(), null);
                        } else {
                            error = bError;
                        }
                        RecordType streamConstraint =
                                (RecordType) TypeUtils.getReferredType(targetType.getDescribingType());
                        BObject persistStream = ValueCreator.createObjectValue(
                                getModule(), Constants.PERSIST_GOOGLE_SHEETS_STREAM,
                                null, targetType,
                                fields, includes, typeDescriptions, persistClient, error
                        );
                        balFuture.complete(
                                ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                                        PredefinedTypes.TYPE_NULL), persistStream)
                        );
                    }
                }, null, streamTypeWithIdFields,
                targetTypeWithIdFields, true, typeMap, true, fields, true, includes, true
        );

        return null;
    }

    public static BStream queryStream(Environment env, BObject client, BTypedesc targetType) {
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

        Future balFuture = env.markAsync();
        env.getRuntime().invokeMethodAsyncSequentially(
                persistClient, Constants.RUN_READ_TABLE_AS_STREAM_METHOD,
                null, null, new Callback() {
                    @Override
                    public void notifySuccess(Object o) {
                        RecordType streamConstraint =
                                    (RecordType) TypeUtils.getReferredType(targetType.getDescribingType());
                        if (o instanceof BError) {
                            BError error;
                            if (!((BError) o).getType().getPkg().getName().equals(Constants.PERSIST)) {
                                error = ErrorCreator.createError(ModuleUtils.getModule(), Constants.ERROR,
                                        ((BError) o).getErrorMessage(), ((BError) o).getCause(), null);
                            } else {
                                error = (BError) o;
                            }
                            BObject persistStream = ValueCreator.createObjectValue(
                                    getModule(), Constants.PERSIST_GOOGLE_SHEETS_STREAM,
                                    null, targetType,
                                    fields, includes, typeDescriptions, persistClient, error
                            );
                            balFuture.complete(
                                    ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                                            PredefinedTypes.TYPE_NULL), persistStream)
                            );
                        } else {
                            BStream gSheetStream = (BStream) o;
                            BObject persistStream = ValueCreator.createObjectValue(
                                    getModule(), Constants.PERSIST_GOOGLE_SHEETS_STREAM,
                                    gSheetStream, targetType,
                                    fields, includes, typeDescriptions, persistClient, null
                            );
                            balFuture.complete(
                                    ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                                            PredefinedTypes.TYPE_NULL), persistStream)
                            );
                        }
                    }
                    @Override
                    public void notifyFailure(BError bError) {
                        BError error;
                        if (!bError.getType().getPkg().getName().equals(Constants.PERSIST)) {
                            error = ErrorCreator.createError(ModuleUtils.getModule(), Constants.ERROR,
                                    bError.getErrorMessage(), bError.getCause(), null);
                        } else {
                            error = bError;
                        }
                        balFuture.complete(error);
                    }
                }, null, streamTypeWithIdFields,
                targetTypeWithIdFields, true, typeMap, true, fields, true, includes, true
        );
        return null;
    }

    public static Object queryOne(Environment env, BObject client, BArray path, BTypedesc targetType) {
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
        Future balFuture = env.markAsync();
        env.getRuntime().invokeMethodAsyncSequentially(
                getPersistClient(client, entity), RUN_READ_BY_KEY_QUERY_METHOD,
                null, null, new Callback() {
                    @Override
                    public void notifySuccess(Object o) {
                        if (o instanceof BError &&
                                !((BError) o).getType().getPkg().getName().equals(Constants.PERSIST)) {
                            BError bError = ErrorCreator.createError(ModuleUtils.getModule(), Constants.ERROR,
                             ((BError) o).getErrorMessage(), ((BError) o).getCause(), null);
                            balFuture.complete(bError);
                        } else {
                            balFuture.complete(o);
                        }
                    }

                    @Override
                    public void notifyFailure(BError error) {
                        BError bError;
                        if (!error.getType().getPkg().getName().equals(Constants.PERSIST)) {
                            bError = ErrorCreator.createError(ModuleUtils.getModule(), Constants.ERROR,
                                    error.getErrorMessage(), null, null);
                        } else {
                            bError = error;
                        }
                        balFuture.complete(bError);
                    }
                },  null, unionType,
                targetType, true, targetTypeWithIdFields, true, typeMap, true, key, true, fields, true, includes, true,
                typeDescriptions, true
        );
        return null;
    }
}
