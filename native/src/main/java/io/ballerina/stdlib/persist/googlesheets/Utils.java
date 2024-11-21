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

package io.ballerina.stdlib.persist.googlesheets;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.util.Locale;
import java.util.Map;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static io.ballerina.stdlib.persist.googlesheets.Constants.PERSIST_GOOGLE_SHEETS_STREAM;
import static io.ballerina.stdlib.persist.googlesheets.ModuleUtils.getModule;

/**
 * This class has the utility methods required for the Persist module.
 *
 * @since 0.1.0
 */
public class Utils {
    public static BString getEntityFromStreamMethod(Environment env) {
        String functionName = env.getFunctionName();
        String entity = functionName.substring(5, functionName.length() - 6).toLowerCase(Locale.ENGLISH);
        return fromString(entity);
    }
    public static BMap<BString, Object> getFieldTypes(RecordType recordType) {
        MapType stringMapType = TypeCreator.createMapType(PredefinedTypes.TYPE_STRING);
        BMap<BString, Object> typeMap = ValueCreator.createMapValue(stringMapType);
        Map<String, Field> fieldsMap = recordType.getFields();
        for (Field field : fieldsMap.values()) {

            Type type = field.getFieldType();
            String fieldName = field.getFieldName();
            typeMap.put(StringUtils.fromString(fieldName), StringUtils.fromString(type.getName()));
        }
        return typeMap;
    }

    private static BObject createPersistGSheetsStream(BStream sqlStream, BTypedesc targetType, BArray fields,
                                                      BArray includes, BArray typeDescriptions, BObject persistClient,
                                                      BError persistError) {
        return ValueCreator.createObjectValue(getModule(), PERSIST_GOOGLE_SHEETS_STREAM,
                sqlStream, targetType, fields, includes, typeDescriptions, persistClient, persistError);
    }

    private static BStream createPersistGSheetsStreamValue(BTypedesc targetType, BObject persistSQLStream) {
        RecordType streamConstraint =
                (RecordType) TypeUtils.getReferredType(targetType.getDescribingType());
        return ValueCreator.createStreamValue(
                TypeCreator.createStreamType(streamConstraint, PredefinedTypes.TYPE_NULL), persistSQLStream);
    }

    public static BStream createPersistGSheetsStreamValue(BStream sqlStream, BTypedesc targetType, BArray fields,
                                                          BArray includes, BArray typeDescriptions,
                                                          BObject persistClient, BError persistError) {
        BObject persistSQLStream = createPersistGSheetsStream(sqlStream, targetType, fields, includes, typeDescriptions,
                persistClient, persistError);
        return createPersistGSheetsStreamValue(targetType, persistSQLStream);
    }
}
