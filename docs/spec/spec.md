# Specification: Ballerina Persist Google Sheets Library

_Owners_: @daneshk @sahanhe  
_Reviewers_: @daneshk  
_Created_: 2026/11/06  
_Updated_: 2026/11/06  
_Edition_: Swan Lake  

## Introduction

This is the specification for the `persist.googlesheets` library of the [Ballerina language](https://ballerina.io/), which provides the Google Sheets datastore of the [`persist` library](https://github.com/ballerina-platform/module-ballerina-persist/blob/main/docs/spec/spec.md). It covers what is specific to Google Sheets: the supported types, the configuration of the generated client, how entities are stored in a spreadsheet, and how the generated client behaves. The data model, the derived entity types, the generated client API and the error types are specified in the [persist specification](https://github.com/ballerina-platform/module-ballerina-persist/blob/main/docs/spec/spec.md), and the `bal persist` commands and the code generation are specified in the [persist tools specification](https://github.com/ballerina-platform/persist-tools/blob/main/docs/spec/spec.md).

The conforming implementation of the specification is released and included in the distribution. Any deviation from the specification is considered a bug. The released versions of the specification can be found under the relevant GitHub tag.

### Notation

The key words MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT, SHOULD, SHOULD NOT, RECOMMENDED, MAY and OPTIONAL are interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) and [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174) when, and only when, they appear in all capitals. Sentences that state behaviour in the present tense are also normative. The overview, notes and examples are informative.

## Contents

- [1. Overview](#1-overview)
- [2. Terminology](#2-terminology)
- [3. Supported types](#3-supported-types)
    - [3.1. Description](#31-description)
    - [3.2. Behaviour](#32-behaviour)
    - [3.3. Compile-time validation](#33-compile-time-validation)
- [4. Configuration](#4-configuration)
    - [4.1. Description](#41-description)
    - [4.2. Configurable variables](#42-configurable-variables)
    - [4.3. Behaviour](#43-behaviour)
    - [4.4. Errors](#44-errors)
- [5. Storage layout](#5-storage-layout)
    - [5.1. Description](#51-description)
    - [5.2. Sheets and columns](#52-sheets-and-columns)
    - [5.3. Row identity](#53-row-identity)
    - [5.4. Value representation](#54-value-representation)
- [6. The Google Sheets client](#6-the-google-sheets-client)
    - [6.1. Description](#61-description)
    - [6.2. Reading records](#62-reading-records)
    - [6.3. Writing records](#63-writing-records)
    - [6.4. Relations](#64-relations)
    - [6.5. Errors](#65-errors)
- [7. Transactions](#7-transactions)
- [8. Security considerations](#8-security-considerations)
- [References](#references)

## 1. Overview

The `persist.googlesheets` library lets a persist client store the records of a data model in a Google Sheets spreadsheet. The application defines the data model as specified in [data model definition](https://github.com/ballerina-platform/module-ballerina-persist/blob/main/docs/spec/spec.md#3-data-model-definition) of the persist specification, and selects the `googlesheets` datastore for it. The persist tool then generates the derived entity types, a client that uses this library, the configurable variables that the client needs, and a Google Apps Script file that creates the sheets of the data model.

Each entity is stored in its own sheet of one spreadsheet, with one column per field and one row per record. The client reaches the spreadsheet through the Google Sheets API, authenticating with OAuth 2.0.

The Google Sheets datastore is experimental. When the persist tool generates a client for the `googlesheets` datastore, it prints the warning `The support for Google Sheets data store is currently an experimental feature, and its behavior may be subject to change in future releases.`

## 2. Terminology

- **Spreadsheet:** the Google Sheets document, identified by its spreadsheet ID, that holds the data of a data model.
- **Sheet:** a tab of the spreadsheet. Each entity is stored in one sheet.
- **Header row:** the first row of a sheet, which holds the column names.
- **Row metadata:** the Google Sheets developer metadata attached to a row, which the client uses to find the row of a record, as specified in [5.3](#53-row-identity).
- **Generated client:** the `Client` class that the persist tool generates for the `googlesheets` datastore. Its API is specified in [derived entity types and persist clients](https://github.com/ballerina-platform/module-ballerina-persist/blob/main/docs/spec/spec.md#4-derived-entity-types-and-persist-clients) of the persist specification.
- **Time types:** `time:Date`, `time:TimeOfDay`, `time:Utc` and `time:Civil`.

Entity, identity field, relation field, owner, foreign key, insert type, update type and target type have the meanings given in the persist specification.

## 3. Supported types

### 3.1. Description

The Google Sheets datastore supports a fixed set of field types, and does not support nillable fields of the built-in simple types or of enum types.

### 3.2. Behaviour

1. A non-relation field of an entity MUST have one of these types: `int`, `boolean`, `decimal`, `float`, `string`, an enum type declared in the model definition file, or a time type.
2. Array types, including `byte[]`, are not supported.
3. A non-relation field MUST NOT be nillable, `T? field`, when `T` is `int`, `boolean`, `decimal`, `float`, `string` or an enum type.
4. The identity fields and the optional fields of an entity follow the rules of the persist specification.

For example, with the `googlesheets` datastore:

```ballerina
type Product record {|
    readonly int id;
    string name;
    decimal price;
    time:Date addedOn;
    string? description; // Rejected with PERSIST_308
    byte[] image;        // Rejected with PERSIST_306
|};
```

### 3.3. Compile-time validation

The `persist` compiler plugin applies these rules when the `Ballerina.toml` file of the package configures the `googlesheets` datastore for the model, and reports each condition as an error:

| Condition | Code |
|---|---|
| A field's type is not one of the types of [3.2](#32-behaviour) rule 1 | `PERSIST_305` |
| A field's type is an array type | `PERSIST_306` |
| A field is nillable, and its type is `int`, `boolean`, `decimal`, `float`, `string` or an enum type | `PERSIST_308` |

The other conditions that the compiler plugin reports are specified in the persist specification.

## 4. Configuration

### 4.1. Description

The generated client reads the ID of the spreadsheet and the OAuth 2.0 credentials from configurable variables, which the persist tool declares and adds to `Config.toml`.

### 4.2. Configurable variables

The persist tool declares these configurable variables in the `persist_sheet_config.bal` file of the generated module:

| Variable | Type | Default | Meaning |
|---|---|---|---|
| `spreadsheetId` | `string` | Required | The ID of the spreadsheet that holds the sheets of the data model. |
| `clientId` | `string` | Required | The client ID of the OAuth 2.0 client. |
| `clientSecret` | `string` | Required | The client secret of the OAuth 2.0 client. |
| `refreshToken` | `string` | Required | The OAuth 2.0 refresh token that the client uses to obtain access tokens. |

### 4.3. Behaviour

1. The persist tool creates `persist_sheet_config.bal` only when the file does not exist, and adds a table named `<package>.<module>` to the `Config.toml` file of the package, with each variable of [4.2](#42-configurable-variables) set to the empty string. The application sets the values before running.
2. The persist tool adds a `[[dependency]]` on `ballerinax/googleapis.sheets` version `3.0.0` to `Ballerina.toml`, so that the generated client is resolved against a compatible version of the Google Sheets connector or a later one.
3. The `init` function of the generated client:
    1. Creates a Google Sheets connector client and an HTTP client for `https://sheets.googleapis.com/v4/spreadsheets/<spreadsheetId>/values`. Both use the OAuth 2.0 refresh token grant with `clientId`, `clientSecret` and `refreshToken`, and the token endpoint `sheets:REFRESH_URL`.
    2. Gets, for each entity, the sheet whose name is the sheet name of the entity, as specified in [5.2](#52-sheets-and-columns).
4. The `init` function does not check the header rows. They are checked when a sheet is read, as specified in [6.2](#62-reading-records).
5. The `close` function does nothing, and returns `()`.

For example, the generated `Config.toml` entry for the module `entities` of the package `shop` is:

```toml
[shop.entities]
spreadsheetId = ""
clientId = ""
clientSecret = ""
refreshToken = ""
```

### 4.4. Errors

The `init` function returns a `persist:Error` with the message of the underlying error when:

- the HTTP client or the Google Sheets connector client cannot be created; or
- the sheet of an entity cannot be got, such as when the spreadsheet has no sheet with that name or the credentials are rejected.

## 5. Storage layout

### 5.1. Description

Each entity is stored in a sheet of the spreadsheet. Each record is a row of that sheet, and each non-relation field and foreign key field is a column.

### 5.2. Sheets and columns

1. The sheet name of an entity is the entity name, without a leading `'`.
2. The columns of an entity are its non-relation fields and, for each relation that the entity owns, its foreign key fields, in the order in which the fields are declared. The foreign key fields of a relation take the place of the relation field.
3. A column name is the field name, without a leading `'`.
4. The first column is `A`. An entity MUST NOT have more than 26 columns: the persist tool fails to generate the client for an entity with more columns.
5. The first row of a sheet is the header row, and holds the column names. Each later row holds one record.
6. The `@sql:Name` annotation does not change sheet or column names, and the `@sql:Generated` annotation does not make the datastore generate a value: the field stays in the insert type, and the application gives its value.
7. The persist tool generates a Google Apps Script file, `script.gs`, with a `createSheets` function. For each entity, the function deletes the sheet with the sheet name of the entity if it exists, inserts a new sheet with that name, and appends the header row. It then deletes the sheet named `Sheet1` if it exists. The application runs the script in the spreadsheet before using the client.

For example, for these entities:

```ballerina
type Department record {|
    readonly string deptNo;
    string deptName;
    Employee[] employees;
|};

type Employee record {|
    readonly string empNo;
    string name;
    time:Date hireDate;
    Department department;
|};
```

the `Employee` sheet holding one record is:

| | A | B | C | D |
|---|---|---|---|---|
| 1 | `empNo` | `name` | `hireDate` | `departmentDeptNo` |
| 2 | `E1` | `Alice` | `3-1-2024` | `D1` |

### 5.3. Row identity

1. When the client inserts a record, it attaches row metadata to the new row. The metadata key is the sheet name of the entity, and the metadata value is the value of each identity field converted with `toString`, in the order of the identity fields, joined with `:`. The metadata is visible to everyone who can access the spreadsheet.
2. The client finds the row of a record for `post`, `put` and `delete` by its row metadata, as specified in [6.3](#63-writing-records). A row without row metadata, such as a row added in the Google Sheets user interface, is read by `get` and `get` by identity, but is not found by `put` and `delete`.

For example, the row of the `OrderItem` record with the identity fields `orderId = "order-1"` and `itemId = "item-1"` has the metadata key `OrderItem` and the metadata value `order-1:item-1`.

### 5.4. Value representation

1. The client writes the values of a row with the Google Sheets `USER_ENTERED` value input option, so Google Sheets parses each value as if a user typed it into the cell.
2. The client reads the values of a sheet as formatted values, and converts each value from its string form to the type of its field.
3. Values are written and read as follows:

    | Type | Written as | Read as |
    |---|---|---|
    | `int`, `float`, `decimal` | The number. | The number parsed from the string, with `int:fromString`, `float:fromString` or `decimal:fromString`. |
    | `boolean` | The boolean. | `true` when the string is `TRUE`, and `false` otherwise. |
    | `string`, enum types | The string. | The string. |
    | `time:Date` | `<day>-<month>-<year>`, without leading zeros. | The date parsed from that form. |
    | `time:TimeOfDay` | `<hour>-<minute>-<second>`, without leading zeros. | The time of day parsed from that form. |
    | `time:Utc` | The string of `time:utcToString`. | The value of `time:utcFromString`. |
    | `time:Civil` | `<YYYY>-<MM>-<DD>T<hh>:<mm>`, followed by `:<ss>` when the seconds are present, by `<+\|-><hh>:<mm>:<ss>` when the UTC offset is present, and by `(<abbreviation>)` when the time abbreviation is present. Each field except the year and the fraction of the seconds has two digits. The seconds of the UTC offset are `00` when absent. | The civil time parsed from that form. |

For example, the `time:Civil` value `{year: 2021, month: 4, day: 12, hour: 23, minute: 20, second: 50.52, utcOffset: {hours: -5, minutes: -30, seconds: 0}, timeAbbrev: "Asia/Colombo"}` is written as `2021-04-12T23:20:50.52-05:30:00(Asia/Colombo)`.

## 6. The Google Sheets client

### 6.1. Description

The generated client implements the resource methods of the persist specification over the sheets of the data model. It has no native SQL methods. The public types and functions of this library, such as `googlesheets:GoogleSheetsClient`, are used by the generated client, and their behaviour is specified through the resource methods of the generated client.

### 6.2. Reading records

1. To read the records of an entity, the client reads all the rows of its sheet with one request, and then:
    1. If the sheet has no rows, it fails with the message `Error: the spreadsheet is not initialised correctly. Spreadsheet is empty.`.
    2. If the sheet has only the header row, the entity has no records.
    3. If the number of columns in the header row differs from the number of columns of the entity, it fails with the message `Error: the spreadsheet is not initialised correctly. Number of columns in the sheet does not match with the entity. `.
    4. If a column name in the header row is not a column name of the entity, it fails with the message `Error: the spreadsheet is not initialised correctly. Column <name> is not defined in the data types.`.
    5. Otherwise, it converts each later row into a record, as specified in [5.4](#54-value-representation). If a value cannot be converted, it fails with the message of the conversion error.
2. `get` returns the records of the entity in the order of their rows. The client does not filter, sort or limit the records in the datastore: it reads every record of the entity, and of each entity related through a field of the target type, before it returns the first record of the stream.
3. `get` returns the fields of the target type, including the related records specified in [6.4](#64-relations). The identity fields are read for every record, and are removed from the result when the target type does not have them.
4. `get` by identity reads the records of the entity as in rule 1, and returns the first record whose identity fields equal the given identity, with the fields of the target type. It returns a `persist:NotFoundError` when no record has the identity.

### 6.3. Writing records

1. `post` inserts the given records one at a time, in order. For each record, the client:
    1. Looks up the rows whose row metadata is that of the record. If there is such a row, it returns a `persist:AlreadyExistsError`, whose key is a record of the identity fields of the record.
    2. Appends a row with the values of the record.
    3. Attaches the row metadata of the record to the appended row, as specified in [5.3](#53-row-identity).
2. The records that `post` inserted before a failure stay in the sheet. The lookup in step 1.1 and the append in step 1.2 are separate requests, so a record inserted by another client between them is not detected.
3. `put` updates the record with the given identity:
    1. The client looks up the rows whose row metadata is that of the identity. If there is no such row, it returns a `persist:NotFoundError`. If there is more than one, it returns a `persist:Error` with the message `Multiple elements found for given key: <key>`.
    2. It writes the whole row: the given identity for the identity fields, the values of the update record for the fields that it has, and the current values of the row for the other fields.
    3. It reads the record back with `get` by identity, and returns it as the persist specification specifies.
4. `delete` deletes the record with the given identity:
    1. The client reads the record with `get` by identity, which returns a `persist:NotFoundError` when no record has the identity.
    2. It looks up the rows whose row metadata is that of the identity. If there is no such row, it returns a `persist:Error` with the message `no element found for delete`.
    3. It deletes the rows whose row metadata is that of the identity, and returns the record read in step 1.

### 6.4. Relations

1. For a relation that the entity owns, the related record is the record of the related entity whose identity fields equal the foreign key fields of the record.
2. For a 1-n relation that the related entity owns, the related records are the records of the related entity whose foreign key fields equal the identity fields of the record. When there are none, the relation field is the empty array.
3. The client does not check foreign keys. `post` and `put` store a foreign key that refers to no record, and `delete` deletes a record that other records refer to. The client never returns a `persist:ConstraintViolationError`.

### 6.5. Errors

The resource methods return these errors. The error types are specified in the persist specification.

| Resource method | Condition | Error |
|---|---|---|
| `get` | A condition of [6.2](#62-reading-records) rule 1, the request to Google Sheets fails, or a related record cannot be read. | `persist:Error`, returned from the stream. The message of a failure to read related records is `unsupported data format`. |
| `get` by identity | No record has the identity. | `persist:NotFoundError` |
| `get` by identity | A condition of [6.2](#62-reading-records) rule 1, or the request to Google Sheets fails. | `persist:Error` |
| `post` | A row has the row metadata of a given record. | `persist:AlreadyExistsError` |
| `post`, `put` | A value cannot be converted to its stored form, or a request to Google Sheets fails. | `persist:Error` with the message of the underlying error |
| `put` | No row has the row metadata of the identity. | `persist:NotFoundError` |
| `put` | More than one row has the row metadata of the identity. | `persist:Error` |
| `put` | The record is updated, but cannot be read back. | `persist:ReadBackError`, as specified in the persist specification |
| `delete` | No record has the identity. | `persist:NotFoundError` |
| `delete` | No row has the row metadata of the identity. | `persist:Error` with the message `no element found for delete` |
| `delete` | The row metadata lookup or the deletion request fails. | `persist:Error` |

## 7. Transactions

The Google Sheets datastore does not support transactions. The resource methods of the generated client do not take part in a Ballerina `transaction` statement: each change is written to the spreadsheet when the resource method runs, and is not rolled back when the transaction fails.

## 8. Security considerations

1. The OAuth 2.0 client secret and refresh token are read from configurable variables, which are typically given in `Config.toml` in plain text. The client has every permission that the refresh token was granted; the library does not restrict the OAuth 2.0 scopes.
2. String values are written with the `USER_ENTERED` value input option, as specified in [5.4](#54-value-representation). The client does not escape them, so a string that starts with `=` is stored as a formula, which Google Sheets evaluates.
3. Anyone who can edit the spreadsheet can change its rows, header rows and row metadata. The client does not detect such changes, except as specified in [6.2](#62-reading-records).
4. The generated `script.gs` deletes the existing sheets whose names are the sheet names of the entities, and the sheet named `Sheet1`, with their data.

## References

- [Ballerina persist specification](https://github.com/ballerina-platform/module-ballerina-persist/blob/main/docs/spec/spec.md)
- [Ballerina persist tools specification](https://github.com/ballerina-platform/persist-tools/blob/main/docs/spec/spec.md)
- [Ballerina Google Sheets connector](https://central.ballerina.io/ballerinax/googleapis.sheets)
- [Google Sheets API: ValueInputOption](https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption)
- [Google Sheets API: Developer metadata](https://developers.google.com/sheets/api/guides/metadata)
- [Using OAuth 2.0 to access Google APIs](https://developers.google.com/identity/protocols/oauth2)
- [RFC 2119: Key words for use in RFCs to Indicate Requirement Levels](https://www.rfc-editor.org/rfc/rfc2119)
- [RFC 8174: Ambiguity of Uppercase vs Lowercase in RFC 2119 Key Words](https://www.rfc-editor.org/rfc/rfc8174)
