# UDM Utils - Data Transformation Framework

This module provides a comprehensive data transformation framework for the UDM (Unified Data Model) pipeline. It supports multi-target curation, array exploding, value normalization, and various data type transformations.

## Table of Contents
- [Overview](#overview)
- [Multi-Target Curation](#multi-target-curation)
- [Array Exploding](#array-exploding)
- [Configuration Structure](#configuration-structure)
- [Transformation Types](#transformation-types)
  - [String Transformations](#1-string-transformations)
  - [Numeric Transformations](#2-numeric-transformations)
  - [Date and Timestamp Transformations](#3-date-and-timestamp-transformations)
  - [Fee Rate Transformations](#4-fee-rate-transformations)
  - [Value Normalization](#5-value-normalization)
  - [Metadata Extraction](#6-metadata-extraction)
  - [S3 Path Transformations](#7-s3-path-transformations)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Performance Considerations](#performance-considerations)

## Overview

The UDM transformation framework processes data through configurable transformations and supports:

- **Multi-target curation** - Write to multiple target tables from single source
- **Array exploding** - Convert array fields into multiple rows
- **Value normalization** - Standardize values using categorized mappings
- **Data type conversions** - String, numeric, date, and timestamp transformations
- **Data quality** - Missing value handling and data cleaning
- **Nested data flattening** - Extract fields from nested JSON structures

## Multi-Target Curation

### Overview
Multi-target curation allows writing different column sets to multiple target tables from a single source dataset. This enables normalized data structures and specialized views.

### Configuration
```yaml
curation:
  - name: contracts_main
    target_table_name: contracts_main_table
    columns: [contract_uuid, contract_name, operator_name]
  
  - name: contracts_fees
    target_table_name: contracts_fees_table
    columns: [contract_uuid, fee_name, fee_value, fee_unit]
```

### Processing Flow
1. **Prepare Phase** - Apply transformations and prepare data for all targets
2. **Write Phase** - Atomically write to all target tables
3. **Audit Phase** - Record processing results for each target

## Array Exploding

### Overview
Array exploding converts array fields into multiple rows, extracting specific fields from array elements and optionally renaming them.

### Example Transformation
```
Input:
contract_uuid: C001
fees: [
  {"name": "Storage", "amount": 50.0, "currency": "USD"},
  {"name": "Transport", "amount": 75.0, "currency": "USD"}
]

Output (after exploding):
Row 1: contract_uuid=C001, fee_name=Storage, fee_amount=50.0, fee_currency=USD
Row 2: contract_uuid=C001, fee_name=Transport, fee_amount=75.0, fee_currency=USD
```

### Configuration
```yaml
fees_data:
  type: array_explode
  source_path: "contract.fees"
  explode_columns:
    fee_name:
      source_field: "name"
      type: string
    fee_amount:
      source_field: "amount"
      type: float
    fee_currency:
      source_field: "currency"
      type: string
```

## Configuration Structure

### Basic Column Configuration
```yaml
column_name:
  type: string                    # Transformation type
  source_path: "nested.path"      # Path to source field (optional)
  missing_value_default: "N/A"   # Default for missing values
  skip_cleaning: false           # Skip automatic cleaning
  corrections:                   # Value corrections mapping
    "old_value": "new_value"
```

### Defaults Section
```yaml
defaults:
  string:
    missing_value_default: "Unknown"
  
  integer:
    missing_value_default: 0
  
  float:
    missing_value_default: 0.0
  
  date:
    date_overrides:
      "no expiration date": ""
      "TBD": ""
  
  value_normalization:
    business_line:
      "gathering": "Gathering and Processing"
      "processing": "Gathering and Processing"
    product:
      "crude oil": "Crude Oil"
      "refined products": "Refined Products"
```

## Transformation Types

### 1. String Transformations

#### Basic String Processing
```yaml
contract_name:
  type: string
  source_path: "contract.name"
  missing_value_default: "Unknown"
```

**Input/Output Example:**
```
Input:  "  Acme Corp Contract  "
Output: "Acme Corp Contract"        # Whitespace trimmed

Input:  null
Output: "Unknown"                    # Default applied
```

#### String with Case Conversion
```yaml
company_name:
  type: string
  lowercase: true        # Convert to lowercase
  # OR
  uppercase: true        # Convert to uppercase
  # OR
  title_case: true       # Title Case Each Word
  # OR
  capitalize: true       # Capitalize first letter only
```

**Input/Output Examples:**
```
# lowercase: true
Input:  "Acme Corporation"
Output: "acme corporation"

# uppercase: true
Input:  "Acme Corporation"
Output: "ACME CORPORATION"

# title_case: true
Input:  "acme corporation"
Output: "Acme Corporation"

# capitalize: true
Input:  "acmeCORP corporation"
Output: "Acmecorp corporation"     # Only first letter capitalized
```

#### String with Corrections
```yaml
status:
  type: string
  corrections:
    "active": "Active"
    "inactive": "Inactive"
    "pending": "Pending"
```

**Input/Output Example:**
```
Input:  "active"
Output: "Active"

Input:  "inactive"
Output: "Inactive"

Input:  "unknown_status"             # No mapping found
Output: "unknown_status"             # Unchanged
```

#### String Cleaning Options
```yaml
description:
  type: string
  skip_cleaning: true    # Skip automatic whitespace/regex cleaning
```

**Input/Output Example:**
```
# skip_cleaning: false (default)
Input:  "  Contract   with    multiple   spaces  "
Output: "Contract with multiple spaces"

# skip_cleaning: true
Input:  "  Contract   with    multiple   spaces  "
Output: "  Contract   with    multiple   spaces  "  # No cleaning applied
```

### 2. Numeric Transformations

#### Integer Fields
```yaml
frequency:
  type: integer
  missing_value_default: 0
```

**Input/Output Example:**
```
Input:  "123"
Output: 123                          # String to integer

Input:  "123.7"
Output: 123                          # Float truncated to integer

Input:  null
Output: 0                            # Default applied

Input:  "invalid"
Output: null                         # Invalid conversion
```

#### Float/Double Fields
```yaml
rate:
  type: float
  missing_value_default: 0.0

amount:
  type: double
  missing_value_default: 0.0
```

**Input/Output Examples:**
```
# Float conversion
Input:  "123.45"
Output: 123.45                       # String to float

Input:  "123"
Output: 123.0                        # Integer to float

Input:  null
Output: 0.0                          # Default applied

# Double conversion (higher precision)
Input:  "123.456789012345"
Output: 123.456789012345             # String to double

Input:  "invalid_number"
Output: null                         # Invalid conversion
```

### 3. Date and Timestamp Transformations

#### Basic Date Conversion
```yaml
contract_date:
  type: date
  missing_value_default: null
```

**Input/Output Example:**
```
Input:  "2024-03-15"
Output: 2024-03-15                   # String to date

Input:  "03/15/2024"
Output: 2024-03-15                   # Auto-parsed to standard format

Input:  null
Output: null                         # Default applied
```

#### Date with Format Conversion
```yaml
effective_date:
  type: date
  input_format: "MM/dd/yyyy"     # Input format
  target_format: "yyyy-MM-dd"    # Output format
```

**Input/Output Example:**
```
Input:  "03/15/2024"                 # MM/dd/yyyy format
Output: "2024-03-15"                 # yyyy-MM-dd format

Input:  "12/31/2023"
Output: "2023-12-31"

Input:  "invalid_date"
Output: null                         # Invalid format
```

#### Timestamp Conversion
```yaml
created_timestamp:
  type: timestamp
  input_format: "yyyy-MM-dd HH:mm:ss"
  target_format: "yyyy-MM-dd'T'HH:mm:ss'Z'"
```

**Input/Output Example:**
```
Input:  "2024-03-15 14:30:25"        # yyyy-MM-dd HH:mm:ss
Output: "2024-03-15T14:30:25Z"       # ISO format

Input:  "2024-03-15 09:15:00"
Output: "2024-03-15T09:15:00Z"
```

#### Date Overrides
```yaml
expiration_date:
  type: date
  date_overrides:
    "no expiration date": ""
    "exp_date_not_available": ""
    "TBD": ""
```

**Input/Output Example:**
```
Input:  "no expiration date"
Output: ""                           # Override applied

Input:  "TBD"
Output: ""                           # Override applied

Input:  "2024-12-31"
Output: 2024-12-31                   # Valid date unchanged
```

### 4. Fee Rate Transformations
```yaml
fee_rate:
  type: fee_rate
  invalid_values: ["N/A", "TBD", ""]
```

**Input/Output Example:**
```
Input:  "5.25%"
Output: 0.0525                       # Percentage to decimal

Input:  "12.5"
Output: 12.5                         # Numeric value

Input:  "N/A"
Output: null                         # Invalid value cleaned

Input:  "TBD"
Output: null                         # Invalid value cleaned

Input:  "$15.50"
Output: 15.5                         # Currency symbol removed
```

### 5. Value Normalization

#### Basic Value Normalization
```yaml
business_type:
  type: value_normalization
  normalization_key: "business_line"
```

**Input/Output Example:**
```
Input:  "gathering"
Output: "Gathering and Processing"     # Normalized using defaults mapping

Input:  "processing"
Output: "Gathering and Processing"     # Normalized using defaults mapping

Input:  "unknown_type"
Output: "unknown_type"               # No mapping found, unchanged
```

### 6. Metadata Extraction

#### Overview
Metadata extraction allows extracting specific fields from complex JSON structures by dynamically mapping field names to their corresponding keys in the metadata.

#### Basic Metadata Extraction
```yaml
document_number:
  type: metadata_extraction
  json_column: "raw_json_data"
  target_fields: ["Document Number", "Contract ID", "Effective Date"]
```

**Input/Output Example:**
```
Input JSON Structure:
{
  "metadata": {
    "categories": [
      {
        "doc_num": {"name": "Document Number", "type": "string"},
        "contract_id": {"name": "Contract ID", "type": "string"}
      }
    ]
  },
  "data": {
    "categories": [
      {
        "doc_num": "DOC-12345",
        "contract_id": "CTR-67890"
      }
    ]
  }
}

Output Columns:
- document_number: "DOC-12345"
- contract_id: "CTR-67890"
```

#### Metadata Extraction Features
- **Dynamic Key Mapping** - Automatically finds metadata keys by field names
- **Multiple Data Types** - Handles strings, arrays, objects, and booleans
- **Array Handling** - Converts arrays to JSON strings or extracts first non-null value
- **Null Safety** - Gracefully handles missing fields and null values
- **Column Naming** - Automatically converts field names to valid column names

### 7. S3 Path Transformations

#### Overview
S3 transformations extract values from S3 paths and prefixes, useful for extracting identifiers or folder names from file paths.

#### Extract from S3 Prefix
```yaml
contract_id:
  type: s3_prefix_extraction
  source_column: "s3_file_path"
  position: 4
```

**Input/Output Example:**
```
Input:  "s3://my-bucket/contracts/processed/8003247/contract.json"
Path after s3:// removal: "my-bucket/contracts/processed/8003247/contract.json"
Position 4 extraction: "8003247"
Output: "8003247"

Input:  "s3://data-bucket/invoices/2024/03/INV-12345/invoice.pdf"
Path after s3:// removal: "data-bucket/invoices/2024/03/INV-12345/invoice.pdf"
Position 5 extraction: "INV-12345"
Output: "INV-12345"
```

#### S3 Transformation Features
- **Position-Based Extraction** - Extract values by position in S3 path
- **Automatic Prefix Removal** - Removes s3:// prefix automatically
- **Null Safety** - Returns null if source column doesn't exist
- **Flexible Positioning** - Uses 1-based indexing for intuitive configuration: business_line    # References defaults.value_normalization.business_line
  missing_value_default: "Unknown"

defaults:
  value_normalization:
    business_line:
      "gathering": "Gathering and Processing"
      "processing": "Gathering and Processing"
      "others": "Gathering and Processing"
    product:
      "crude oil": "Crude Oil"
      "refined products": "Refined Products"
```

**Input/Output Example:**
```
# Using business_line normalization
Input:  "gathering"
Output: "Gathering and Processing"

Input:  "processing"
Output: "Gathering and Processing"

Input:  "unknown_type"           # No mapping found
Output: "unknown_type"           # Unchanged

Input:  null
Output: "Unknown"                # Default applied
```

#### Multiple Normalization Keys
```yaml
defaults:
  value_normalization:
    missing_value_default: "Unknown"
    business_line:
      "gathering": "Gathering and Processing"
      "processing": "Gathering and Processing"
    product:
      "crude oil": "Crude Oil"
      "refined products": "Refined Products"
    status:
      "active": "Active"
      "inactive": "Inactive"

columns:
  business_type:
    type: value_normalization
    normalization_key: business_line
  
  product_type:
    type: value_normalization
    normalization_key: product
  
  contract_status:
    type: value_normalization
    normalization_key: status
```

### 6. Struct (Complex Type Preservation)

#### Basic Struct Handling
```yaml
pipeline_locations:
  type: struct
  source_path: "attribute_summary.pipeline_locations.final_value.pipeline_locations"
  missing_value_default: null
```

**Input/Output Example:**
```
Input:  {"location_id": "L001", "name": "Houston Terminal", "capacity": 5000}
Output: {"location_id": "L001", "name": "Houston Terminal", "capacity": 5000}

Input:  null
Output: null                     # Default applied
```

**Use Cases:**
- Preserving nested JSON structures
- Maintaining complex data relationships
- Storing metadata objects

**Note**: Struct columns are preserved as-is without flattening. Use `source_path` to extract specific nested structs.

### 7. Array Explode (Advanced)

#### Complete Array Explode Configuration
```yaml
fees_data:
  type: array_explode
  source_path: "contract.fees"           # Path to array field
  explode_columns:
    fee_name:
      source_field: "name"               # Field within array element
      type: string
      missing_value_default: "Unknown"
    fee_amount:
      source_field: "amount"
      type: float
      missing_value_default: 0.0
    fee_currency:
      source_field: "currency"
      type: string
      missing_value_default: "USD"
    fee_rate:
      source_path: "rate.value"          # Nested path within array element
      type: float
      missing_value_default: 0.0
```

**Input/Output Example:**
```
Input Row:
contract_uuid: "C001"
contract: {
  "fees": [
    {"name": "Storage", "amount": 50.0, "currency": "USD", "rate": {"value": 0.05}},
    {"name": "Transport", "amount": 75.0, "currency": "USD", "rate": {"value": 0.08}}
  ]
}

Output Rows:
Row 1:
  contract_uuid: "C001"
  fee_name: "Storage"
  fee_amount: 50.0
  fee_currency: "USD"
  fee_rate: 0.05

Row 2:
  contract_uuid: "C001"
  fee_name: "Transport"
  fee_amount: 75.0
  fee_currency: "USD"
  fee_rate: 0.08
```

#### Array Explode with Empty Arrays
```yaml
fees_data:
  type: array_explode
  source_path: "contract.fees"
  explode_columns:
    fee_name:
      source_field: "name"
      type: string
      missing_value_default: "No Fee"
```

**Input/Output Example:**
```
Input (Empty Array):
contract_uuid: "C002"
contract: {"fees": []}

Output (Single Row with Defaults):
contract_uuid: "C002"
fee_name: "No Fee"               # Default applied

Input (Null Array):
contract_uuid: "C003"
contract: {"fees": null}

Output (Single Row with Defaults):
contract_uuid: "C003"
fee_name: "No Fee"               # Default applied
```

#### Array Explode with Multiple Source Paths
```yaml
fee_value:
  type: array_explode
  source_path: "fees"
  explode_columns:
    amount:
      source_path:                       # Multiple fallback paths
        - "rate.value.double"
        - "rate.value.long"
        - "rate.value"
      type: float
      missing_value_default: 0.0
```

**Processing Logic:**
1. Tries `rate.value.double` first
2. Falls back to `rate.value.long` if first fails
3. Falls back to `rate.value` if both fail
4. Uses default if all paths fail

### 8. Multiple Source Paths (Fallback Logic)

#### Coalesce Multiple Paths
```yaml
base_rate_value:
  type: float
  source_path:
    - "attribute_summary.rates.baserate.value.double"
    - "attribute_summary.rates.baserate.value.long"
    - "attribute_summary.rates.baserate.value"
  missing_value_default: 0.0
```

**Input/Output Example:**
```
# Scenario 1: First path exists
Input: {"attribute_summary": {"rates": {"baserate": {"value": {"double": 12.5}}}}}
Output: 12.5                     # From first path

# Scenario 2: First path missing, second exists
Input: {"attribute_summary": {"rates": {"baserate": {"value": {"long": 12}}}}}
Output: 12.0                     # From second path

# Scenario 3: All paths missing
Input: {"attribute_summary": {"rates": {}}}
Output: 0.0                      # Default applied
```

**Use Cases:**
- Handling schema variations
- Supporting data type changes over time
- Graceful degradation for missing fields

## Advanced Configuration Features

### 1. Nested Data Extraction

#### Deep Nested Paths
```yaml
operator_name:
  type: string
  source_path: "attribute_summary.operator_name.final_value.value"
```

**Extraction Methods:**
- **Struct Access**: For properly typed nested structures
- **JSON Extraction**: For JSON string columns using `get_json_object`
- **Automatic Fallback**: Framework tries struct access first, then JSON extraction

#### Complex Nested Structures
```yaml
shell_capacity_value:
  type: float
  source_path: "attribute_summary.max_shell_capacity_terminal.final_value.shell_capacity.value"
  missing_value_default: 0.0

shell_capacity_unit:
  type: string
  source_path: "attribute_summary.max_shell_capacity_terminal.final_value.shell_capacity.unit"
  missing_value_default: "BBL"
```

### 2. Data Quality and Cleaning

#### Invalid Value Handling
```yaml
fee_rate:
  type: fee_rate
  invalid_values:
    "VAR/TBD": null
    "VAR/TBD?": null
    "-": null
    "N/A": null
    "": null
    "data not available": null
    "0.00": null
```

**Processing:**
1. Replaces all invalid values with null
2. Converts remaining values to float
3. Handles percentage conversion ("5.25%" → 0.0525)
4. Removes currency symbols ("$15.50" → 15.5)

#### String Cleaning
```yaml
contract_name:
  type: string
  skip_cleaning: false             # Default behavior
```

**Automatic Cleaning (when skip_cleaning=false):**
- Trims leading/trailing whitespace
- Normalizes multiple spaces to single space
- Removes special whitespace characters

**Example:**
```
Input:  "  Contract    Name   with    Spaces  "
Output: "Contract Name with Spaces"
```

### 3. Case Transformations

#### All Case Options
```yaml
company_name:
  type: string
  lowercase: true                  # "ACME CORP" → "acme corp"

status:
  type: string
  uppercase: true                  # "active" → "ACTIVE"

product_name:
  type: string
  title_case: true                 # "crude oil" → "Crude Oil"

description:
  type: string
  capitalize: true                 # "HELLO WORLD" → "Hello world"
```

**Transformation Details:**
- **lowercase**: Converts entire string to lowercase
- **uppercase**: Converts entire string to uppercase
- **title_case**: Capitalizes first letter of each word
- **capitalize**: Capitalizes only first letter of entire string

### 4. Corrections Mapping

#### Value Replacement
```yaml
status:
  type: string
  corrections:
    "active": "Active"
    "inactive": "Inactive"
    "pending": "Pending"
    "cancelled": "Cancelled"
    "expired": "Expired"
```

**Use Cases:**
- Standardizing inconsistent values
- Fixing typos in source data
- Mapping legacy values to new standards

### 5. Date Format Conversions

#### Custom Date Formats
```yaml
effective_date:
  type: date
  input_format: "MM/dd/yyyy"       # Input: "03/15/2024"
  target_format: "yyyy-MM-dd"      # Output: "2024-03-15"

contract_start:
  type: date
  input_format: "dd-MMM-yyyy"      # Input: "15-Mar-2024"
  target_format: "yyyy-MM-dd"      # Output: "2024-03-15"
```

#### Timestamp Formats
```yaml
created_timestamp:
  type: timestamp
  input_format: "yyyy-MM-dd HH:mm:ss"
  target_format: "yyyy-MM-dd'T'HH:mm:ss'Z'"

updated_at:
  type: timestamp
  input_format: "MM/dd/yyyy HH:mm:ss"
  target_format: "yyyy-MM-dd HH:mm:ss"
```

**Common Format Patterns:**
- `yyyy`: 4-digit year (2024)
- `MM`: 2-digit month (03)
- `dd`: 2-digit day (15)
- `HH`: 24-hour format (14)
- `mm`: Minutes (30)
- `ss`: Seconds (25)
- `MMM`: 3-letter month (Mar)

### 6. Schema Evolution Support

#### Automatic Schema Alignment
The framework automatically handles:
- **Missing columns**: Adds with null/default values
- **Type mismatches**: Casts to target schema types
- **Struct incompatibilities**: Converts to JSON strings when needed
- **Extra fields**: Preserves or drops based on configuration

#### Struct to String Conversion
```yaml
# When target schema expects string but source has struct
pipeline_locations:
  type: struct
  source_path: "locations"
```

**Automatic Handling:**
- If target table has `pipeline_locations` as STRING
- Framework converts struct to JSON string automatically
- Preserves data integrity during schema evolution

## Complete Configuration Example

### Full Pipeline Configuration
```yaml
source: contract
source_type: dynamodb
source_table_name: contract-data-metadata-tst
source_incremental_col: updated_timestamp

stage:
  database_name: aws_samples_udm_raw_tst
  target_table_name: contract_data_metadata
  load_method: append

curation:
  - name: contract_data_metadata
    database_name: aws_samples_udm_curated_tst
    target_table_name: contract_data_metadata
    filter_source_data: "status = 'ACTIVE'"
    load_method: upsert
    primary_keys:
      - contract_uuid
      - documents_hash
    columns:
      - contract_uuid
      - contract_name
      - operator_name
      - status
      - base_rate_value

defaults:
  string:
    missing_value_default: ""
  integer:
    missing_value_default: 0
  float:
    missing_value_default: 0.0
  double:
    missing_value_default: 0.0
  date:
    date_overrides:
      "no expiration date": null
      "exp_date_not_available": null
      "TBD": null
  timestamp:
    date_overrides:
      "no expiration date": null
  fee_rate:
    invalid_values:
      "VAR/TBD": null
      "N/A": null
      "-": null
  value_normalization:
    missing_value_default: "Unknown"
    business_line:
      "gathering": "Gathering and Processing"
      "processing": "Gathering and Processing"
    product:
      "crude oil": "Crude Oil"
      "refined products": "Refined Products"
  struct:
    missing_value_default: null
  array_explode:
    missing_value_default: null

columns:
  contract_uuid:
    type: string
  
  contract_name:
    type: string
    skip_cleaning: false
  
  operator_name:
    type: string
    source_path: "attribute_summary.operator_name.final_value.value"
  
  status:
    type: string
    corrections:
      "active": "Active"
      "inactive": "Inactive"
  
  base_rate_value:
    type: float
    source_path:
      - "attribute_summary.rates.baserate.value.double"
      - "attribute_summary.rates.baserate.value.long"
    missing_value_default: 0.0
  
  contract_type_business_line:
    type: value_normalization
    normalization_key: business_line
    source_path: "attribute_summary.contract_type.final_value.value"
  
  pipeline_locations:
    type: struct
    source_path: "attribute_summary.pipeline_locations.final_value.pipeline_locations"
  
  effective_date:
    type: date
    source_path: "attribute_summary.effective_date.final_value.value"
    input_format: "MM/dd/yyyy"
    target_format: "yyyy-MM-dd"
```

## Transformation Processing Order

1. **Source Path Extraction**: Extract from nested structures
2. **Type-Specific Transformations**: Apply based on field type
3. **Data Quality**: Handle missing/invalid values
4. **Case Transformations**: Apply case conversions
5. **Corrections**: Apply value mappings
6. **String Cleaning**: Trim and normalize (unless skipped)
7. **Type Casting**: Final type conversion
8. **Schema Alignment**: Match target table schema

## Supported Data Types Summary

| Type | Description | Key Features |
|------|-------------|--------------|
| `string` | Text data | Cleaning, case conversion, corrections |
| `integer` | Whole numbers | Type casting, defaults |
| `float` | Decimal numbers | Type casting, defaults |
| `double` | High-precision decimals | Type casting, defaults |
| `boolean` | True/false values | Configurable true/false mappings, case-insensitive |
| `date` | Date values | Format conversion, overrides |
| `timestamp` | Date with time | Format conversion, overrides |
| `fee_rate` | Financial rates | Invalid value cleaning, percentage conversion |
| `value_normalization` | Categorical mapping | Multi-key normalization, defaults |
| `struct` | Complex nested objects | Preservation, JSON conversion |
| `array_explode` | Array to rows | Multi-row expansion, nested extraction |

## Usage Examples

### Example 1: Simple String Transformation
```yaml
columns:
  company_name:
    type: string
    source_path: "company.name"
    lowercase: true
    missing_value_default: "Unknown Company"
```

### Example 2: Numeric with Fallback
```yaml
columns:
  capacity:
    type: float
    source_path:
      - "capacity.value.double"
      - "capacity.value.float"
      - "capacity.value"
    missing_value_default: 0.0
```

### Example 3: Date with Format
```yaml
columns:
  contract_date:
    type: date
    source_path: "dates.contract_start"
    input_format: "MM/dd/yyyy"
    target_format: "yyyy-MM-dd"
    date_overrides:
      "TBD": null
      "N/A": null
```

### Example 4: Boolean Conversion
```yaml
columns:
  is_active:
    type: boolean
    true_values: ["yes", "y", "true", "1", "active"]
    false_values: ["no", "n", "false", "0", "inactive"]
    source_path: "contract.status"
  
  has_incentive_fee:
    type: boolean
    true_values: ["yes", "y", "true", "1"]
    false_values: ["no", "n", "false", "0", "N/A"]
    source_path: "attribute_summary.is_there_gathering_incentive_fee.final_value.value"
```

### Example 5: Value Normalization
```yaml
defaults:
  value_normalization:
    status:
      "A": "Active"
      "I": "Inactive"
      "P": "Pending"

columns:
  contract_status:
    type: value_normalization
    normalization_key: status
    source_path: "status_code"
    missing_value_default: "Unknown"
```

### Example 6: Array Explode
```yaml
columns:
  fees:
    type: array_explode
    source_path: "contract.fee_schedule"
    explode_columns:
      fee_type:
        source_field: "type"
        type: string
      fee_amount:
        source_field: "amount"
        type: float
      fee_frequency:
        source_field: "frequency"
        type: string
        missing_value_default: "Monthly"
```

## Best Practices

### 1. Configuration Organization
- Group related transformations together
- Use consistent naming conventions
- Document complex transformations with comments
- Keep defaults section comprehensive

### 2. Performance Optimization
- Minimize array explode operations (creates multiple rows)
- Use struct type for complex nested data when possible
- Avoid excessive nested path extractions
- Leverage column selection to reduce data volume

### 3. Data Quality
- Always define missing_value_default for critical fields
- Use corrections mapping for known data issues
- Implement invalid value cleaning for user input fields
- Test transformations with sample data first

### 4. Schema Management
- Define clear primary keys for upsert operations
- Use filter_source_data to exclude invalid records
- Plan for schema evolution with struct types
- Document expected data types in configuration

### 5. Debugging
- Start with simple transformations and add complexity
- Test each transformation type independently
- Use CloudWatch logs to trace transformation issues
- Validate output data against expectations

## Troubleshooting

### Common Issues

#### Issue 1: Column Not Found
**Symptom**: "Column X not found" error
**Solution**: 
- Verify source_path exists in source data
- Check for typos in nested path
- Use multiple source_path for fallback

#### Issue 2: Type Conversion Failure
**Symptom**: Null values after transformation
**Solution**:
- Check input data format matches expected type
- Add invalid_values mapping for fee_rate type
- Use corrections to fix malformed values first

#### Issue 3: Array Explode Creates Too Many Rows
**Symptom**: Unexpected row count increase
**Solution**:
- Verify array data is correct
- Check for nested arrays (not supported)
- Consider filtering before explode

#### Issue 4: Date Parsing Fails
**Symptom**: Dates become null
**Solution**:
- Verify input_format matches source data
- Add date_overrides for invalid date strings
- Check for timezone issues in timestamps

#### Issue 5: Struct Conversion Issues
**Symptom**: Struct becomes string unexpectedly
**Solution**:
- Check target table schema type
- Review schema evolution logs
- Verify struct fields match target schema

### Debugging Tips

1. **Enable Detailed Logging**:
   - Check Glue job logs in CloudWatch
   - Look for transformation warnings
   - Review schema alignment messages

2. **Test with Sample Data**:
   - Use small dataset for testing
   - Validate each transformation step
   - Compare input vs output

3. **Incremental Development**:
   - Add one transformation at a time
   - Test after each addition
   - Document working configurations

4. **Schema Validation**:
   - Check source data schema
   - Verify target table schema
   - Review type compatibility

## Performance Considerations

### Optimization Strategies

1. **Minimize Transformations**:
   - Only transform fields that need it
   - Use skip_cleaning when appropriate
   - Avoid redundant type conversions

2. **Efficient Array Handling**:
   - Filter data before array explode
   - Limit exploded columns to necessary fields
   - Consider denormalization alternatives

3. **Partitioning**:
   - Use appropriate partition columns
   - Balance partition size (not too small/large)
   - Align with query patterns

4. **Resource Allocation**:
   - Adjust Glue worker count for data volume
   - Use appropriate worker type (G.1X, G.2X)
   - Monitor job metrics for bottlenecks

### Performance Metrics

- **Stage Job**: Typically 2-5 minutes for 10K rows
- **Curated Job**: 3-10 minutes depending on transformations
- **Array Explode**: Adds 20-50% processing time
- **Struct Conversions**: Minimal overhead (<5%)

## Framework Architecture

### Module Structure
```
utils/
├── transformations/
│   ├── core_transformations.py      # String, numeric, type conversions
│   ├── array_transformations.py     # Array explode logic
│   ├── business_transformations.py  # Value normalization
│   ├── date_transformations.py      # Date/timestamp handling
│   ├── nested_data.py               # Struct and nested extraction
│   ├── data_quality.py              # Missing/invalid value handling
│   ├── extraction_utils.py          # Common extraction utilities
│   └── schema_evolution.py          # Schema alignment and evolution
├── transformation_engine.py         # Main orchestration engine
├── config_validator.py              # Configuration validation
└── job_utils.py                     # Utility functions
```

### Processing Flow
```
1. Load Configuration
   ↓
2. Read Source Data
   ↓
3. Apply Transformations (transformation_engine.py)
   ├── Extract nested fields
   ├── Apply type-specific transformations
   ├── Handle data quality
   └── Align with target schema
   ↓
4. Write to Target
   ↓
5. Audit and Log
```

## Advanced Topics

### Custom Transformation Extensions

The framework is extensible. To add custom transformations:

1. Create new transformation class in `transformations/`
2. Implement transformation methods
3. Register in `transformation_engine.py`
4. Update configuration schema
5. Add tests and documentation

### Multi-Target Curation

Write different column sets to multiple tables:

```yaml
curation:
  - name: contracts_main
    target_table_name: contracts_main
    columns: [contract_uuid, contract_name, operator_name]
    load_method: upsert
    primary_keys: [contract_uuid]
  
  - name: contracts_fees
    target_table_name: contracts_fees
    columns: [contract_uuid, fee_name, fee_amount]
    load_method: append
```

### Incremental Processing

Support for incremental loads:

```yaml
source_incremental_col: updated_timestamp

stage:
  load_method: append              # Append new records only

curation:
  - name: curated_table
    load_method: upsert            # Update existing, insert new
    primary_keys: [id, version]
```

## Support and Contribution

### Getting Help
- Review CloudWatch logs for detailed error messages
- Check configuration examples in `config/pipeline/`
- Consult transformation test cases
- Contact UDM workstream leads

### Contributing
- Follow existing code patterns
- Add unit tests for new transformations
- Update documentation
- Test with multiple data scenarios

## Version History

- **v1.0**: Initial framework with core transformations
- **v1.1**: Added array explode support
- **v1.2**: Enhanced schema evolution handling
- **v1.3**: Added multi-source path fallback
- **v1.4**: Improved struct type handling
- **v1.5**: Added boolean data type support with configurable true/false value mappings
- **Current**: Comprehensive transformation framework with full type support including boolean conversions

## Boolean Type References

For value_normalization.business_line references
  missing_value_default: "Unknown"

product_type:
  type: value_normalization
  normalization_key: product         # References defaults.value_normalization.product
```

**Input/Output Examples:**
```
# Business line normalization
Input:  "gathering"
Output: "Gathering and Processing"   # Mapped from defaults.value_normalization.business_line

Input:  "processing"
Output: "Gathering and Processing"   # Same mapping

Input:  "unknown_business"
Output: "unknown_business"           # No mapping found, unchanged

Input:  null
Output: "Unknown"                    # Default applied

# Product normalization
Input:  "crude oil"
Output: "Crude Oil"                  # Mapped from defaults.value_normalization.product

Input:  "refined products"
Output: "Refined Products"           # Mapped value
```

### 6. Boolean Transformations

#### Basic Boolean Conversion
```yaml
is_active:
  type: boolean
  true_values: ["yes", "y", "true", "1"]
  false_values: ["no", "n", "false", "0"]
  source_path: "contract.active_status"
```

**Input/Output Examples:**
```
# String to boolean conversion
Input:  "yes"
Output: true

Input:  "no"
Output: false

Input:  "1"
Output: true

Input:  "0"
Output: false

# Case-insensitive matching
Input:  "YES"
Output: true

Input:  "No"
Output: false

# Numeric to boolean conversion
Input:  1
Output: true

Input:  0
Output: false

Input:  1.0
Output: true

Input:  0.0
Output: false

# Unmapped values
Input:  "maybe"
Output: null                         # Not in true_values or false_values

Input:  null
Output: null                         # Null remains null
```

#### Boolean with N/A Handling
```yaml
is_processing_included:
  type: boolean
  true_values: ["yes", "y", "true", "1"]
  false_values: ["no", "n", "false", "0", "N/A"]
  source_path: "attribute_summary.is_processing_included.final_value.value"
```

**Input/Output Examples:**
```
Input:  "yes"
Output: true

Input:  "N/A"
Output: false                        # N/A treated as false

Input:  "no"
Output: false
```

#### Boolean from Numeric Fields
```yaml
is_pipeline_loss_allowance_included:
  type: boolean
  true_values: ["1", "1.0"]
  false_values: ["0", "0.0"]
  source_path:
    - "attribute_summary.is_pipeline_loss_allowance_included.final_value.pipeline_loss_allowance_in_percentage.double"
    - "attribute_summary.is_pipeline_loss_allowance_included.final_value.pipeline_loss_allowance_in_percentage.long"
```

**Input/Output Examples:**
```
# From double field
Input:  1.0
Output: true

Input:  0.0
Output: false

# From long field
Input:  1
Output: true

Input:  0
Output: false

# String representation of numbers
Input:  "1.0"
Output: true

Input:  "0.0"
Output: false
```

**Configuration Options:**
- `true_values`: List of values that map to true (case-insensitive for strings)
- `false_values`: List of values that map to false (case-insensitive for strings)
- `source_path`: Single path or array of paths for fallback logic
- Values not in either list return null

**Use Cases:**
- Converting yes/no fields to boolean
- Standardizing flag fields from various formats
- Handling numeric boolean representations (0/1)
- Converting string boolean values to actual booleans
- Treating special values like "N/A" as false

### 7. Array Exploding
```yaml
fees_data:
  type: array_explode
  source_path: "contract.fees"
  explode_columns:
    fee_name:
      source_field: "name"
      type: string
    fee_amount:
      source_field: "amount"
      type: float
    fee_unit:
      source_field: "unit"
      type: string
```

**Input/Output Example:**
```
# Input (single row)
contract_uuid: "C001"
contract.fees: [
  {"name": "Storage", "amount": "50.0", "unit": "USD"},
  {"name": "Transport", "amount": "75.5", "unit": "USD"},
  {"name": "Handling", "amount": "25.0", "unit": "USD"}
]

# Output (multiple rows)
Row 1: contract_uuid="C001", fee_name="Storage", fee_amount=50.0, fee_unit="USD"
Row 2: contract_uuid="C001", fee_name="Transport", fee_amount=75.5, fee_unit="USD"
Row 3: contract_uuid="C001", fee_name="Handling", fee_amount=25.0, fee_unit="USD"

# Empty array handling
Input:  contract.fees: []
Output: No rows created (or single row with null values if configured)

# Missing array handling
Input:  contract.fees: null
Output: No rows created
```

## Usage Examples

### Complete Multi-Target Configuration
```yaml
defaults:
  string:
    missing_value_default: "Unknown"
  float:
    missing_value_default: 0.0
  value_normalization:
    business_line:
      "gathering": "Gathering and Processing"
      "processing": "Gathering and Processing"
    region:
      "tx": "Texas"
      "la": "Louisiana"

curation:
  - name: contracts_main
    target_table_name: contracts_main
    columns: [contract_uuid, contract_name, operator_name, business_type]
  
  - name: contracts_fees
    target_table_name: contracts_fees
    columns: [contract_uuid, fee_name, fee_amount, fee_unit]

columns:
  contract_uuid:
    type: string
    source_path: "contract.id"
  
  contract_name:
    type: string
    source_path: "contract.name"
    title_case: true
  
  operator_name:
    type: string
    source_path: "contract.operator"
    uppercase: true
  
  business_type:
    type: value_normalization
    normalization_key: business_line
    source_path: "contract.business_line"
  
  fees_data:
    type: array_explode
    source_path: "contract.fees"
    explode_columns:
      fee_name:
        source_field: "name"
        type: string
      fee_amount:
        source_field: "amount"
        type: float
      fee_unit:
        source_field: "unit"
        type: string
```

### Nested Data Flattening
```yaml
# Single path flattening
operator_name:
  type: string
  source_path: "contract.operator.name"

# Multiple path fallback (uses coalesce)
contact_email:
  type: string
  source_path: 
    - "contract.primary_contact.email"
    - "contract.operator.email"
    - "contract.default_email"
```

**Input/Output Examples:**
```
# Single path flattening
Input JSON:
{
  "contract": {
    "operator": {
      "name": "Acme Corp"
    }
  }
}
Output: operator_name = "Acme Corp"

# Multiple path fallback (coalesce)
Input JSON:
{
  "contract": {
    "primary_contact": {"email": null},
    "operator": {"email": "ops@example.com"},
    "default_email": "default@company.com"
  }
}
Output: contact_email = "ops@example.com"    # First non-null value

# All paths missing
Input JSON:
{
  "contract": {
    "primary_contact": {"email": null},
    "operator": {"email": null},
    "default_email": null
  }
}
Output: contact_email = null               # All paths null
```

### Complex Date Handling
```yaml
contract_start:
  type: date
  source_path: "contract.dates.effective"
  input_format: "MM/dd/yyyy"
  target_format: "yyyy-MM-dd"
  date_overrides:
    "TBD": ""
    "Not Available": ""

contract_end:
  type: timestamp
  source_path: "contract.dates.expiration"
  input_format: "yyyy-MM-dd HH:mm:ss"
```

## Best Practices

### Configuration Design
1. **Use appropriate data types** - Match source data characteristics
2. **Define comprehensive defaults** - Handle missing values gracefully
3. **Organize value mappings** - Use categorized normalization keys
4. **Plan primary keys carefully** - Include array-specific identifiers for exploded data
5. **Test with sample data** - Validate transformations before deployment

### Multi-Target Best Practices
1. **Design atomic operations** - Ensure all targets succeed or fail together
2. **Optimize column selection** - Only include needed columns per target
3. **Monitor resource usage** - Multiple targets increase processing time
4. **Validate data relationships** - Ensure referential integrity
5. **Plan recovery procedures** - Handle partial failure scenarios

### Array Exploding Best Practices
1. **Check array existence** - Handle optional array fields gracefully
2. **Validate array structure** - Ensure consistent element schema
3. **Consider performance impact** - Large arrays create many rows
4. **Plan storage requirements** - Exploded data requires more space
5. **Design appropriate indexes** - Optimize query performance

### Data Quality Best Practices
1. **Handle missing values** - Define appropriate defaults for each type
2. **Validate data ranges** - Check numeric and date boundaries
3. **Standardize formats** - Use consistent date and string formats
4. **Clean data early** - Apply cleaning before complex transformations
5. **Monitor data quality** - Track transformation success rates

## Troubleshooting

### Common Configuration Issues
```
"Config error: 'curation' must be an array of target tables"
→ Update curation to array format:
  curation:
    - name: target1
      target_table_name: table1
      columns: [col1, col2]

"No valid columns found for target_name"
→ Check column names in target.columns list match columns section

"Normalization key 'key_name' not found in defaults"
→ Add missing key to defaults.value_normalization section
```

### Array Exploding Issues
```
"Array column not found: column_name"
→ Verify source_path points to valid array field in source data

"Empty arrays creating no rows"
→ Check explode_empty_arrays setting in defaults
→ Consider if empty arrays should create rows with null values

"Memory exhaustion during exploding"
→ Process data in smaller batches
→ Monitor array sizes and row multiplication factor
```

### Data Type Issues
```
"Cannot convert 'value' to integer"
→ Check for non-numeric strings in source data
→ Add data cleaning or corrections mapping

"Date parsing failed for format"
→ Verify input_format matches source data format
→ Add date_overrides for invalid date strings

"Boolean conversion returns null"
→ Verify value exists in true_values or false_values lists
→ Check for case sensitivity issues (strings are case-insensitive)
→ Add missing values to appropriate list

"Column not found after transformation"
→ Ensure source_path exists in source data
→ Check for typos in nested path specifications
```

### Multi-Target Issues
```
"Column not found after transformation"
→ Ensure columns exist after global transformations
→ Check that array exploding creates expected columns

"Primary key conflicts in exploded data"
→ Include array-specific identifiers in primary keys
→ Consider adding row_number or array_index columns

"Partial target failures"
→ Check atomic processing requirements
→ Review error logs for specific target failures
```

### Debug Tips
1. **Enable verbose logging** - Set log level to DEBUG
2. **Use config validator** - Validate configuration before deployment
3. **Test with sample data** - Start with small datasets
4. **Monitor resource usage** - Track memory and CPU consumption
5. **Check intermediate results** - Inspect data at each transformation step

## Performance Considerations

### Array Exploding Performance
- **Memory usage** - Large arrays consume significant memory during exploding
- **Row multiplication** - Each array element creates new rows (1 → N relationship)
- **Processing time** - Exploding increases transformation duration significantly
- **Storage impact** - Exploded data requires more storage space
- **Join performance** - More rows impact downstream join operations

### Multi-Target Performance
- **Read optimization** - Source data read only once, shared across targets
- **Memory efficiency** - Transformations applied once, filtered per target
- **Write serialization** - Targets written sequentially for atomicity
- **Audit overhead** - Individual tracking and logging per target
- **Resource scaling** - Processing time scales with number of targets

### Data Type Performance
- **String operations** - Case conversions and regex operations are expensive
- **Date parsing** - Custom format parsing slower than standard formats
- **Nested flattening** - Deep nesting impacts performance
- **Type conversions** - Multiple type changes add overhead
- **Corrections mapping** - Large mapping dictionaries slow processing

### Optimization Strategies
1. **Filter early** - Remove unnecessary columns before transformation
2. **Batch processing** - Process data in manageable chunks
3. **Monitor resources** - Track memory and CPU usage patterns
4. **Partition appropriately** - Use effective partitioning strategies
5. **Cache transformations** - Reuse computed values across targets
6. **Optimize joins** - Use broadcast joins for small lookup tables
7. **Minimize exploding** - Only explode arrays when necessary
8. **Use appropriate data types** - Choose efficient Spark data types

## Configuration Validation

Use the config validator to ensure configuration correctness:

```python
from utils.config_validator import validate_config_structure

cfg = load_configs("path/to/config.yaml")
if validate_config_structure(cfg):
    print("✓ Configuration is valid")
else:
    print("✗ Configuration has issues")
```

### Validation Checks
- **Curation structure** - Ensures curation is an array of target definitions
- **Required fields** - Validates defaults and columns sections exist
- **Target definitions** - Checks target table configurations are complete
- **Column mappings** - Verifies column lists reference defined columns
- **Type consistency** - Validates transformation types are supported
- **Array configurations** - Checks explode_columns structure and field mappings
- **Normalization keys** - Ensures referenced keys exist in defaults
- **Path validation** - Checks source_path syntax for nested fields

## Module Structure

```
utils/
├── transformations/
│   ├── core_transformations.py      # String, numeric, type conversions
│   ├── date_transformations.py      # Date and timestamp handling
│   ├── business_transformations.py  # Value normalization
│   ├── array_transformations.py     # Array exploding functionality
│   ├── data_quality.py             # Missing values, data cleaning
│   └── nested_data.py              # JSON flattening operations
├── transformation_engine.py         # Main transformation orchestrator
├── job_utils.py                    # Utility functions and helpers
├── config_validator.py             # Configuration validation
└── README.md                       # This documentation
```

## Future Enhancements

### Planned Features
- **Conditional transformations** - Apply transformations based on field conditions
- **Cross-column validations** - Validate relationships between multiple fields
- **Dynamic mappings** - Load normalization mappings from external sources
- **Parallel processing** - Multi-threaded transformation execution
- **Real-time processing** - Stream processing capabilities for live data

### Potential Improvements
- **Custom transformation functions** - User-defined transformation logic
- **Advanced data quality** - Statistical validation and anomaly detection
- **Schema evolution** - Automatic schema change handling and migration
- **External integrations** - Data validation services and quality tools
- **Performance benchmarks** - Optimization testing and monitoring frameworks
- **Visual configuration** - GUI-based configuration builder and validator

## Support and Resources

- **Module Documentation** - Individual class and method documentation in source code
- **Source Code Comments** - Inline documentation and usage examples
- **Configuration Examples** - Sample configurations in `/config/` directory
- **Test Cases** - Unit tests demonstrating usage patterns and edge cases
- **Performance Guides** - Optimization best practices and benchmarking results

For questions, issues, or contributions:
1. **Review documentation** - Check this README and source code comments
2. **Validate configuration** - Use config validator for syntax and structure issues
3. **Test incrementally** - Start with simple transformations and build complexity
4. **Monitor logs** - Enable debug logging for detailed troubleshooting information
5. **Study examples** - Review working configuration patterns and test cases