"""
schema_loader.py

Loads table schemas from YAML definition files and exposes them as
typed SchemaField / TableSchema objects.

Used by:
  - governance.py            → column names, dtypes, nullable flags, PII tags
  - create_iceberg_tables.py → table name, iceberg_location, column DDL types
  - ingest_to_minio.py       → source_file, iceberg_location

Directory expected:
  schemas/
    orders_data.yml
    customers_data.yml
    products_data.yml
    feedback_data.yml
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import Optional

#dtype mapping tables

# Maps YAML dtype → Python type  (used for casting validation)
DTYPE_TO_PYTHON = {
    "string":  str,
    "integer": int,
    "decimal": float,
    "boolean": bool,
    "date":    str,   # kept as string; governance layer parses with dateutil
}

# Maps YAML dtype → Iceberg / Trino SQL type  (used by create_iceberg_tables.py)
DTYPE_TO_ICEBERG = {
    "string":  "VARCHAR",
    "integer": "INTEGER",
    "decimal": "DOUBLE",
    "boolean": "BOOLEAN",
    "date":    "DATE",
}

# Maps YAML dtype → Pandas read dtype  (used when loading CSVs for validation)
DTYPE_TO_PANDAS = {
    "string":  "object",
    "integer": "Int64",    # nullable integer
    "decimal": "float64",
    "boolean": "boolean",  # nullable boolean
    "date":    "object",   # parse separately with pd.to_datetime
}

VALID_DTYPES = set(DTYPE_TO_PYTHON.keys())


# Data classes

@dataclass
class SchemaField:
    """Represents a single column definition from a YAML schema file."""
    name:        str
    dtype:       str
    nullable:    bool
    pii:         bool
    description: str
    pii_type:    Optional[str] = None   # "name" | "email" | "phone" | None

    @property
    def python_type(self):
        return DTYPE_TO_PYTHON[self.dtype]

    @property
    def iceberg_type(self):
        return DTYPE_TO_ICEBERG[self.dtype]

    @property
    def pandas_dtype(self):
        return DTYPE_TO_PANDAS[self.dtype]

    @property
    def is_date(self) -> bool:
        return self.dtype == "date"


@dataclass
class TableSchema:
    """Represents the full schema definition for one table."""
    table_name:       str
    description:      str
    primary_key:      str
    source_file:      str
    iceberg_location: str
    columns:          list[SchemaField] = field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        """Returns ordered list of all column names."""
        return [c.name for c in self.columns]

    @property
    def non_nullable_columns(self) -> list[SchemaField]:
        """Returns columns that must NOT contain nulls."""
        return [c for c in self.columns if not c.nullable]

    @property
    def date_columns(self) -> list[SchemaField]:
        """Returns columns that should be parsed as dates."""
        return [c for c in self.columns if c.is_date]

    @property
    def pii_columns(self) -> list[SchemaField]:
        """Returns columns flagged as PII — used by governance masking layer."""
        return [c for c in self.columns if c.pii]

    @property
    def pandas_dtype_map(self) -> dict:
        """
        Returns a dtype dict suitable for pd.read_csv(dtype=...).
        Date columns are excluded — pass them via parse_dates instead.
        """
        return {
            c.name: c.pandas_dtype
            for c in self.columns
            if not c.is_date
        }

    def get_column(self, name: str) -> Optional[SchemaField]:
        """Look up a column definition by name. Returns None if not found."""
        return next((c for c in self.columns if c.name == name), None)

    def iceberg_ddl_columns(self) -> list[str]:
        """
        Returns Iceberg-compatible column DDL strings.
        Example: '"order_id" VARCHAR NOT NULL'
        """
        ddl_parts = []
        for col in self.columns:
            null_clause = "" if col.nullable else " NOT NULL"
            ddl_parts.append(f'"{col.name}" {col.iceberg_type}{null_clause}')
        return ddl_parts


#Internal helpers

def _validate_yaml_structure(raw: dict, filepath: str) -> None:
    required_top_level = {
        "table_name", "description", "primary_key",
        "source_file", "iceberg_location", "columns"
    }
    missing = required_top_level - set(raw.keys())
    if missing:
        raise ValueError(
            f"[schema_loader] '{filepath}' is missing required top-level keys: {missing}"
        )
    if not isinstance(raw["columns"], list) or len(raw["columns"]) == 0:
        raise ValueError(
            f"[schema_loader] '{filepath}' must have at least one column defined."
        )


def _parse_column(col_dict: dict, table_name: str) -> SchemaField:
    required_col_keys = {"name", "dtype", "nullable", "pii", "description"}
    missing = required_col_keys - set(col_dict.keys())
    if missing:
        raise ValueError(
            f"[schema_loader] Column in table '{table_name}' "
            f"is missing required keys: {missing}. Got: {col_dict}"
        )
    dtype = col_dict["dtype"]
    if dtype not in VALID_DTYPES:
        raise ValueError(
            f"[schema_loader] Column '{col_dict['name']}' in table '{table_name}' "
            f"has unsupported dtype '{dtype}'. Valid options: {sorted(VALID_DTYPES)}"
        )
    return SchemaField(
        name        = col_dict["name"],
        dtype       = dtype,
        nullable    = bool(col_dict["nullable"]),
        pii         = bool(col_dict["pii"]),
        description = col_dict["description"],
        pii_type    = col_dict.get("pii_type"),
    )


def _load_single_schema(filepath: str) -> TableSchema:
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"[schema_loader] Schema file not found: '{filepath}'"
        )
    with open(filepath, "r") as f:
        raw = yaml.safe_load(f)

    _validate_yaml_structure(raw, filepath)

    columns   = [_parse_column(col, raw["table_name"]) for col in raw["columns"]]
    col_names = [c.name for c in columns]

    if raw["primary_key"] not in col_names:
        raise ValueError(
            f"[schema_loader] primary_key '{raw['primary_key']}' in '{filepath}' "
            f"does not match any column. Available: {col_names}"
        )

    return TableSchema(
        table_name       = raw["table_name"],
        description      = raw["description"],
        primary_key      = raw["primary_key"],
        source_file      = raw["source_file"],
        iceberg_location = raw["iceberg_location"],
        columns          = columns,
    )


# Public API

# Canonical mapping: logical key → YAML filename
# Keys are consistent: all use the table_name as the key
SCHEMA_FILES = {
    "orders_data":    "orders_data.yml",
    "customers_data": "customers_data.yml",   # fixed: was "customers" → inconsistent
    "products_data":  "products_data.yml",
    "feedback_data":  "feedback_data.yml",
}

def load_schema(table_key: str, schemas_dir: str = "schemas") -> TableSchema:
    """
    Load and return the TableSchema for a single table.

    Parameters
    ----------
    table_key   : str — one of "orders_data", "customers_data", "products_data", "feedback_data"
    schemas_dir : str — path to the folder containing the YAML files (default: "schemas")
    """
    if table_key not in SCHEMA_FILES:
        raise KeyError(
            f"[schema_loader] Unknown table key '{table_key}'. "
            f"Valid keys: {list(SCHEMA_FILES.keys())}"
        )
    filepath = os.path.join(schemas_dir, SCHEMA_FILES[table_key])
    return _load_single_schema(filepath)


def load_all_schemas(schemas_dir: str = "schemas") -> dict[str, TableSchema]:
    """
    Load and return schemas for all four tables.

    Returns dict mapping table_key → TableSchema
    """
    return {
        key: load_schema(key, schemas_dir)
        for key in SCHEMA_FILES
    }


# Self-test

if __name__ == "__main__":
    print("Loading all schemas...\n")
    schemas = load_all_schemas()

    for key, schema in schemas.items():
        print(f"{'='*60}")
        print(f"Table   : {schema.table_name}")
        print(f"PK      : {schema.primary_key}")
        print(f"Source  : {schema.source_file}")
        print(f"Location: {schema.iceberg_location}")
        print(f"Columns ({len(schema.columns)}):")
        for col in schema.columns:
            pii_tag  = f"  [PII:{col.pii_type}]" if col.pii else ""
            null_tag = "nullable" if col.nullable else "NOT NULL"
            print(f"  {col.name:<25} {col.dtype:<10} {null_tag:<10}{pii_tag}")
        print(f"\nNon-nullable : {[c.name for c in schema.non_nullable_columns]}")
        print(f"Date cols    : {[c.name for c in schema.date_columns]}")
        print(f"PII cols     : {[c.name for c in schema.pii_columns]}")
        print(f"\nIceberg DDL preview:")
        for ddl in schema.iceberg_ddl_columns():
            print(f"  {ddl}")
        print()