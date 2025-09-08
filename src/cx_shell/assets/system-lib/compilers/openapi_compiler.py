#!/usr/bin/env python3
import json
import re
import sys
from typing import Any, Dict, List

import yaml

# --- The Custom Contextually Pydantic Model & Blueprint Generator ---

# Mapping from OpenAPI/JSON Schema types to Python types
TYPE_MAP = {
    "string": "str",
    "number": "float",
    "integer": "int",
    "boolean": "bool",
    "array": "List",
    "object": "Dict[str, Any]",
}

# Mapping for common string formats that require specific Python types
FORMAT_MAP = {"date-time": "datetime", "date": "date", "uuid": "UUID"}

# Python keywords that cannot be used as field names
PYTHON_KEYWORDS = {
    "in",
    "from",
    "for",
    "is",
    "while",
    "class",
    "def",
    "return",
    "True",
    "False",
    "None",
}


def log_to_stderr(message: str):
    """Writes a log message to stderr, prefixed for clarity."""
    print(f"contextual_compiler: {message}", file=sys.stderr)


def to_pascal_case(snake_case_str: str) -> str:
    """Converts a snake_case string to PascalCase for class names."""
    return "".join(word.capitalize() for word in snake_case_str.split("_"))


def safe_snake_case(name: str) -> str:
    """
    Converts any string to a valid Python identifier in snake_case.
    e.g., "get-multiple-artists" -> "get_multiple_artists"
    e.g., "GetUsersProfile" -> "get_users_profile"
    e.g., "petId" -> "pet_id"
    """
    if not name:
        return "_unknown"
    # Replace hyphens, dots, and spaces with underscores
    s1 = re.sub(r"[-\s\.]+", "_", name)
    # Insert underscores before uppercase letters (for camelCase/PascalCase conversion)
    s2 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s1)
    s3 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s2).lower()

    # Remove any characters that are not alphanumeric or underscore
    cleaned_name = re.sub(r"\W+", "", s3)

    # Ensure it doesn't start with a number
    if cleaned_name and cleaned_name[0].isdigit():
        cleaned_name = "_" + cleaned_name

    if cleaned_name in PYTHON_KEYWORDS:
        return f"{cleaned_name}_"
    return cleaned_name or "_unknown"


def _get_schemas(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts the reusable schema definitions from either Swagger 2.0 or OpenAPI 3.x."""
    return spec.get("components", {}).get("schemas", {}) or spec.get("definitions", {})


def _generate_data_models(schemas: Dict[str, Any]) -> List[str]:
    """
    Generates Pydantic models for complex object schemas and TypeAliases for
    primitive type schemas found in the OpenAPI specification.
    """
    code_lines = []
    if not schemas:
        return code_lines

    for schema_name, schema_def in schemas.items():
        schema_type = schema_def.get("type")

        # --- Case 1: The schema defines a complex object ---
        if schema_type == "object" and "properties" in schema_def:
            class_name = to_pascal_case(safe_snake_case(schema_name))
            required_fields = set(schema_def.get("required", []))

            code_lines.append(f"class {class_name}(BaseModel):")
            properties = schema_def.get("properties", {})
            if not properties:
                code_lines.append("    pass\n")
                continue

            fields = []
            for prop_name, prop_def in properties.items():
                field_name = safe_snake_case(prop_name)
                is_required = prop_name in required_fields
                python_type = "Any"

                if "$ref" in prop_def:
                    ref_name = prop_def["$ref"].split("/")[-1]
                    python_type = f'"{to_pascal_case(safe_snake_case(ref_name))}"'
                elif "type" in prop_def:
                    prop_type = prop_def["type"]
                    if prop_type == "array":
                        items_def = prop_def.get("items", {})
                        item_type = "Any"
                        if "$ref" in items_def:
                            ref_name = items_def["$ref"].split("/")[-1]
                            item_type = f'"{to_pascal_case(safe_snake_case(ref_name))}"'
                        elif "type" in items_def:
                            item_type = TYPE_MAP.get(items_def["type"], "Any")
                        python_type = f"List[{item_type}]"
                    else:
                        python_type = TYPE_MAP.get(prop_type, "Any")
                        if prop_def.get("format") in FORMAT_MAP:
                            python_type = FORMAT_MAP[prop_def["format"]]

                alias = prop_name if field_name != prop_name else None

                if is_required:
                    field_type_hint = python_type
                    if alias:
                        fields.append(
                            f'    {field_name}: {field_type_hint} = Field(alias="{alias}")'
                        )
                    else:
                        fields.append(f"    {field_name}: {field_type_hint}")
                else:  # Optional field
                    field_type_hint = f"Optional[{python_type}]"
                    field_args = [
                        "None"
                    ]  # Default value is always None for optional fields
                    if alias:
                        field_args.append(f'alias="{alias}"')
                    fields.append(
                        f"    {field_name}: {field_type_hint} = Field({', '.join(field_args)})"
                    )

            code_lines.extend(fields if fields else ["    pass"])
            code_lines.append("\n")

        # --- Case 2: The schema is an alias for a primitive type ---
        elif schema_type in TYPE_MAP:
            alias_name = to_pascal_case(safe_snake_case(schema_name))
            python_type = TYPE_MAP[schema_type]

            # Add the original description as a comment for context
            description = schema_def.get("description", "").replace("\n", " ").strip()
            if description:
                code_lines.append(f"# {alias_name}: {description}")

            # Use TypeAlias for clarity
            code_lines.append(f"{alias_name} = {python_type}")
            code_lines.append("\n")

        # --- Case 3: Skip other unhandled schema types ---
        else:
            log_to_stderr(
                f"Skipping unhandled schema type '{schema_type}' for definition: {schema_name}"
            )
            continue

    return code_lines


def _generate_parameter_models(spec: Dict[str, Any]) -> List[str]:
    """Generates Pydantic models for the parameters of each operation."""
    code_lines = []
    for path, path_item in spec.get("paths", {}).items():
        for method, operation in path_item.items():
            if "operationId" not in operation or not operation.get("parameters"):
                continue

            action_key = safe_snake_case(operation["operationId"])
            class_name = f"{to_pascal_case(action_key)}Parameters"
            fields = []

            for param in operation.get("parameters", []):
                if param.get("in") not in ["path", "query", "header"]:
                    continue

                param_name = param["name"]
                field_name = safe_snake_case(param_name)
                is_required = param.get("required", False)

                schema = param.get("schema", param)
                python_type = TYPE_MAP.get(schema.get("type"), "Any")
                field_type = (
                    python_type if is_required else f"Optional[{python_type}] = None"
                )

                alias = param_name if field_name != param_name else None
                if alias:
                    fields.append(
                        f'    {field_name}: {field_type} = Field(alias="{alias}")'
                    )
                else:
                    fields.append(f"    {field_name}: {field_type}")

            if fields:
                code_lines.append(f"class {class_name}(BaseModel):")
                code_lines.extend(fields)
                code_lines.append("\n")
    return code_lines


def generate_pydantic_code(spec: Dict[str, Any]) -> str:
    """Generates the complete schemas.py file content."""
    schemas = _get_schemas(spec)
    header = [
        "# Generated by the Contextually Blueprint Compiler",
        "from __future__ import annotations",
        "from typing import Any, Dict, List, Optional, TypeAlias",
        "from datetime import date, datetime",
        "from uuid import UUID",
        "from pydantic import BaseModel, Field",
        "\n",
    ]
    data_model_code = _generate_data_models(schemas)
    param_model_code = _generate_parameter_models(spec)
    return "\n".join(header + data_model_code + param_model_code)


def generate_ccl_blueprint(spec: Dict[str, Any]) -> str:
    """Generates the blueprint.cx.yaml content as a YAML string."""
    info = spec.get("info", {})

    if "servers" in spec and spec["servers"]:
        server_url = spec["servers"][0].get("url", "https://api.example.com")
    elif "host" in spec:
        server_url = f"{spec.get('schemes', ['https'])[0]}://{spec['host']}{spec.get('basePath', '')}"
    else:
        server_url = "https://api.example.com"

    blueprint = {
        "id": f"blueprint:{safe_snake_case(info.get('title', 'untitled'))}",
        "name": info.get("title", "Untitled API"),
        "version": info.get("version", "1.0.0"),
        "connector_provider_key": "rest-declarative",
        "supported_auth_methods": [
            {"type": "none", "display_name": "No Authentication", "fields": []}
        ],
    }

    action_templates = {}
    for path, path_item in spec.get("paths", {}).items():
        for method, operation in path_item.items():
            if (
                method.lower() not in ["get", "post", "put", "patch", "delete"]
                or "operationId" not in operation
            ):
                continue

            action_key = safe_snake_case(operation["operationId"])
            api_endpoint = path.replace("{", "{{ context.").replace("}", " }}")
            action = {"http_method": method.upper(), "api_endpoint": api_endpoint}

            if any(
                p.get("in") in ["path", "query", "header"]
                for p in operation.get("parameters", [])
            ):
                params_class_name = f"{to_pascal_case(action_key)}Parameters"
                action["parameters_model"] = f"schemas.{params_class_name}"

            body_schema_ref = None
            if "requestBody" in operation:
                try:
                    body_schema_ref = operation["requestBody"]["content"][
                        "application/json"
                    ]["schema"]["$ref"]
                except KeyError:
                    pass
            else:
                for param in operation.get("parameters", []):
                    if param.get("in") == "body" and "$ref" in param.get("schema", {}):
                        body_schema_ref = param["schema"]["$ref"]
                        break

            if body_schema_ref:
                body_schema_name = to_pascal_case(
                    safe_snake_case(body_schema_ref.split("/")[-1])
                )
                action["payload_constructor"] = {
                    "_model": f"schemas.{body_schema_name}"
                }

            action_templates[action_key] = action

    blueprint["browse_config"] = {
        "base_url_template": server_url,
        "action_templates": action_templates,
    }
    return yaml.dump(blueprint, sort_keys=False, indent=2, width=120)


if __name__ == "__main__":
    try:
        log_to_stderr("Contextually custom compiler started.")
        spec_content = yaml.safe_load(sys.stdin.read())

        log_to_stderr("Generating Pydantic models with custom generator...")
        schemas_py = generate_pydantic_code(spec_content)

        log_to_stderr("Generating Contextually blueprint...")
        blueprint_yaml = generate_ccl_blueprint(spec_content)

        output = {"blueprint_yaml": blueprint_yaml, "schemas_py": schemas_py}
        print(json.dumps(output, indent=2))

        log_to_stderr("Compiler finished successfully.")
        sys.exit(0)
    except Exception as e:
        log_to_stderr(f"FATAL ERROR: {type(e).__name__} - {e}")
        import traceback

        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
