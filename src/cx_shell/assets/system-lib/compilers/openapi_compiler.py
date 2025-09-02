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
FORMAT_MAP = {
    "date-time": "datetime",
    "date": "date",
    "uuid": "UUID",
}

# Python keywords that cannot be used as field names
PYTHON_KEYWORDS = {"in", "from", "for", "is", "while", "class", "def", "return"}


def log_to_stderr(message: str):
    """Writes a log message to stderr, prefixed for clarity."""
    print(f"contextual_compiler: {message}", file=sys.stderr)


def safe_name(name: str) -> str:
    """Ensures a name is a valid Python identifier by cleaning and suffixing keywords."""
    # Replace slashes and other invalid characters with underscores
    cleaned_name = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    if cleaned_name in PYTHON_KEYWORDS:
        return f"{cleaned_name}_"
    return cleaned_name


def _get_schemas(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts the reusable schema definitions from either Swagger 2.0 or OpenAPI 3.x."""
    is_swagger_v2 = "swagger" in spec and spec["swagger"].startswith("2.")
    if is_swagger_v2:
        return spec.get("definitions", {})
    return spec.get("components", {}).get("schemas", {})


def _generate_data_models(schemas: Dict[str, Any]) -> List[str]:
    """Generates Pydantic models for data objects (e.g., Pet, User)."""
    code = []
    if not schemas:
        return code

    for schema_name, schema_def in schemas.items():
        class_name = safe_name(schema_name.strip())
        required_fields = set(schema_def.get("required", []))

        if schema_def.get("type") != "object" or "properties" not in schema_def:
            log_to_stderr(f"Skipping non-object schema in definitions: {schema_name}")
            continue

        code.append(f"class {class_name}(BaseModel):")
        properties = schema_def.get("properties", {})
        if not properties:
            code.append("    pass\n")
            continue

        fields = []
        for prop_name, prop_def in properties.items():
            field_name = safe_name(prop_name)
            is_required = prop_name in required_fields
            python_type = "Any"
            if "$ref" in prop_def:
                python_type = f'"{safe_name(prop_def["$ref"].split("/")[-1])}"'
            elif "type" in prop_def:
                json_type = prop_def["type"]
                if json_type == "array":
                    items_def = prop_def.get("items", {})
                    item_type = "Any"
                    if "$ref" in items_def:
                        item_type = f'"{safe_name(items_def["$ref"].split("/")[-1])}"'
                    elif "type" in items_def:
                        item_type = TYPE_MAP.get(items_def["type"], "Any")
                    python_type = f"List[{item_type}]"
                else:
                    python_type = TYPE_MAP.get(json_type, "Any")
                    if prop_def.get("format") in FORMAT_MAP:
                        python_type = FORMAT_MAP[prop_def["format"]]

            field_type = (
                python_type if is_required else f"Optional[{python_type}] = None"
            )
            if field_name != prop_name:
                fields.append(
                    f'    {field_name}: {field_type} = Field(alias="{prop_name}")'
                )
            else:
                fields.append(f"    {field_name}: {field_type}")

        code.extend(fields if fields else ["    pass"])
        code.append("\n")

    return code


def _generate_parameter_models(spec: Dict[str, Any]) -> List[str]:
    """Generates Pydantic models for the parameters of each operation."""
    code = []
    for path, path_item in spec.get("paths", {}).items():
        for method, operation in path_item.items():
            if "operationId" not in operation or not operation.get("parameters"):
                continue

            op_id = operation["operationId"]
            class_name = f"{op_id.capitalize()}Parameters"

            fields = []
            for param in operation.get("parameters", []):
                if param.get("in") not in ["path", "query", "header"]:
                    continue

                param_name = param["name"]
                field_name = safe_name(param_name)
                is_required = param.get("required", False)

                schema = param.get("schema", param)  # Swagger 2 puts schema inline
                json_type = schema.get("type")
                python_type = TYPE_MAP.get(json_type, "Any")

                field_type = (
                    python_type if is_required else f"Optional[{python_type}] = None"
                )

                # Header parameters with hyphens need an alias
                alias = (
                    param_name
                    if field_name != param_name or "-" in param_name
                    else None
                )
                if alias:
                    fields.append(
                        f'    {field_name}: {field_type} = Field(alias="{alias}")'
                    )
                else:
                    fields.append(f"    {field_name}: {field_type}")

            if fields:
                code.append(f"class {class_name}(BaseModel):")
                code.extend(fields)
                code.append("\n")
    return code


def generate_pydantic_code(spec: Dict[str, Any]) -> str:
    """Generates the complete schemas.py file content."""
    schemas = _get_schemas(spec)

    header = [
        "# Generated by the Contextually Blueprint Compiler",
        "from __future__ import annotations",
        "from typing import Any, Dict, List, Optional",
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
        "id": f"blueprint:{info.get('title', 'untitled').lower().replace(' ', '-')}",
        "name": info.get("title", "Untitled API"),
        "version": info.get("version", "1.0.0"),
        "connector_provider_key": "rest-declarative",
        "auth_config": {"type": "none"},
        "browse_config": {"base_url_template": server_url, "action_templates": {}},
    }

    action_templates = {}
    for path, path_item in spec.get("paths", {}).items():
        for method, operation in path_item.items():
            if (
                method.lower() not in ["get", "post", "put", "patch", "delete"]
                or "operationId" not in operation
            ):
                continue

            op_id = operation["operationId"]
            api_endpoint = path.replace("{", "{{ ").replace("}", " }}")
            action = {"http_method": method.upper(), "api_endpoint": api_endpoint}

            if any(
                p.get("in") in ["path", "query", "header"]
                for p in operation.get("parameters", [])
            ):
                action["parameters_model"] = f"schemas.{op_id.capitalize()}Parameters"

            body_schema_ref = None
            if "requestBody" in operation:  # OpenAPI 3.x
                try:
                    body_schema_ref = operation["requestBody"]["content"][
                        "application/json"
                    ]["schema"]["$ref"]
                except KeyError:
                    pass
            else:  # Swagger 2.0
                for param in operation.get("parameters", []):
                    if param.get("in") == "body" and "$ref" in param.get("schema", {}):
                        body_schema_ref = param["schema"]["$ref"]
                        break
            if body_schema_ref:
                body_schema_name = safe_name(body_schema_ref.split("/")[-1])
                action["payload_constructor"] = {
                    "_constructor": f"schemas.{body_schema_name}"
                }

            action_templates[op_id] = action

    blueprint["browse_config"]["action_templates"] = action_templates

    return yaml.dump(blueprint, sort_keys=False, indent=2, width=120)


if __name__ == "__main__":
    try:
        log_to_stderr("Contextually custom compiler started.")
        spec_content = json.load(sys.stdin)

        log_to_stderr("Generating Pydantic models with custom generator...")
        schemas_py = generate_pydantic_code(spec_content)
        log_to_stderr(f"Generated {len(schemas_py)} bytes of Python model code.")

        log_to_stderr("Generating Contextually blueprint...")
        blueprint_yaml = generate_ccl_blueprint(spec_content)
        log_to_stderr(f"Generated {len(blueprint_yaml)} bytes of YAML blueprint.")

        output = {"blueprint_yaml": blueprint_yaml, "schemas_py": schemas_py}
        print(json.dumps(output, indent=2))

        log_to_stderr("Compiler finished successfully.")
        sys.exit(0)

    except Exception as e:
        log_to_stderr(f"FATAL ERROR: {type(e).__name__} - {e}")
        print(f"Compiler failed: {e}", file=sys.stderr)
        sys.exit(1)
