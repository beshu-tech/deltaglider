#!/usr/bin/env python3
"""
Generate API documentation for DeltaGlider SDK.

This script generates documentation from Python source code using introspection.
Can be extended to use tools like Sphinx, pdoc, or mkdocs.
"""

import ast
import json
import sys
from pathlib import Path
from typing import Dict, List, Any

def extract_docstrings(file_path: Path) -> Dict[str, Any]:
    """Extract docstrings and signatures from Python file."""
    with open(file_path, 'r') as f:
        tree = ast.parse(f.read(), filename=str(file_path))

    docs = {
        "module": ast.get_docstring(tree),
        "classes": {},
        "functions": {}
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            class_docs = {
                "docstring": ast.get_docstring(node),
                "methods": {}
            }

            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    method_doc = {
                        "docstring": ast.get_docstring(item),
                        "signature": get_function_signature(item)
                    }
                    class_docs["methods"][item.name] = method_doc

            docs["classes"][node.name] = class_docs

        elif isinstance(node, ast.FunctionDef) and node.col_offset == 0:
            docs["functions"][node.name] = {
                "docstring": ast.get_docstring(node),
                "signature": get_function_signature(node)
            }

    return docs

def get_function_signature(node: ast.FunctionDef) -> str:
    """Extract function signature."""
    args = []

    for arg in node.args.args:
        arg_str = arg.arg
        if arg.annotation:
            arg_str += f": {ast.unparse(arg.annotation)}"
        args.append(arg_str)

    defaults = node.args.defaults
    if defaults:
        for i, default in enumerate(defaults, start=len(args) - len(defaults)):
            args[i] += f" = {ast.unparse(default)}"

    return f"({', '.join(args)})"

def generate_markdown_docs(docs: Dict[str, Any], module_name: str) -> str:
    """Generate Markdown documentation from extracted docs."""
    lines = [f"# {module_name} API Documentation\n"]

    if docs["module"]:
        lines.append(f"{docs['module']}\n")

    if docs["functions"]:
        lines.append("## Functions\n")
        for name, func in docs["functions"].items():
            lines.append(f"### `{name}{func['signature']}`\n")
            if func["docstring"]:
                lines.append(f"{func['docstring']}\n")

    if docs["classes"]:
        lines.append("## Classes\n")
        for class_name, class_info in docs["classes"].items():
            lines.append(f"### {class_name}\n")
            if class_info["docstring"]:
                lines.append(f"{class_info['docstring']}\n")

            if class_info["methods"]:
                lines.append("#### Methods\n")
                for method_name, method_info in class_info["methods"].items():
                    lines.append(f"##### `{method_name}{method_info['signature']}`\n")
                    if method_info["docstring"]:
                        lines.append(f"{method_info['docstring']}\n")

    return "\n".join(lines)

def main():
    """Generate documentation for DeltaGlider SDK."""
    src_dir = Path(__file__).parent.parent.parent / "src" / "deltaglider"

    # Extract documentation from client.py
    client_docs = extract_docstrings(src_dir / "client.py")

    # Generate API documentation
    api_content = generate_markdown_docs(client_docs, "deltaglider.client")

    # Save generated documentation
    output_file = Path(__file__).parent / "generated_api.md"
    with open(output_file, 'w') as f:
        f.write(api_content)

    print(f"Documentation generated: {output_file}")

    # Generate index of all modules
    modules = []
    for py_file in src_dir.rglob("*.py"):
        if not py_file.name.startswith("_"):
            rel_path = py_file.relative_to(src_dir)
            module_name = str(rel_path).replace("/", ".").replace(".py", "")
            modules.append(module_name)

    index_file = Path(__file__).parent / "module_index.json"
    with open(index_file, 'w') as f:
        json.dump({"modules": sorted(modules)}, f, indent=2)

    print(f"Module index generated: {index_file}")

if __name__ == "__main__":
    main()