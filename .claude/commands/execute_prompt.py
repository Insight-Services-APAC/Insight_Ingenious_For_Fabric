#!/usr/bin/env python3
"""
Prompt Executor with Frontmatter Support

This script processes prompts with frontmatter parameters and executes them
with the specified arguments, making prompts truly reusable and parameterized.
"""

import yaml
import argparse
import sys
from pathlib import Path
from typing import Dict, Any


class PromptExecutor:
    """Execute prompts with frontmatter parameter substitution."""
    
    def __init__(self, prompt_file: str):
        self.prompt_file = Path(prompt_file)
        self.frontmatter = {}
        self.content = ""
        self._parse_prompt()
    
    def _parse_prompt(self):
        """Parse frontmatter and content from prompt file."""
        with open(self.prompt_file, 'r') as f:
            content = f.read()
        
        if content.startswith('---'):
            parts = content.split('---', 2)
            if len(parts) >= 3:
                # Has frontmatter
                try:
                    self.frontmatter = yaml.safe_load(parts[1])
                    self.content = parts[2].strip()
                except yaml.YAMLError as e:
                    print(f"❌ Error parsing frontmatter: {e}")
                    sys.exit(1)
            else:
                # No frontmatter
                self.content = content
        else:
            # No frontmatter
            self.content = content
    
    def get_parameters(self) -> Dict[str, Any]:
        """Get parameter definitions from frontmatter."""
        return self.frontmatter.get('parameters', {})
    
    def validate_parameters(self, provided_params: Dict[str, Any]) -> bool:
        """Validate provided parameters against frontmatter spec."""
        param_spec = self.get_parameters()
        
        # Check required parameters
        for param_name, spec in param_spec.items():
            if spec.get('required', False) and param_name not in provided_params:
                print(f"❌ Required parameter '{param_name}' not provided")
                return False
        
        # Validate parameter types and ranges
        for param_name, value in provided_params.items():
            if param_name in param_spec:
                spec = param_spec[param_name]
                
                # Type validation
                expected_type = spec.get('type', 'string')
                if expected_type == 'integer':
                    try:
                        int_value = int(value)
                        # Range validation
                        if 'min' in spec and int_value < spec['min']:
                            print(f"❌ Parameter '{param_name}' below minimum: {int_value} < {spec['min']}")
                            return False
                        if 'max' in spec and int_value > spec['max']:
                            print(f"❌ Parameter '{param_name}' above maximum: {int_value} > {spec['max']}")
                            return False
                        provided_params[param_name] = int_value
                    except ValueError:
                        print(f"❌ Parameter '{param_name}' must be an integer, got: {value}")
                        return False
                
                # Options validation
                if 'options' in spec and value not in spec['options']:
                    print(f"❌ Parameter '{param_name}' must be one of {spec['options']}, got: {value}")
                    return False
        
        return True
    
    def execute_prompt(self, parameters: Dict[str, Any]) -> str:
        """Execute prompt with parameter substitution."""
        
        # Add defaults for missing optional parameters
        param_spec = self.get_parameters()
        for param_name, spec in param_spec.items():
            if param_name not in parameters and 'default' in spec:
                parameters[param_name] = spec['default']
        
        # Validate parameters
        if not self.validate_parameters(parameters):
            sys.exit(1)
        
        # Substitute parameters in content
        processed_content = self.content
        for param_name, value in parameters.items():
            placeholder = f"{{{param_name}}}"
            processed_content = processed_content.replace(placeholder, str(value))
        
        return processed_content
    
    def show_help(self):
        """Show help information from frontmatter."""
        print(f"📋 Prompt: {self.frontmatter.get('name', 'Unknown')}")
        print(f"📖 Description: {self.frontmatter.get('description', 'No description')}")
        print(f"📅 Version: {self.frontmatter.get('version', 'Unknown')}")
        print()
        
        parameters = self.get_parameters()
        if parameters:
            print("📝 Parameters:")
            for param_name, spec in parameters.items():
                required = "✅ Required" if spec.get('required', False) else "⚪ Optional"
                param_type = spec.get('type', 'string')
                default = f" (default: {spec['default']})" if 'default' in spec else ""
                
                print(f"  • {param_name} ({param_type}){default} - {required}")
                print(f"    {spec.get('description', 'No description')}")
                
                if 'options' in spec:
                    print(f"    Options: {spec['options']}")
                if 'example' in spec:
                    print(f"    Example: {spec['example']}")
                print()
        
        if 'usage' in self.frontmatter:
            print("💡 Usage Instructions:")
            print(self.frontmatter['usage'])
        
        if 'outputs' in self.frontmatter:
            print("📊 Expected Outputs:")
            outputs = self.frontmatter['outputs']
            if isinstance(outputs, list):
                for output in outputs:
                    print(f"  • {output}")
            elif isinstance(outputs, dict):
                for output_name, description in outputs.items():
                    print(f"  • {output_name}: {description}")


def main():
    """Command line interface for prompt execution."""
    parser = argparse.ArgumentParser(
        description="Execute prompts with frontmatter parameter support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show prompt help
  python execute_prompt.py comprehensive_table_analysis.md --help

  # Execute with parameters
  python execute_prompt.py comprehensive_table_analysis.md --table_name account --schema_name dbo

  # Execute with all parameters
  python execute_prompt.py comprehensive_table_analysis.md --table_name contact --analysis_depth standard --sample_size 500
        """
    )
    
    parser.add_argument("prompt_file", help="Path to the prompt file with frontmatter")
    parser.add_argument("--help-prompt", action="store_true", help="Show prompt-specific help from frontmatter")
    
    # Parse known args first to handle dynamic parameters
    known_args, remaining = parser.parse_known_args()
    
    # Load prompt to get parameter definitions
    try:
        executor = PromptExecutor(known_args.prompt_file)
    except FileNotFoundError:
        print(f"❌ Prompt file not found: {known_args.prompt_file}")
        sys.exit(1)
    
    # Show prompt help if requested
    if known_args.help_prompt:
        executor.show_help()
        sys.exit(0)
    
    # Add dynamic parameters based on frontmatter
    parameters = executor.get_parameters()
    for param_name, spec in parameters.items():
        param_type = spec.get('type', 'string')
        help_text = spec.get('description', f'Parameter {param_name}')
        
        if 'default' in spec:
            help_text += f" (default: {spec['default']})"
        
        if param_type == 'integer':
            parser.add_argument(f"--{param_name}", type=int, help=help_text)
        else:
            parser.add_argument(f"--{param_name}", type=str, help=help_text)
    
    # Parse all arguments
    args = parser.parse_args()
    
    # Extract parameter values
    param_values = {}
    for param_name in parameters.keys():
        value = getattr(args, param_name, None)
        if value is not None:
            param_values[param_name] = value
    
    # Execute prompt
    print("🚀 Executing Prompt with Parameters")
    print("=" * 50)
    print(f"📁 Prompt File: {known_args.prompt_file}")
    print(f"📋 Parameters: {param_values}")
    print("=" * 50)
    print()
    
    try:
        processed_prompt = executor.execute_prompt(param_values)
        print(processed_prompt)
    except Exception as e:
        print(f"❌ Error executing prompt: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()