import traceback
from pathlib import Path
import json

from rich.console import Console

from ingen_fab.az_cli.onelake_utils import OneLakeUtils
from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.config_utils.variable_lib_factory import VariableLibraryFactory
from ingen_fab.fabric_cicd.promotion_utils import SyncToFabricEnvironment


def deploy_to_environment(ctx):
    if ctx.obj.get("fabric_workspace_repo_dir") is None:
        ConsoleStyles.print_error(
            Console(),
            "Fabric workspace repository directory not set. Use --fabric-workspace-repo-dir directly after ingen_fab to specify it.",
        )
        raise SystemExit(1)
    if ctx.obj.get("fabric_environment") is None:
        ConsoleStyles.print_error(
            Console(),
            "Fabric environment not set. Use --fabric-environment directly after ingen_fab to specify it.",
        )
        raise SystemExit(1)

    # Check if environment is local. If so raise an error
    if ctx.obj.get("fabric_environment") == "local":
        ConsoleStyles.print_error(
            Console(),
            "Cannot deploy to local environment. Please specify a different environment.",
        )
        raise SystemExit(1)

    stf = SyncToFabricEnvironment(
        project_path=Path(ctx.obj.get("fabric_workspace_repo_dir")),
        environment=str(ctx.obj.get("fabric_environment")),
    )
    stf.sync_environment()


def cleanup_orphaned_items(ctx, dry_run: bool = False, force: bool = False):
    """Remove workspace items that are not in fabric_workspace_items."""
    console = Console()
    
    if ctx.obj.get("fabric_workspace_repo_dir") is None:
        ConsoleStyles.print_error(
            console, "Fabric workspace repository directory not set."
        )
        raise SystemExit(1)
        
    project_path = Path(ctx.obj.get("fabric_workspace_repo_dir"))
    environment = str(ctx.obj.get("fabric_environment"))
    
    # Get local items from fabric_workspace_items
    local_items = []
    fabric_items_path = project_path / "fabric_workspace_items"
    
    if not fabric_items_path.exists():
        ConsoleStyles.print_error(console, "fabric_workspace_items directory not found")
        raise SystemExit(1)
        
    for platform_file in fabric_items_path.rglob(".platform"):
        try:
            with open(platform_file, "r", encoding="utf-8") as f:
                platform_data = json.load(f)
                display_name = platform_data.get("metadata", {}).get("displayName")
                item_type = platform_data.get("metadata", {}).get("type")
                if display_name and item_type:
                    local_items.append(f"{display_name}.{item_type}")
        except (json.JSONDecodeError, FileNotFoundError):
            continue
    
    ConsoleStyles.print_info(console, f"Found {len(local_items)} local items")
    
    # Initialize Fabric API
    from ingen_fab.fabric_api.utils import FabricApiUtils
    fabric_api = FabricApiUtils(environment=environment, project_path=project_path)
    
    if dry_run:
        # Show what would be deleted
        workspace_items = fabric_api.list_workspace_items()
        preserved_types = {"Lakehouse", "Warehouse", "VariableLibrary", "Environment", "SQLEndpoint"}
        local_items_set = set(local_items)
        
        orphaned_items = []
        for item in workspace_items:
            item_type = item.get("type")
            item_name = item.get("displayName", "Unknown")
            item_key = f"{item_name}.{item_type}"
            
            if item_type not in preserved_types and item_key not in local_items_set:
                orphaned_items.append(f"{item_name} ({item_type})")
        
        if orphaned_items:
            ConsoleStyles.print_warning(console, f"Would delete {len(orphaned_items)} orphaned items:")
            for item in orphaned_items:
                ConsoleStyles.print_dim(console, f"  - {item}")
        else:
            ConsoleStyles.print_success(console, "No orphaned items found")
        return
    
    # Confirm deletion
    if not force:
        import typer
        if not typer.confirm("Delete orphaned workspace items?"):
            ConsoleStyles.print_info(console, "Operation cancelled")
            return
    
    # Perform cleanup
    ConsoleStyles.print_info(console, "Cleaning up orphaned items...")
    result = fabric_api.delete_orphaned_items(local_items)
    
    if result["total_deleted"] > 0:
        ConsoleStyles.print_success(
            console, f"Deleted {result['total_deleted']} orphaned items"
        )
    else:
        ConsoleStyles.print_info(console, "No orphaned items to delete")
    
    if result["errors"]:
        ConsoleStyles.print_error(console, f"Encountered {len(result['errors'])} errors:")
        for error in result["errors"][:5]:  # Show first 5 errors
            ConsoleStyles.print_dim(
                console, 
                f"  - {error['item_name']} ({error['item_type']}): {error['status_code']} - {error['error'][:100]}"
            )



def perform_code_replacements(ctx, output_dir=None, preserve_structure=True):
    if ctx.obj.get("fabric_workspace_repo_dir") is None:
        ConsoleStyles.print_error(
            Console(),
            "Fabric workspace repository directory not set. Use --fabric-workspace-repo-dir directly after ingen_fab to specify it.",
        )
        raise SystemExit(1)
    if ctx.obj.get("fabric_environment") is None:
        ConsoleStyles.print_error(
            Console(),
            "Fabric environment not set. Use --fabric-environment directly after ingen_fab to specify it.",
        )
        raise SystemExit(1)

    vlu = VariableLibraryFactory.for_deployment_from_cli(ctx)

    # Call inject_variables_into_template with specific parameters:
    # - output_dir=None for in-place (default), or specified output directory
    # - replace_placeholders=False (only inject code, don't replace {{varlib:...}})
    # - inject_code=True (inject code between markers)
    vlu.inject_variables_into_template(
        output_dir=output_dir,
        preserve_structure=preserve_structure,
        replace_placeholders=False,  # Don't replace {{varlib:...}} placeholders
        inject_code=True,  # Only inject code between markers
    )


def upload_python_libs_to_config_lakehouse(
    environment: str, project_path: str, console: Console = Console()
):
    """Uploads Python libraries to the config lakehouse in OneLake with variable injection during upload."""
    try:
        # Variable injection will be performed automatically during OneLake upload
        # This eliminates redundant processing and improves performance
        onelake_utils = OneLakeUtils(
            environment=environment, project_path=Path(project_path), console=console
        )

        # OneLakeUtils now handles all the UI messaging and progress tracking
        results = onelake_utils.upload_python_libs_to_config_lakehouse()

        # Show any failed uploads in detail if needed
        if results["failed"]:
            console.print("\n[red]Failed uploads (details):[/red]")
            for result in results["failed"]:
                console.print(
                    f"  [red]✗[/red] [dim]{result['local_path']}:[/dim] {result['error']}"
                )

    except Exception as e:
        ConsoleStyles.print_error(console, f"Error: {str(e)}")
        ConsoleStyles.print_error(console, "Stack trace:")
        ConsoleStyles.print_error(console, traceback.format_exc())
        ConsoleStyles.print_error(
            console,
            "Make sure you're authenticated with Azure and have the correct permissions.",
        )

def upload_dbt_project_to_config_lakehouse(
    environment: str, dbt_project_name: str, project_path: str, console: Console = Console()
):
    """Uploads a dbt project to the config lakehouse in OneLake with variable injection during upload."""
    try:
        # Variable injection will be performed automatically during OneLake upload
        # This eliminates redundant processing and improves performance
        onelake_utils = OneLakeUtils(
            environment=environment, project_path=Path(project_path), console=console
        )

        # OneLakeUtils now handles all the UI messaging and progress tracking
        results = onelake_utils.upload_dbt_project_to_config_lakehouse(dbt_project_name=dbt_project_name, dbt_project_path=project_path)

        # Show any failed uploads in detail if needed
        if results["failed"]:
            console.print("\n[red]Failed uploads (details):[/red]")
            for result in results["failed"]:
                console.print(
                    f"  [red]✗[/red] [dim]{result['local_path']}:[/dim] {result['error']}"
                )

    except Exception as e:
        ConsoleStyles.print_error(console, f"Error: {str(e)}")
        ConsoleStyles.print_error(console, "Stack trace:")
        ConsoleStyles.print_error(console, traceback.format_exc())
        ConsoleStyles.print_error(
            console,
            "Make sure you're authenticated with Azure and have the correct permissions.",
        )

def _save_item_definition_content(
    artefact_dir: Path, 
    definition: dict, 
    artefact_type: str, 
    console: Console
) -> list[str]:
    """
    Save the item definition content to appropriate files based on artefact type.
    
    Args:
        artefact_dir: Directory to save content to
        definition: The definition content from Fabric API
        artefact_type: Type of the artefact
        console: Console for user feedback
        
    Returns:
        List of created file names
    """
    import base64
    import json
    
    files_created = []
    
    try:
        # Get the definition content
        definition_content = definition.get("definition", {})
        
        # Handle different artefact types
        if artefact_type == "Notebook":
            # For notebooks, save the notebook content
            parts = definition_content.get("parts", [])
            for part in parts:
                part_name = part.get("path", "unknown")
                part_payload = part.get("payload", "")
                
                if part_name and part_payload:
                    # Skip .platform files from notebook content as we create our own
                    if part_name == ".platform":
                        continue
                        
                    # Decode base64 content if needed
                    try:
                        if part.get("payloadType") == "InlineBase64":
                            content = base64.b64decode(part_payload).decode('utf-8')
                        else:
                            content = part_payload
                        
                        # Save content to file
                        content_file = artefact_dir / part_name
                        content_file.parent.mkdir(parents=True, exist_ok=True)
                        
                        with open(content_file, "w", encoding="utf-8") as f:
                            f.write(content)
                        files_created.append(part_name)
                        
                    except Exception as e:
                        ConsoleStyles.print_warning(
                            console,
                            f"⚠️  Failed to decode content for {part_name}: {e}"
                        )
                        
        elif artefact_type in ["Report", "SemanticModel", "DataPipeline", "GraphQLApi"]:
            # For other artefact types, save parts as individual files
            parts = definition_content.get("parts", [])
            for part in parts:
                part_name = part.get("path", f"part_{len(files_created)}")
                part_payload = part.get("payload", "")
                
                if part_payload:
                    try:
                        if part.get("payloadType") == "InlineBase64":
                            # For binary content, save as binary
                            content_bytes = base64.b64decode(part_payload)
                            content_file = artefact_dir / part_name
                            content_file.parent.mkdir(parents=True, exist_ok=True)
                            
                            with open(content_file, "wb") as f:
                                f.write(content_bytes)
                        else:
                            # For text content
                            content_file = artefact_dir / part_name
                            content_file.parent.mkdir(parents=True, exist_ok=True)
                            
                            with open(content_file, "w", encoding="utf-8") as f:
                                f.write(part_payload)
                        
                        files_created.append(part_name)
                        
                    except Exception as e:
                        ConsoleStyles.print_warning(
                            console,
                            f"⚠️  Failed to save content for {part_name}: {e}"
                        )
        else:
            # For other types, save the raw definition as JSON
            definition_file = artefact_dir / "definition.json"
            with open(definition_file, "w", encoding="utf-8") as f:
                json.dump(definition_content, f, indent=2)
            files_created.append("definition.json")
        
    except Exception as e:
        ConsoleStyles.print_warning(
            console,
            f"⚠️  Error processing definition content: {e}"
        )
    
    return files_created

def download_artefact(
    ctx,
    artefact_name: str,
    artefact_type: str,
    workspace_id: str | None = None,
    output_path: Path | None = None,
    force: bool = False,
    console: Console | None = None,
) -> None:
    """Download a specific Fabric artefact from workspace using Fabric REST API."""
    import json
    import typer
    from ingen_fab.fabric_api.utils import FabricApiUtils
    from ingen_fab.config_utils.variable_lib import VariableLibraryUtils
    
    if console is None:
        console = Console()
    
    project_path = ctx.obj["fabric_workspace_repo_dir"]
    environment = ctx.obj["fabric_environment"]
    
    # Resolve workspace ID if not provided
    if workspace_id is None:
        vlu = VariableLibraryUtils(
            project_path=project_path,
            environment=environment,
        )
        workspace_id = vlu.get_workspace_id()
        ConsoleStyles.print_info(console, f"Using workspace ID from environment: {workspace_id}")
    else:
        ConsoleStyles.print_info(console, f"Using specified workspace ID: {workspace_id}")
    
    # Set default output path
    if output_path is None:
        output_path = Path(project_path) / "fabric_workspace_items"
    else:
        output_path = Path(output_path)
    
    # Validate artefact type
    valid_types = [
        "VariableLibrary", "DataPipeline", "Environment", "Notebook", "Report", 
        "SemanticModel", "Lakehouse", "MirroredDatabase", "CopyJob", "Eventhouse", 
        "Reflex", "Eventstream", "Warehouse", "SQLDatabase", "GraphQLApi"
    ]
    if artefact_type not in valid_types:
        ConsoleStyles.print_error(
            console, 
            f"❌ Invalid artefact type '{artefact_type}'. Valid types: {', '.join(valid_types)}"
        )
        raise typer.Exit(code=1)
    
    ConsoleStyles.print_info(
        console, 
        f"🔍 Searching for artefact '{artefact_name}' of type '{artefact_type}' in workspace {workspace_id}"
    )
    
    try:
        # Initialize Fabric API client
        fabric_api = FabricApiUtils(
            environment=environment,
            project_path=Path(project_path),
            workspace_id=workspace_id
        )
        
        # Get list of items in workspace to find the target artefact
        items = fabric_api.list_workspace_items(workspace_id)
        if not items:
            ConsoleStyles.print_error(console, "❌ Failed to retrieve workspace items")
            raise typer.Exit(code=1)
        
        target_item = None
        for item in items:
            if (item.get("displayName") == artefact_name and 
                item.get("type") == artefact_type):
                target_item = item
                break
        
        if target_item is None:
            ConsoleStyles.print_error(
                console,
                f"❌ Artefact '{artefact_name}' of type '{artefact_type}' not found in workspace"
            )
            # List available items of this type for user reference
            matching_type_items = [
                item for item in items
                if item.get("type") == artefact_type
            ]
            if matching_type_items:
                ConsoleStyles.print_info(console, f"\nAvailable {artefact_type} items:")
                for item in matching_type_items[:10]:  # Show first 10
                    ConsoleStyles.print_dim(console, f"  - {item.get('displayName')}")
                if len(matching_type_items) > 10:
                    ConsoleStyles.print_dim(console, f"  ... and {len(matching_type_items) - 10} more")
            raise typer.Exit(code=1)
        
        item_id = target_item["id"]
        
        # Create folder structure: fabric_workspace_items/downloaded/{artefact_type}/{artefact_name}.{artefact_type}
        downloaded_dir = output_path / "downloaded" / artefact_type
        artefact_dir = downloaded_dir / f"{artefact_name}.{artefact_type}"
        
        # Check if artefact already exists and handle force flag
        if artefact_dir.exists() and not force:
            ConsoleStyles.print_warning(
                console,
                f"⚠️  Artefact directory already exists: {artefact_dir}"
            )
            if not typer.confirm("Overwrite existing artefact?"):
                ConsoleStyles.print_info(console, "❌ Download cancelled by user")
                raise typer.Exit(code=0)
        
        # Create output directory
        artefact_dir.mkdir(parents=True, exist_ok=True)
        
        ConsoleStyles.print_info(
            console,
            f"📥 Downloading artefact '{artefact_name}' to {artefact_dir}"
        )
        
        # Download the complete item content and definition
        ConsoleStyles.print_info(console, "📥 Downloading artefact definition and content...")
        
        try:
            # Download the full item content including definition
            item_content = fabric_api.download_item_content(workspace_id, item_id, artefact_type)
            definition = item_content.get("definition", {})
            
            # Get basic item details for metadata
            item_details = fabric_api.get_item_details(workspace_id, item_id)
            if not item_details:
                ConsoleStyles.print_warning(
                    console,
                    f"⚠️  Could not get item metadata for '{artefact_name}', continuing with definition only"
                )
                item_details = {}
            
        except Exception as e:
            ConsoleStyles.print_warning(
                console,
                f"⚠️  Failed to download full definition for '{artefact_name}': {e}"
            )
            ConsoleStyles.print_info(console, "📋 Falling back to metadata-only download...")
            
            # Fallback to metadata-only download
            item_details = fabric_api.get_item_details(workspace_id, item_id)
            if not item_details:
                ConsoleStyles.print_error(
                    console,
                    f"❌ Failed to get artefact details for '{artefact_name}'"
                )
                raise typer.Exit(code=1)
            
            definition = None
            item_content = None
        
        # Save the .platform file with metadata
        platform_file = artefact_dir / ".platform"
        platform_content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": artefact_type,
                "displayName": artefact_name
            },
            "config": {
                "version": "2.0",
                "logicalId": item_id
            }
        }
        
        with open(platform_file, "w", encoding="utf-8") as f:
            json.dump(platform_content, f, indent=2)
        
        # Item metadata is available but not saved to reduce clutter
        
        # Process and save the definition content if available
        files_created = [".platform"]
            
        if definition and item_content:
            try:
                files_created.extend(
                    _save_item_definition_content(artefact_dir, definition, artefact_type, console)
                )
                ConsoleStyles.print_success(
                    console,
                    f"✅ Successfully downloaded complete artefact definition for '{artefact_name}' to {artefact_dir}"
                )
            except Exception as e:
                ConsoleStyles.print_warning(
                    console,
                    f"⚠️  Error processing definition content: {e}"
                )
                ConsoleStyles.print_success(
                    console,
                    f"✅ Successfully downloaded artefact metadata for '{artefact_name}' to {artefact_dir}"
                )
        else:
            ConsoleStyles.print_success(
                console,
                f"✅ Successfully downloaded artefact metadata for '{artefact_name}' to {artefact_dir}"
            )
            ConsoleStyles.print_info(
                console,
                "📋 Note: Full definition content was not available. This may be expected for certain artefact types."
            )
        
        # List downloaded files
        if files_created:
            ConsoleStyles.print_info(console, "\nDownloaded files:")
            for file_name in files_created:
                ConsoleStyles.print_dim(console, f"  - {file_name}")
        else:
            # Fallback to listing all files in directory
            downloaded_files = list(artefact_dir.rglob("*"))
            if downloaded_files:
                ConsoleStyles.print_info(console, "\nDownloaded files:")
                for file_path in downloaded_files:
                    if file_path.is_file():
                        relative_path = file_path.relative_to(artefact_dir)
                        ConsoleStyles.print_dim(console, f"  - {relative_path}")
                    
    except Exception as e:
        ConsoleStyles.print_error(
            console,
            f"❌ Error downloading artefact: {e}"
        )
        raise typer.Exit(code=1)
