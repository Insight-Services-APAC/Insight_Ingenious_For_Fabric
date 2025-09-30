Generate a comprehensive daily summary of Azure subscription activity using Azure CLI.

1. **Check Azure login status**: Run `az account show` to verify authentication
2. **Get subscription info**: Display current subscription name and ID
3. **Summarize resource activity** for today's date:
   - List all resource groups and their resource counts
   - Show recent deployments and their status
   - Display any new resources created today
   - Check for any failed operations or errors
4. **Monitor key services**:
   - Container Apps: list running apps and their status
   - Storage accounts: show any new containers or significant changes
   - SQL databases: check for any schema changes or performance issues
   - Container registries: list recent image pushes
   - Redis caches: show connection and performance metrics
5. **Security and access**:
   - Review any new firewall rules added today
   - Check for certificate updates or expirations
   - List any new service principal activities
6. **Cost insights**:
   - Show estimated daily costs if available
   - Highlight any resource scaling events
7. **Format the output** in a clear, organized summary with:
   - Executive summary of key changes
   - Detailed breakdown by service
   - Any recommendations or alerts
   - Total resource counts and changes from yesterday

Focus on actionable insights and highlight anything that requires attention or follow-up.
